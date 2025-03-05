import time

import polars as pl
import os
import ray

# 初始化 Ray
if not ray.is_initialized():
    ray.init(ignore_reinit_error=True)


# **单个文件的处理逻辑**
@ray.remote
def process_single_file(file_path, group_columns, sum_columns, mean_columns):
    df = pl.scan_csv(file_path)  # 使用惰性读取，提高性能

    # 计算需要求和的列
    grouped_sum = df.group_by(group_columns).agg(
        [pl.sum(col).alias(col) for col in sum_columns]
    )

    # 需要求总体平均的列，在计算单个文件的时候，需要返回列的和，以及列的个数，用于后面的汇总计算
    grouped_weighted = df.group_by(group_columns).agg(
        [pl.sum(col).alias(col + "总分") for col in mean_columns] +
        [pl.count().alias("count")]
    )

    return grouped_sum.collect(), grouped_weighted.collect()


# **多进程 处理所有文件**
def process_files_separately(directory, group_columns, sum_columns, mean_columns):
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".csv")]

    # 使用 Ray 并行计算
    results = ray.get(
        [process_single_file.remote(file, group_columns, sum_columns, mean_columns) for file in
         files])

    # 拆分结果
    partial_sum_results, partial_weighted_results = zip(*results)

    # 得到需要计算和的列的结果
    combined_sum_result = pl.concat(partial_sum_results).group_by(group_columns).sum()

    # 计算需要计算平均值的列的结果
    combined_weighted = pl.concat(partial_weighted_results).group_by(group_columns).sum()
    mean_results = combined_weighted.with_columns(
        [(combined_weighted[col + "总分"] / combined_weighted["count"]).alias(col) for col in mean_columns]
    ).select(group_columns + [col for col in mean_columns])  # 保留分组列和计算后的列

    # **合并最终结果**
    final_result = combined_sum_result.join(mean_results, on=group_columns)

    return final_result


# **一次性 读取所有文件计算**
def process_all_at_once(directory, group_columns, sum_columns, mean_columns):
    all_data = [pl.read_csv(os.path.join(directory, f)) for f in os.listdir(directory) if f.endswith(".csv")]
    full_df = pl.concat(all_data)

    # **计算需要求和的列**
    sum_result = full_df.group_by(group_columns).agg(
        [pl.sum(col).alias(col) for col in sum_columns]
    )

    # **计算需要求平均的列**
    mean_result = full_df.group_by(group_columns).agg(
        [pl.mean(col).alias(col) for col in mean_columns]
    )

    # **合并结果**
    result = sum_result.join(mean_result, on=group_columns)
    return result


if __name__ == "__main__":
    # 设定数据目录
    output_dir = "student_scores"

    # 定义分组条件
    group_columns = ["省份", "班级"]  # 你可以在这里更改分组条件

    # 定义需要求和的列
    sum_columns = ["语文", "数学"]  # 你可以在这里更改需要求和的列

    # 定义需要求平均的列
    mean_columns = ["化学", "生物", "地理"]  # 你可以在这里更改需要求平均的列

    # **执行两种计算方式**
    start_time = time.time()
    result_separate = process_files_separately(output_dir, group_columns, sum_columns, mean_columns).sort(group_columns)
    end_time = time.time()
    print(end_time - start_time)

    start_time = time.time()
    result_all_at_once = process_all_at_once(output_dir, group_columns, sum_columns, mean_columns).sort(group_columns)
    end_time = time.time()
    print(end_time - start_time)

    # **对比结果**
    comparison = result_separate.equals(result_all_at_once)
    print("两种计算方式的结果是否一致:", comparison)

    # **输出计算结果**
    res_dir = "res_dir_avg4"
    os.makedirs(res_dir, exist_ok=True)
    temp_file1 = os.path.join(res_dir, "result_separate.csv")
    temp_file2 = os.path.join(res_dir, "result_all_at_once.csv")

    result_separate.write_csv(temp_file1)
    result_all_at_once.write_csv(temp_file2)

    print(f"分步计算结果保存至: {temp_file1}")
    print(f"一次性计算结果保存至: {temp_file2}")

    # **关闭 Ray，释放资源**
    ray.shutdown()
