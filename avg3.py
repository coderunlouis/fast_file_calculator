import polars as pl
import os
import ray

# 初始化 Ray
if not ray.is_initialized():
    ray.init(ignore_reinit_error=True)


# **单个文件的处理逻辑**
@ray.remote
def process_single_file(file_path):
    df = pl.scan_csv(file_path)  # 使用惰性读取，提高性能

    # 计算 语文、数学 总分
    grouped_sum = df.group_by(["省份", "班级"]).agg(
        [pl.sum("语文").alias("语文"), pl.sum("数学").alias("数学")]
    )

    # 计算 化学、生物 总分 和 加权平均所需的数据
    grouped_weighted = df.group_by(["省份", "班级"]).agg(
        [pl.sum("化学").alias("化学总分"), pl.sum("生物").alias("生物总分"), pl.count().alias("count")]
    )

    return grouped_sum.collect(), grouped_weighted.collect()


# **多进程 处理所有文件**
def process_files_separately(directory):
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".csv")]

    # 使用 Ray 并行计算
    results = ray.get([process_single_file.remote(file) for file in files])

    # 拆分结果
    partial_sum_results, partial_weighted_results = zip(*results)

    # **合并数据**
    combined_sum_result = pl.concat(partial_sum_results).group_by(["省份", "班级"]).sum()
    combined_weighted = pl.concat(partial_weighted_results).group_by(["省份", "班级"]).sum()

    # **计算加权平均（总分 / 总人数）**
    combined_weighted = combined_weighted.with_columns(
        (combined_weighted["化学总分"] / combined_weighted["count"]).alias("化学"),
        (combined_weighted["生物总分"] / combined_weighted["count"]).alias("生物"),
    ).select(["省份", "班级", "化学", "生物"])  # 只保留计算后的列

    # **合并最终结果**
    final_result = combined_sum_result.join(combined_weighted, on=["省份", "班级"])

    return final_result


# **一次性 读取所有文件计算**
def process_all_at_once(directory):
    all_data = [pl.read_csv(os.path.join(directory, f)) for f in os.listdir(directory) if f.endswith(".csv")]
    full_df = pl.concat(all_data)

    # **计算 语文、数学 总分**
    sum_result = full_df.group_by(["省份", "班级"]).agg(
        [pl.sum("语文").alias("语文"), pl.sum("数学").alias("数学")]
    )

    # **计算 化学、生物 平均分**
    mean_result = full_df.group_by(["省份", "班级"]).agg(
        [pl.mean("化学").alias("化学"), pl.mean("生物").alias("生物")]
    )

    # **合并结果**
    result = sum_result.join(mean_result, on=["省份", "班级"])
    return result


if __name__ == "__main__":
    # 设定数据目录
    output_dir = "student_scores"

    # **执行两种计算方式**
    result_separate = process_files_separately(output_dir).sort(["省份", "班级"])
    result_all_at_once = process_all_at_once(output_dir).sort(["省份", "班级"])

    # **对比结果**
    comparison = result_separate.equals(result_all_at_once)
    print("两种计算方式的结果是否一致:", comparison)

    # **输出计算结果**
    res_dir = "res_dir_avg3"
    os.makedirs(res_dir, exist_ok=True)
    temp_file1 = os.path.join(res_dir, "result_separate.csv")
    temp_file2 = os.path.join(res_dir, "result_all_at_once.csv")

    result_separate.write_csv(temp_file1)
    result_all_at_once.write_csv(temp_file2)

    print(f"分步计算结果保存至: {temp_file1}")
    print(f"一次性计算结果保存至: {temp_file2}")

    # **关闭 Ray，释放资源**
    ray.shutdown()
