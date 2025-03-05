import multiprocessing as mp
import os
from multiprocessing import freeze_support

import pandas as pd


# 单个文件的处理逻辑
def process_single_file(file_path):
    df = pd.read_csv(file_path)

    # 计算语文、数学的总分
    grouped_sum = df.groupby(["省份", "班级"])[["语文", "数学"]].sum()

    # 计算化学、生物的总分
    grouped_total = df.groupby(["省份", "班级"])[["化学", "生物"]].sum()

    # 计算学生人数
    grouped_count = df.groupby(["省份", "班级"]).size().reset_index(name="count")

    # 变成 DataFrame 便于后续合并
    return grouped_sum.reset_index(), grouped_total.reset_index(), grouped_count


# 使用多进程处理所有文件
def process_files_separately(directory):
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".csv")]

    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(process_single_file, files)

    # 拆分结果
    partial_sum_results, partial_total_scores, partial_count_results = zip(*results)

    # 合并数据
    combined_sum_result = pd.concat(partial_sum_results).groupby(["省份", "班级"]).sum().reset_index()
    combined_total_scores = pd.concat(partial_total_scores).groupby(["省份", "班级"]).sum().reset_index()
    combined_count = pd.concat(partial_count_results).groupby(["省份", "班级"]).sum().reset_index()

    # 计算化学、生物的平均分
    combined_total_scores[["化学", "生物"]] = combined_total_scores[["化学", "生物"]].div(combined_count["count"],
                                                                                          axis=0)

    # 合并最终结果
    final_result = combined_sum_result.merge(combined_total_scores, on=["省份", "班级"])

    return final_result


# 直接合并所有文件后进行分组计算
def process_all_at_once(directory):
    all_data = []
    for file in os.listdir(directory):
        if file.endswith(".csv"):
            df = pd.read_csv(os.path.join(directory, file))
            all_data.append(df)

    full_df = pd.concat(all_data)

    sum_result = full_df.groupby(["省份", "班级"])[["语文", "数学"]].sum()
    mean_result = full_df.groupby(["省份", "班级"])[["化学", "生物"]].mean()

    result = sum_result.merge(mean_result, on=["省份", "班级"]).reset_index()
    return result


if __name__ == '__main__':
    freeze_support()
    # 设定数据目录
    output_dir = "student_scores"

    # 执行两种计算方式
    result_separate = process_files_separately(output_dir)
    result_all_at_once = process_all_at_once(output_dir)

    # 对比结果
    comparison = result_separate.equals(result_all_at_once)
    print("两种计算方式的结果是否一致:", comparison)

    # 输出计算结果
    res_dir = "res_dir_avg2"
    os.makedirs(res_dir, exist_ok=True)
    temp_file1 = os.path.join(res_dir, "result_separate.csv")
    temp_file2 = os.path.join(res_dir, "result_all_at_once.csv")
    result_separate.to_csv(temp_file1, index=False, encoding="utf-8-sig")
    result_all_at_once.to_csv(temp_file2, index=False, encoding="utf-8-sig")

    print(f"分步计算结果保存至: {temp_file1}")
    print(f"一次性计算结果保存至: {temp_file2}")
