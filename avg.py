import time

import pandas as pd
import os


# 读取并处理 CSV 文件（分步计算）
def process_files_separately(directory):
    partial_sum_results = []
    partial_count_results = []
    partial_total_scores = []  # 用于存储化学和生物的总分

    for file in os.listdir(directory):
        if file.endswith(".csv"):
            df = pd.read_csv(os.path.join(directory, file))

            grouped_sum = df.groupby(["省份", "班级"])[["语文", "数学"]].sum()
            grouped_total = df.groupby(["省份", "班级"])[["化学", "生物"]].sum()  # 计算化学和生物的总分
            grouped_count = df.groupby(["省份", "班级"]).size().reset_index(name='count')  # 计算学生人数

            grouped_sum = grouped_sum.reset_index()
            grouped_total = grouped_total.reset_index()

            partial_sum_results.append(grouped_sum)
            partial_total_scores.append(grouped_total)
            partial_count_results.append(grouped_count)

    # 合并计算语文和数学总分
    combined_sum_result = pd.concat(partial_sum_results).groupby(["省份", "班级"]).sum().reset_index()

    # 合并计算化学、生物的总分
    combined_total_scores = pd.concat(partial_total_scores).groupby(["省份", "班级"]).sum().reset_index()

    # 合并计算学生人数
    combined_count = pd.concat(partial_count_results).groupby(["省份", "班级"]).sum().reset_index()

    # 计算化学和生物的平均分
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


# 设定数据目录
output_dir = "student_scores"

# 执行两种计算方式
start_time = time.time()
result_separate = process_files_separately(output_dir)
end_time = time.time()
print(end_time - start_time)

start_time = time.time()
result_all_at_once = process_all_at_once(output_dir)
end_time = time.time()
print(end_time - start_time)

# 对比结果
comparison = result_separate.equals(result_all_at_once)
print("两种计算方式的结果是否一致:", comparison)

# 输出计算结果
res_dir = "res_dir"
os.makedirs(res_dir, exist_ok=True)
temp_file1 = os.path.join(res_dir, "result_separate.csv")
temp_file2 = os.path.join(res_dir, "result_all_at_once.csv")
result_separate.to_csv(temp_file1, index=False, encoding="utf-8-sig")
result_all_at_once.to_csv(temp_file2, index=False, encoding="utf-8-sig")

print(f"分步计算结果保存至: {temp_file1}")
print(f"一次性计算结果保存至: {temp_file2}")
