import pandas as pd
import os


# 读取并处理 CSV 文件（分步计算）
def process_files_separately(directory):
    partial_results = []
    for file in os.listdir(directory):
        if file.endswith(".csv"):
            df = pd.read_csv(os.path.join(directory, file))
            grouped = df.groupby(["省份", "班级"])[["语文", "数学"]].sum().reset_index()
            partial_results.append(grouped)
    combined_result = pd.concat(partial_results).groupby(["省份", "班级"])[["语文", "数学"]].sum().reset_index()
    return combined_result


# 直接合并所有文件后进行分组计算
def process_all_at_once(directory):
    all_data = []
    for file in os.listdir(directory):
        if file.endswith(".csv"):
            df = pd.read_csv(os.path.join(directory, file))
            all_data.append(df)
    full_df = pd.concat(all_data)
    result = full_df.groupby(["省份", "班级"])[["语文", "数学"]].sum().reset_index()
    return result


# 设定数据目录
output_dir = "student_scores"

# 执行两种计算方式
result_separate = process_files_separately(output_dir)
result_all_at_once = process_all_at_once(output_dir)

# 对比结果
comparison = result_separate.equals(result_all_at_once)
print("两种计算方式的结果是否一致:", comparison)

# 输出计算结果
res_dir = "res_dir"
temp_file1 = os.path.join(res_dir, "result_separate.csv")
temp_file2 = os.path.join(res_dir, "result_all_at_once.csv")
result_separate.to_csv(temp_file1, index=False, encoding="utf-8-sig")
result_all_at_once.to_csv(temp_file2, index=False, encoding="utf-8-sig")

print(f"分步计算结果保存至: {temp_file1}")
print(f"一次性计算结果保存至: {temp_file2}")
