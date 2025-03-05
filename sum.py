import pandas as pd
import random
import os


# # 生成数据的参数
# num_students = 1000  # 总学生数
# num_classes = 10  # 班级数量
# num_files = 10  # 生成的 CSV 文件数量
# provinces = ["北京", "上海", "广东", "江苏", "浙江", "山东", "四川", "湖北", "湖南", "陕西"]
# names = ["张伟", "王芳", "李强", "赵敏", "刘洋", "陈杰", "杨阳", "吴昊", "徐静", "孙丽"]
# subjects = ["语文", "数学", "英语", "物理", "化学", "生物", "历史", "地理", "政治"]
#
#
# # 随机生成学生数据
# def generate_student_data(num_students):
#     data = []
#     for _ in range(num_students):
#         student = {
#             "姓名": random.choice(names),
#             "班级": random.randint(1, num_classes),
#             "性别": random.choice(["男", "女"]),
#             "省份": random.choice(provinces)
#         }
#         for subject in subjects:
#             student[subject] = random.randint(50, 100)  # 生成 50-100 之间的分数
#         data.append(student)
#     return data
#
#
# # 生成数据
# students_data = generate_student_data(num_students)
# random.shuffle(students_data)  # 打乱数据，以便分布到不同文件
#
# # 确保输出目录存在
output_dir = "student_scores"
# os.makedirs(output_dir, exist_ok=True)
#
# # 拆分数据并保存到多个 CSV 文件
# chunk_size = len(students_data) // num_files
# for i in range(num_files):
#     chunk = students_data[i * chunk_size: (i + 1) * chunk_size]
#     df = pd.DataFrame(chunk)
#     file_path = os.path.join(output_dir, f"student_scores_{i + 1}.csv")
#     df.to_csv(file_path, index=False, encoding="utf-8-sig")
#
# print(f"生成 {num_files} 份学生成绩数据 CSV 文件，存储在 {output_dir} 目录下！")
#

# 读取并处理 CSV 文件
def process_files_separately(directory):
    partial_results = []
    for file in os.listdir(directory):
        if file.endswith(".csv"):
            df = pd.read_csv(os.path.join(directory, file))
            grouped = df.groupby(["省份", "性别"])[["语文", "数学"]].sum().reset_index()
            partial_results.append(grouped)
    combined_result = pd.concat(partial_results).groupby(["省份", "性别"])[["语文", "数学"]].sum().reset_index()
    return combined_result


# 直接合并所有文件后进行分组计算
def process_all_at_once(directory):
    all_data = []
    for file in os.listdir(directory):
        if file.endswith(".csv"):
            df = pd.read_csv(os.path.join(directory, file))
            all_data.append(df)
    full_df = pd.concat(all_data)
    result = full_df.groupby(["省份", "性别"])[["语文", "数学"]].sum().reset_index()
    return result


# 执行两种计算方式
result_separate = process_files_separately(output_dir)
result_all_at_once = process_all_at_once(output_dir)

# 对比结果
comparison = result_separate.equals(result_all_at_once)
print("两种计算方式的结果是否一致:", comparison)

# 输出计算结果
temp_file1 = os.path.join(output_dir, "result_separate.csv")
temp_file2 = os.path.join(output_dir, "result_all_at_once.csv")
result_separate.to_csv(temp_file1, index=False, encoding="utf-8-sig")
result_all_at_once.to_csv(temp_file2, index=False, encoding="utf-8-sig")

print(f"分步计算结果保存至: {temp_file1}")
print(f"一次性计算结果保存至: {temp_file2}")
