import pandas as pd
import random
import os

# 生成数据的参数
num_students = 10 * 10000  # 总学生数
num_classes = 1000  # 班级数量
num_files = 100  # 生成的 CSV 文件数量
provinces = ["北京", "上海", "广东", "江苏", "浙江", "山东", "四川", "湖北", "湖南", "陕西"]
names = ["张伟", "王芳", "李强", "赵敏", "刘洋", "陈杰", "杨阳", "吴昊", "徐静", "孙丽"]
subjects = ["语文", "数学", "英语", "物理", "化学", "生物", "历史", "地理", "政治"]


# 随机生成学生数据
def generate_student_data(num_students):
    data = []
    for _ in range(num_students):
        student = {
            "姓名": random.choice(names),
            "班级": random.randint(1, num_classes),
            "性别": random.choice(["男", "女"]),
            "省份": random.choice(provinces)
        }
        for subject in subjects:
            student[subject] = random.randint(50, 100)  # 生成 50-100 之间的分数
        data.append(student)
    return data


# 生成数据
students_data = generate_student_data(num_students)
random.shuffle(students_data)  # 打乱数据，以便分布到不同文件

# 确保输出目录存在
output_dir = "student_scores"
os.makedirs(output_dir, exist_ok=True)

# 拆分数据并保存到多个 CSV 文件
chunk_size = len(students_data) // num_files
for i in range(num_files):
    chunk = students_data[i * chunk_size: (i + 1) * chunk_size]
    df = pd.DataFrame(chunk)
    file_path = os.path.join(output_dir, f"student_scores_{i + 1}.csv")
    df.to_csv(file_path, index=False, encoding="utf-8-sig")

print(f"生成 {num_files} 份学生成绩数据 CSV 文件，存储在 {output_dir} 目录下！")
