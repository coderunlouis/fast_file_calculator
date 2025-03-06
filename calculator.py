import time
from typing import List, Dict, Union, Callable

import polars as pl
import os
import ray

# **单个文件的处理逻辑**
@ray.remote
def process_single_file(
    file_path, 
    group_columns, 
    sum_columns, 
    mean_columns, 
    filter_conditions: List[Dict[str, Union[str, Callable]]] = None
):
    # 使用惰性读取
    df = pl.scan_csv(file_path)
    
    # 应用过滤条件
    if filter_conditions:
        for condition in filter_conditions:
            column = condition['column']
            op = condition.get('op', '==')
            value = condition['value']
            
            # 根据不同操作符应用过滤
            if op == '==':
                df = df.filter(pl.col(column) == value)
            elif op == '>=':
                df = df.filter(pl.col(column) >= value)
            elif op == '<=':
                df = df.filter(pl.col(column) <= value)
            elif op == '>':
                df = df.filter(pl.col(column) > value)
            elif op == '<':
                df = df.filter(pl.col(column) < value)
            elif op == 'in':
                # 新增IN过滤支持
                df = df.filter(pl.col(column).is_in(value))
            elif callable(op):
                # 支持自定义过滤函数
                df = df.filter(pl.col(column).map(op))
    
    # 计算需要求和的列
    grouped_sum = df.group_by(group_columns).agg(
        [pl.sum(col).alias(col) for col in sum_columns]
    )

    # 需要求总体平均的列
    grouped_weighted = df.group_by(group_columns).agg(
        [pl.sum(col).alias(col + "总分") for col in mean_columns] +
        [pl.count().alias("count")]
    )

    return grouped_sum.collect(), grouped_weighted.collect()


# **多进程 处理所有文件**
def process_files_separately(
    directory, 
    group_columns, 
    sum_columns, 
    mean_columns, 
    filter_conditions: List[Dict[str, Union[str, Callable]]] = None
):
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".csv")]

    # 使用 Ray 并行计算
    results = ray.get(
        [process_single_file.remote(
            file, 
            group_columns, 
            sum_columns, 
            mean_columns, 
            filter_conditions
        ) for file in files]
    )

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
def process_all_at_once(
    directory, 
    group_columns, 
    sum_columns, 
    mean_columns, 
    filter_conditions: List[Dict[str, Union[str, Callable]]] = None
):
    all_data = [pl.read_csv(os.path.join(directory, f)) for f in os.listdir(directory) if f.endswith(".csv")]
    full_df = pl.concat(all_data)
    
    # 应用过滤条件
    if filter_conditions:
        for condition in filter_conditions:
            column = condition['column']
            op = condition.get('op', '==')
            value = condition['value']
            
            # 根据不同操作符应用过滤
            if op == '==':
                full_df = full_df.filter(pl.col(column) == value)
            elif op == '>=':
                full_df = full_df.filter(pl.col(column) >= value)
            elif op == '<=':
                full_df = full_df.filter(pl.col(column) <= value)
            elif op == '>':
                full_df = full_df.filter(pl.col(column) > value)
            elif op == '<':
                full_df = full_df.filter(pl.col(column) < value)
            elif op == 'in':
                # 新增IN过滤支持
                full_df = full_df.filter(pl.col(column).is_in(value))
            elif callable(op):
                # 支持自定义过滤函数
                full_df = full_df.filter(pl.col(column).map(op))

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


class MyRayContext:
    _instance = None
    
    def __new__(cls):
        """
        单例模式，确保只有一个Ray上下文实例
        """
        if not cls._instance:
            cls._instance = super(MyRayContext, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """
        初始化Ray上下文
        """
        if not self._initialized:
            try:
                # 如果Ray尚未初始化，则初始化
                if not ray.is_initialized():
                    ray.init(ignore_reinit_error=True)
                self._initialized = True
            except Exception as e:
                print(f"Ray初始化错误: {e}")
    
    def __del__(self):
        """
        对象销毁时关闭Ray
        """
        self.shutdown()
    
    def shutdown(self):
        """
        显式关闭Ray
        """
        if self._initialized and ray.is_initialized():
            ray.shutdown()
            self._initialized = False


class MyDataFrame:
    def __init__(self, directory):
        """
        初始化MyDataFrame，指定文件夹路径
        
        Args:
            directory (str): CSV文件所在的目录路径
        """
        self.directory = directory
        self.group_columns = []
        self.sum_columns = []
        self.mean_columns = []
        self.filter_conditions = []
        
        # 使用MyRayContext管理Ray上下文
        self.ray_context = MyRayContext()
    
    def groupBy(self, columns):
        """
        指定分组列
        
        Args:
            columns (list): 用于分组的列名
        
        Returns:
            self: 返回当前对象，支持链式调用
        """
        self.group_columns = columns
        return self
    
    def sum(self, columns):
        """
        指定需要求和的列
        
        Args:
            columns (list): 需要求和的列名
        
        Returns:
            self: 返回当前对象，支持链式调用
        """
        self.sum_columns = columns
        return self
    
    def mean(self, columns):
        """
        指定需要求平均的列
        
        Args:
            columns (list): 需要求平均的列名
        
        Returns:
            self: 返回当前对象，支持链式调用
        """
        self.mean_columns = columns
        return self
    
    def filter(self, column, op='==', value=None):
        """
        添加过滤条件
        
        Args:
            column (str): 要过滤的列名
            op (str, optional): 比较操作符，默认为'=='
            value (Any, optional): 比较的值
        
        Returns:
            self: 返回当前对象，支持链式调用
        """
        condition = {
            'column': column,
            'op': op,
            'value': value
        }
        self.filter_conditions.append(condition)
        return self
    
    def filter_in(self, column, values):
        """
        添加IN过滤条件
        
        Args:
            column (str): 要过滤的列名
            values (list): 可接受的值列表
        
        Returns:
            self: 返回当前对象，支持链式调用
        """
        condition = {
            'column': column,
            'op': 'in',
            'value': values
        }
        self.filter_conditions.append(condition)
        return self
    
    def compute(self, method='separate'):
        """
        触发计算并返回结果，支持过滤条件
        
        Args:
            method (str, optional): 计算方法，可选 'separate' 或 'all_at_once'
        
        Returns:
            pl.DataFrame: 计算结果
        """
        if not self.group_columns:
            raise ValueError("必须先使用groupBy指定分组列")
        
        if method == 'separate':
            result = process_files_separately(
                self.directory, 
                self.group_columns, 
                self.sum_columns, 
                self.mean_columns,
                self.filter_conditions  # 传递过滤条件
            ).sort(self.group_columns)
        elif method == 'all_at_once':
            result = process_all_at_once(
                self.directory, 
                self.group_columns, 
                self.sum_columns, 
                self.mean_columns,
                self.filter_conditions  # 传递过滤条件
            ).sort(self.group_columns)
        else:
            raise ValueError("method必须是 'separate' 或 'all_at_once'")
        
        return result
    
    def __del__(self):
        """
        对象销毁时不再直接关闭Ray
        """
        pass  # 由MyRayContext负责管理


if __name__ == "__main__":
    # 创建Ray上下文
    ray_context = MyRayContext()
    
    try:
        # 演示新的MyDataFrame使用方法
        output_dir = "student_scores"
        
        # 使用新的MyDataFrame
        df = MyDataFrame(output_dir)
        
        # 链式调用：分组、过滤、求和、求平均
        result_separate = (df.groupBy(["省份", "班级"])
                    .filter_in("省份", ["四川", "重庆"])  # 过滤省份为四川或重庆的记录
                    .sum(["语文", "数学"])
                    .mean(["化学", "生物", "地理"])
                    .compute(method='separate'))
        
        result_all_at_once = (df.groupBy(["省份", "班级"])
                    .filter_in("省份", ["四川", "重庆"])  # 过滤省份为四川或重庆的记录
                    .sum(["语文", "数学"])
                    .mean(["化学", "生物", "地理"])
                    .compute(method='all_at_once'))
        
        comparison = result_separate.equals(result_all_at_once)
        print("两种计算方式的结果是否一致:", comparison)
        
        # **输出计算结果**
        res_dir = "res_dir_avg5"
        os.makedirs(res_dir, exist_ok=True)
        temp_file1 = os.path.join(res_dir, "result_separate_in.csv")
        temp_file2 = os.path.join(res_dir, "result_all_at_once_in.csv")

        result_separate.write_csv(temp_file1)
        result_all_at_once.write_csv(temp_file2)

        print(f"分步计算结果保存至: {temp_file1}")
        print(f"一次性计算结果保存至: {temp_file2}")

    
    finally:
        # 显式关闭Ray上下文
        ray_context.shutdown()
