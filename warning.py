import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime

EXCEL_PATH = 'C:\\Users\\ennin\\Desktop\\work\\Documents\\新车落地服务20180907 (1).xls'
NEW_PATH = 'C:\\Users\\ennin\\Desktop\\work\\Documents\\test_data2.xlsx'
NODE_NAMES = ['订单匹配时间', '通知出库时间', '开票通知时间', '实际出库时间', '开票时间', '发合寄件时间', '交强险购买时间', '商业险购买时间',
'交强险寄件时间', '购置税申请时间', '购置税打款时间', '发合收件时间', '购置税收款时间', '接车工单完成时间', '交强险收件时间']
INDEX_DATA = [1, 2, 3, 4, 5, 15, 8, 9, 10, 14, 12, 11, 13]
LAST_NODES = [9, 10, 11, 12]
NEED_STRIP = [1, 3, 4]

# route index
path_index_0 = {0, 1, 3, 5, 6, 9}
path_index_1 = {0, 1, 3, 4, 7, 8, 10}
path_index_2 = {0, 1, 3, 4, 11}
path_index_3 = {0, 2, 12}
path_index = [path_index_0, path_index_1, path_index_2, path_index_3]

# route distance
path_time_0 = [1.44, 1.3, 0.93, 0.31, 1.55]
path_time_1 = [1.44, 1.3, 1.18, 1.01, 0.3, 0.66]
path_time_2 = [1.44, 1.3, 1.18, 1.9]
path_time_3 = [1.84, 3.62]
path_time = [path_time_0, path_time_1, path_time_2, path_time_3]

# order
path_list_0 = [0, 1, 3, 5, 6, 9]
path_list_1 = [0, 1, 3, 4, 7, 8, 10]
path_list_2 = [0, 1, 3, 4, 11]
path_list_3 = [0, 2, 12]
path_list = [path_list_0, path_list_1, path_list_2, path_list_3]


path_column_0 = ['L_2', 'L_3', 'L_9', 'L_10', 'L_11']
path_column_1 = ['L_2', 'L_3', 'L_4', 'L_6', 'L_7', 'L_8']
path_column_2 = ['L_2', 'L_3', 'L_4', 'L_5']
path_column_3 = ['L_0', 'L_1']
path_column = [path_column_0, path_column_1, path_column_2, path_column_3]

normal_data = pd.read_csv('normal_data.csv')


def delay_probability(path_list_index, warning_node_index, now_time, node_time, early_time):
    select_path_column = path_column[path_list_index]
    current_l = select_path_column[warning_node_index - 1]
    remain_l = select_path_column[warning_node_index:]
    current_l_df = normal_data[current_l]/24
    remain_l_df = normal_data[remain_l]/24
    past_time = now_time - early_time
    remain_time_max = 3 - past_time
    node_past_time = now_time - node_time
    current_l_df = current_l_df - node_past_time
    current_l_df = np.maximum(current_l_df, 0)
    sum_df = current_l_df + remain_l_df.T.sum()
    if remain_time_max <= 0:
        probability = 1
    else:
        z_score = (remain_time_max - sum_df.mean()) / sum_df.std()
        probability = 1 - stats.norm.cdf(z_score)
    return probability


class Stack(object):
    # 初始化栈为空列表
    def __init__(self):
        self.items = []

    # 判断栈是否为空，返回布尔值
    def is_empty(self):
        return self.items == []

    # 返回栈顶元素
    def peek(self):
        return self.items[len(self.items) - 1]

    # 返回栈的大小
    def size(self):
        return len(self.items)

    # 把新的元素堆进栈里面（程序员喜欢把这个过程叫做压栈，入栈，进栈……）
    def push(self, item):
        self.items.append(item)

    # 把栈顶元素丢出去（程序员喜欢把这个过程叫做出栈……）
    def pop(self):
        return self.items.pop()


def load_data(data_path, sheet_name):
    raw_data = pd.read_excel(data_path, sheet_name=sheet_name)
    return raw_data


def warning(sorted_node, property_stack, node_times, start_time):
    result_set = set()
    tmp_set = set()
    # processing_nodes = []
    # 判断有没有路径完成
    not_finished = True
    # 记录首次到达的节点
    first_node = -1
    for node in sorted_node:
        # 判断当前node 有没有被移除
        removed = False
        while not removed:
            # 当告警的节点已经响应，从告警池中删除
            if node in result_set:
                result_set.remove(node)
            # 当前node 在栈顶集合
            if not property_stack.is_empty() and node in property_stack.peek():
                property_stack.peek().remove(node)
                removed = True
                # 判断是否有路径完成
                if not_finished and node in LAST_NODES:
                    not_finished = False
                    first_node = node

            # tmp_set 用于存放告警节点
            elif node not in tmp_set:
                pop_set = property_stack.pop()
                result_set = result_set | pop_set
                tmp_set = tmp_set | pop_set

            else:
                removed = True
            if not property_stack.is_empty() and not property_stack.peek():
                property_stack.pop()

    if not not_finished:
        # 获取当前告警节点
        curr_warning = filter_result_fixed(result_set)
        tmp_warning = curr_warning.copy()
        for warning_node in curr_warning:
            # 遍历各个路径
            tmp_remain_time_list = []
            remove_warning = True
            for path_list_index, path_list_ in enumerate(path_list):
                if warning_node in path_list_:
                    # 获取当前节点索引
                    warning_node_index = path_list_.index(warning_node)
                    ########### plan A ###########
                    # 差值
                    # node_times[sorted_node[-1]] todo 需要用now替换
                    delta = path_time[path_list_index][warning_node_index - 1] - (
                                    node_times[sorted_node[-1]] - node_times[path_list_[warning_node_index - 1]])
                    part_sum = sum(path_time[path_list_index][warning_node_index:])
                    delta_2 = node_times[sorted_node[-1]] - node_times[first_node]
                    tmp_remain_time = 3 - (max(0, delta) + part_sum + delta_2)
                    tmp_remain_time_list.append(tmp_remain_time)
                    ########### plan B ###########
                    # # node_times[sorted_node[-1]] todo 需要用now替换
                    probability_ = delay_probability(path_list_index, warning_node_index, node_times[sorted_node[-1]],
                                                     node_times[path_list_[warning_node_index - 1]], node_times[first_node])
                    print('path: {}, node num: {}, probability: {}.'.format(path_list_index, warning_node, probability_))
            if tmp_remain_time_list:
                for data_ in tmp_remain_time_list:
                    if data_ < 0:
                        remove_warning = False
                if remove_warning:
                    tmp_warning.remove(warning_node)

    else:
        tmp_warning = filter_result_fixed(result_set)

    return tmp_set, result_set, tmp_warning


# 过滤初步告警节点，保留每条路径上优先级最高的节点
def filter_result_fixed(result):
    last_node = set()
    for path in path_index:
        tmp_set = result & path
        if len(tmp_set) > 1:
            tmp_min = 9
            for i_node in tmp_set:
                if i_node < tmp_min:
                    tmp_min = i_node
            last_node = last_node | {tmp_min}
        else:
            last_node = last_node | tmp_set
    return last_node


def init_stack():
    property_1 = {0}
    property_2 = {1, 2, 3}
    property_3 = {4, 5, 6}
    property_4 = {7, 8}
    property_5 = {9, 10, 11, 12}
    property_stack = Stack()
    property_stack.push(property_5)
    property_stack.push(property_4)
    property_stack.push(property_3)
    property_stack.push(property_2)
    property_stack.push(property_1)
    return property_stack


def process_raw_data(one_line_data):
    # 1. strip [0, 2, 3]
    for part_data_01_index in NEED_STRIP:
        part_data_01 = one_line_data[part_data_01_index]
        if part_data_01 is not np.nan:
            one_line_data[part_data_01_index] = strip_data(part_data_01)

    # # 1
    # for index, data_ in enumerate(one_line_data):
    #     if data_ is not np.nan and len(data_) != 19:
    #         one_line_data[index] = strip_data(data_)

    # 2. convert str time to datetime time
    for index, data_ in enumerate(one_line_data):
        one_line_data[index] = convert_to_datetime(data_)

    # 3. calculate diff time
    start_time = one_line_data[0]
    for index, data_ in enumerate(one_line_data):
        one_line_data[index] = diff_time(data_, start_time)

    # 4. 计算车险购买时间
    one_line_data = np.append(one_line_data, max(one_line_data[6], one_line_data[7]))

    # 5. finished
    processed_data = one_line_data[INDEX_DATA]
    return processed_data, start_time


# 截取时间尾部的.0
# eg. 2018-08-10 12:17:13.0 to 2018-08-10 12:17:13
def strip_data(tar):
    tar = str(tar)
    return tar[:19]


# 将str类型的time数据转化为datetime类型的数据
def convert_to_datetime(str_data):
    if str_data is not np.nan:
        str_data = datetime.strptime(str_data, '%Y-%m-%d %H:%M:%S')
    return str_data


# 计算diff time
def diff_time(date_time, start):
    if date_time is not np.nan:
        date_time -= start
        date_time = round(date_time.total_seconds() / 60 / 60 / 24, 2)
    return date_time


# 排序
def sort_index(node_times_):
    node_times_pd = pd.Series(node_times_)
    arrived = len(node_times_pd.dropna())
    sorted_arg = np.argsort(node_times_)
    sorted_index = sorted_arg[:arrived]
    return sorted_index


# convert now to datetime
def convert_now(start_time):
    now = datetime.now()
    now_strip = strip_data(str(now))
    now = datetime.strptime(now_strip, '%Y-%m-%d %H:%M:%S')
    # start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    diff_time_ = now - start_time
    diff_time_ = round(diff_time_.total_seconds() / 60 / 60 / 24, 2)
    return diff_time_


def single_data_test(single_data):
    processed_data_ = single_data
    start_time_ = ''
    need_index = sort_index(processed_data_)
    property_stack_ = init_stack()
    tmp_set_, result_set_, tmp_warning_ = warning(need_index, property_stack_, processed_data_, start_time_)
    return tmp_set_, result_set_, tmp_warning_


def single_data_test_(single_data_):
    processed_data_, start_time_ = process_raw_data(single_data_)
    need_index = sort_index(list(processed_data_))
    property_stack_ = init_stack()
    tmp_set_, result_set_, tmp_warning_ = warning(need_index, property_stack_, processed_data_, start_time_)
    return processed_data_, tmp_set_, result_set_, tmp_warning_


def main():
    # process raw data
    excel_data = load_data(EXCEL_PATH, 'Sheet1')
    need_data = excel_data[NODE_NAMES]

    for index, data_ in enumerate(need_data.values):
        processed_data_a, tmp_set_a, result_set_a, tmp_warning_a = single_data_test_(data_)
        if 0 not in tmp_set_a and tmp_warning_a:
            print(index)
            print(processed_data_a)
            print(tmp_set_a)
            print(result_set_a)
            print(tmp_warning_a)
            print('-----------------------------------------')


def main_():
    single_data_a = [4.92, 5.78, np.nan, 5.81, np.nan, 7.07, 7.55, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]
    tmp_set_b, result_set_b, tmp_warning_b = single_data_test(single_data_a)
    print(tmp_set_b)
    print(result_set_b)
    print(tmp_warning_b)


if __name__ == '__main__':
    main()