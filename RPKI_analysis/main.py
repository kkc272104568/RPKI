# -*- coding: utf-8 -*-
import os
from collections import Counter
from datetime import datetime
import pandas as pd
import numpy as np
from numpy import sort
from pandas import DataFrame

file_path = r"F:\博1 课程\TC3WG2工作组\RPKI_job\20211018"


def file_info(file_dir):
    """根据文件地址，获取文件基本信息"""
    file_list, name_list = list(), list()
    for root, dirs, files in os.walk(file_dir):
        file_list = [root + "\\" + i for i in files]
        name_list = [str(i).split(".")[0] for i in files]
    return file_list, name_list


def file_read(file_name):
    """获取csv数据"""
    pd_date = pd.read_csv(file_name)
    df_date = DataFrame(pd_date)
    return df_date


def save_to_excel(date):
    """数据存储至excel"""
    date.to_excel("result_20211018_1.xls", index=False, sheet_name="result")
    

def main_fun():
    """主函数"""
    files_list, names_list = file_info(file_path)
    total_df = None
    total_pd = None
    total_info = {"max_prefix": 0}
    for _, file in enumerate(files_list):
        pd_info = file_read(file)

        # 各州信息分析
        base_info = dict()
        base_info["ca"] = names_list[_]
        base_info["asn_nums"] = len(pd_info["ASN"].drop_duplicates())
        base_info["prefix_nums"] = len(pd_info["ASN"])
        base_info["roa_nums"] = len(pd_info["URI"].drop_duplicates())  # roa 数量
        base_info["max_prefix"] = Counter(pd_info["URI"]).most_common(1)[0][1]
        print(Counter(pd_info["URI"]).most_common())
        base_info["max_prefix_asn"] = Counter(pd_info["ASN"]).most_common(1)[0][0]
        pd_info["day_span"] = pd.to_datetime(pd_info["Not After"])-pd.to_datetime(pd_info["Not Before"])
        pd_info["day_span"] = pd_info['day_span'].map(lambda x: x/np.timedelta64(1, 'D'))
        pd_info = pd_info.sort_values("day_span", ascending=False)
        base_info["max_lifetime"] = int(pd_info["day_span"].values[0])
        base_info["max_lifetime_asn"] = pd_info["ASN"].values[0]
        base_info["min_lifetime"] = int(pd_info["day_span"].values[-1])
        base_info["min_lifetime_asn"] = pd_info["ASN"].values[-1]
        base_info["average_prefix"] = format(base_info["prefix_nums"]/base_info["roa_nums"], '.2f')
        base_info["average_lifetime"] = format(pd_info["day_span"].mean(), '.2f')

        # 各州信息汇总
        data_df = pd.DataFrame(base_info, index=[_])
        total_df = pd.concat([total_df, data_df])

        # 各种数据汇总
        total_pd = pd.concat([total_pd, pd_info])

        # 汇总数据分析
        total_df = total_df.sort_values("max_prefix", ascending=False)
        total_info["max_prefix"] = int(total_df["max_prefix"].values[0])
        total_info["max_prefix_asn"] = total_df["max_prefix_asn"].values[0]

        total_df = total_df.sort_values("max_lifetime", ascending=False)
        total_info["max_lifetime"] = int(total_df["max_lifetime"].values[0])
        total_info["max_lifetime_asn"] = total_df["max_lifetime_asn"].values[0]

        total_df = total_df.sort_values("min_lifetime")
        total_info["min_lifetime"] = int(total_df["min_lifetime"].values[0])
        total_info["min_lifetime_asn"] = total_df["min_lifetime_asn"].values[0]

    # 汇总数据分析
    total_info["ca"] = "total"
    total_info["asn_nums"] = len(total_pd["ASN"].drop_duplicates())
    total_info["prefix_nums"] = len(total_pd["ASN"])
    total_info["roa_nums"] = len(total_pd["URI"].drop_duplicates())
    total_info["average_prefix"] = format(total_info["prefix_nums"] / total_info["roa_nums"], '.2f')
    total_info["average_lifetime"] = format(total_pd["day_span"].mean(), '.2f')

    total_info = pd.DataFrame(total_info, index=[5])
    total_df = pd.concat([total_df, total_info])
    save_to_excel(total_df)


if __name__ == "__main__":
    main_fun()
