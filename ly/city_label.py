"""
@Name: city_label.py
@Auth: LY
@Date: 2023/5/30-13:47
@Desc: 城市标签
@Ver : 0.0.1
"""


import pandas as pd
import pymysql

dbcon = pymysql.connect(
    host="39.107.97.152",
    user="root",
    password="Ccbupt1234!",
    db="china_data",
    port=3306,
    charset='utf8mb4',
    connect_timeout=10,
)


def insert_basic(year):
    """
    每个脚本 每年 只需要第一次执行即可
    :return:
    """
    city_label_df = pd.read_excel('excel/指标体系_{}.xlsx'.format(year), sheet_name='城市标签')
    row_count = len(city_label_df)
    print(row_count)
    # year = 2020
    # 创建游标
    cursor = dbcon.cursor()
    for row_index in range(row_count):
        # 获取某一行的数据,不包含表头，0表示真正的第一行数据
        row = city_label_df.loc[row_index]
        # 城市
        city = row[1]
        # 城市标签打分
        city_label_score = row[2]
        sql = "insert into city_label_" + str(year) + "(城市,城市标签打分)values('{}','{}')"
        sql1 = sql.format(city, city_label_score)
        print(sql1)
        cursor.execute(sql1)
    dbcon.commit()


def calculate(year):
    """
    一个脚本把所有年的都写入
    :param year:
    :return:
    """
    city_label_df = pd.read_excel('excel/指标体系_{}.xlsx'.format(year), sheet_name='城市标签')
    # 行数
    row_count = len(city_label_df)
    # print(row_count)
    # 列数
    column_count = len(city_label_df.columns)
    # print(column_count)
    # 得到某一列的所有内容
    # col_data = group_emotions_df.iloc[:, 0]
    # print(col_data)
    # 计算某列的最大值
    max = city_label_df['城市标签打分'].max()
    # print("max=" + str(max))
    min = city_label_df['城市标签打分'].min()
    # print("min=" + str(min))
    # year = 2020
    # 创建游标
    cursor = dbcon.cursor()
    for row_index in range(row_count):
        # 对每行进行计算
        # print("这是第" + str(row_index) + "行")
        # 获取某一行的数据,不包含表头，0表示真正的第一行数据
        row = city_label_df.loc[row_index]
        # 城市
        city = row[1]
        # 城市标签打分
        city_label_score = row[2]
        # 标准化值 由计算得到
        standard = (city_label_score - min) / (max - min)
        if not pd.isna(standard):
            print(standard)
            sql = "update education_attraction_" + str(year) + " set 标准化值 = " + str(standard) + " where 城市 = '" + str(city) + "'"
            print(sql)
        cursor.execute(sql)
    dbcon.commit()


if __name__ == '__main__':
    # 从数据库中读取所有年份
    sql = "select year from year"
    cursor = dbcon.cursor()
    cursor.execute(sql)
    years = cursor.fetchall()
    # years格式为(('2020',), ('2021',)) years[0]格式为('2020',) years[0][0]格式为 2020
    print(len(years))
    for i in range(len(years)):
        # 2020 2021
        year = years[i][0]
        # 每一个新年份先执行一遍该函数，向对应的年份表中插入数据
        # insert_basic(year)
        # 以后定时执行这个函数即可
        calculate(year)
    # 关闭连接池
    dbcon.close()
