"""
@Name: city_label.py
@Auth: LY
@Date: 2023/5/30-13:47
@Desc: 旅游吸引
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
    tourist_attraction_df = pd.read_excel('excel/指标体系_{}.xlsx'.format(year), sheet_name='旅游吸引')
    row_count = len(tourist_attraction_df)
    print(row_count)
    # year = 2020
    # 创建游标
    cursor = dbcon.cursor()
    for row_index in range(row_count):
        # 获取某一行的数据,不包含表头，0表示真正的第一行数据
        row = tourist_attraction_df.loc[row_index]
        # 城市
        city = row[1]
        # 年游客数量(万人)
        annual_tourists_num = row[2]
        # 备注
        notes = row[4] if str(row[4]) != "nan" else ""
        sql = "insert into tourist_attraction_" + str(year) + "(城市,年游客数量,备注)values('{}','{}','{}')"
        sql1 = sql.format(city, annual_tourists_num, notes)
        print(sql1)
        cursor.execute(sql1)
    dbcon.commit()


def calculate(year):
    """
    一个脚本把所有年的都写入
    :param year:
    :return:
    """
    tourist_attraction_df = pd.read_excel('excel/指标体系_{}.xlsx'.format(year), sheet_name='旅游吸引')
    # 行数
    row_count = len(tourist_attraction_df)
    # 计算年游客数量(万人)的最大值最小值
    max = tourist_attraction_df['年游客数量（万人）'].max()
    min = tourist_attraction_df['年游客数量（万人）'].min()
    # 创建游标
    cursor = dbcon.cursor()
    for row_index in range(row_count):
        # 对每行进行计算
        # 获取某一行的数据,不包含表头，0表示真正的第一行数据
        row = tourist_attraction_df.loc[row_index]
        # 城市
        city = row[1]
        # 年游客数量(万人)
        annual_tourists_num = row[2]
        # 标准化值 由计算得到
        standard = (annual_tourists_num - min) / (max - min)
        if not pd.isna(standard):        
            print(standard)
            sql = "update tourist_attraction_" + str(year) + " set 标准化值 = " + str(standard) + " where 城市 = '" + str(city) + "'"
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
