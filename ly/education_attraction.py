"""
@Name: city_label.py
@Auth: LY
@Date: 2023/5/30-13:47
@Desc: 就学吸引
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
    education_attraction_df = pd.read_excel('excel/指标体系_{}.xlsx'.format(year), sheet_name='就学吸引')
    row_count = len(education_attraction_df)
    print(row_count)
    # year = 2020
    # 创建游标
    cursor = dbcon.cursor()
    # 获得学校权重所在行
    weight_row = education_attraction_df.loc[0]
    weight_985 = weight_row[2]
    print("weight_985 " + str(weight_985))
    weight_211 = weight_row[3]
    print("weight_211 " + str(weight_211))
    weight_ordinary_higher_school = weight_row[4]
    print("weight_ordinary_higher_school " + str(weight_ordinary_higher_school))
    weight_secondary_positions = weight_row[5]
    print("weight_secondary_positions " + str(weight_secondary_positions))
    # 从第2行开始
    for row_index in range(1, row_count):
        # 获取某一行的数据,不包含表头，0表示真正的第一行数据
        row = education_attraction_df.loc[row_index]
        # 城市
        city = row[1]
        # 985数量
        num_985 = row[2]
        # 211数量
        num_211 = row[3]
        # 普通高等学校
        num_ordinary_higher_school = row[4]
        # 中职数量
        num_secondary_positions = row[5]
        # 综合分
        comprehensive_score = num_985 * weight_985 + num_211 * weight_211 + num_ordinary_higher_school * weight_ordinary_higher_school + num_secondary_positions * weight_secondary_positions
        sql = "insert into education_attraction_" + str(year) + "(城市,985数量,211数量,普通高等学校,中职数量,综合分)values('{}','{}','{}','{}','{}','{}')"
        sql1 = sql.format(city, num_985, num_211, num_ordinary_higher_school, num_secondary_positions, comprehensive_score)
        print(sql1)
        cursor.execute(sql1)
    dbcon.commit()


def calculate(year):
    """
    一个脚本把所有年的都写入
    :param year:
    :return:
    """
    sql = "select 综合分 from education_attraction_" + str(year)
    cursor = dbcon.cursor()
    cursor.execute(sql)
    # 综合分列表
    comprehensive_scores = cursor.fetchall()
    education_attraction_df = pd.read_excel('excel/指标体系_{}.xlsx'.format(year), sheet_name='就学吸引')
    # 行数
    row_count = len(education_attraction_df)
    # 遍历获得总和分数的最大值最小值
    max_comprehensive_score = 0.0
    min_comprehensive_score = 1.0
    for i in range(row_count-2):
        comprehensive_score = comprehensive_scores[i][0]
        max_comprehensive_score = max(max_comprehensive_score, comprehensive_score)
        min_comprehensive_score = min(min_comprehensive_score, comprehensive_score)
    # 创建游标
    cursor = dbcon.cursor()
    # 获得学校权重所在行
    weight_row = education_attraction_df.loc[0]
    weight_985 = weight_row[2]
    weight_211 = weight_row[3]
    weight_ordinary_higher_school = weight_row[4]
    weight_secondary_positions = weight_row[5]
    for row_index in range(1, row_count):
        # 对每行进行计算
        # 获取某一行的数据,不包含表头，0表示真正的第一行数据
        row = education_attraction_df.loc[row_index]
        # 城市
        city = row[1]
        # 985数量
        num_985 = row[2]
        # 211数量
        num_211 = row[3]
        # 普通高等学校
        num_ordinary_higher_school = row[4]
        # 中职数量
        num_secondary_positions = row[5]
        # 综合分
        comprehensive_score = num_985 * weight_985 + num_211 * weight_211 + num_ordinary_higher_school * weight_ordinary_higher_school + num_secondary_positions * weight_secondary_positions
        # 标准化值 由计算得到
        standard = (comprehensive_score - min_comprehensive_score) / (max_comprehensive_score - min_comprehensive_score)
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
