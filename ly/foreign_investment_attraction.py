"""
@Name: city_label.py
@Auth: LY
@Date: 2023/5/30-13:47
@Desc: 外资吸引
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
    foreign_investment_attraction_df = pd.read_excel('excel/指标体系_{}.xlsx'.format(year), sheet_name='外资吸引')
    row_count = len(foreign_investment_attraction_df)
    print(row_count)
    # 创建游标
    cursor = dbcon.cursor()
    for row_index in range(row_count):
        # 获取某一行的数据,不包含表头，0表示真正的第一行数据
        row = foreign_investment_attraction_df.loc[row_index]
        # 城市
        city = row[1]
        # 实际使用外资
        fdi = row[2]
        # 外商直接投资项目
        foreign_direct_investment_projects = row[4]
        sql = "insert into foreign_investment_attraction_" + str(year) + "(城市,实际使用外资,外商直接投资项目)values('{}','{}','{}')"
        sql1 = sql.format(city, fdi, foreign_direct_investment_projects)
        print(sql1)
        cursor.execute(sql1)
    dbcon.commit()


def calculate(year):
    """
    一个脚本把所有年的都写入
    :param year:
    :return:
    """
    foreign_investment_attraction_df = pd.read_excel('excel/指标体系_{}.xlsx'.format(year), sheet_name='外资吸引')
    # 行数
    row_count = len(foreign_investment_attraction_df)
    # 计算实际使用外资的最大值
    max_fdi = foreign_investment_attraction_df['实际使用外资'].max()
    min_fdi = foreign_investment_attraction_df['实际使用外资'].min()
    # 计算外商直接投资项目的最大值
    max_foreign_direct_investment_projects = foreign_investment_attraction_df['外商直接投资项目'].max()
    min_foreign_direct_investment_projects = foreign_investment_attraction_df['外商直接投资项目'].min()
    # 创建游标
    cursor = dbcon.cursor()
    for row_index in range(row_count):
        # 对每行进行计算
        # 获取某一行的数据,不包含表头，0表示真正的第一行数据
        row = foreign_investment_attraction_df.loc[row_index]
        # 城市
        city = row[1]
        # 实际使用外资
        fdi = row[2]
        # 外商直接投资项目
        foreign_direct_investment_projects = row[4]
        # 标准化值1(实际使用外资) 由计算得到
        standard1 = (fdi - min_fdi) / (max_fdi - min_fdi)
        # 标准化值2(外商直接投资项目) 由计算得到
        standard2 = (foreign_direct_investment_projects - min_foreign_direct_investment_projects) / (max_foreign_direct_investment_projects - min_foreign_direct_investment_projects)
        # 均分
        average_score = (standard1 + standard2) / 2
        sql = "update foreign_investment_attraction_" + str(year) + " set 标准化值1 = " + str(standard1) + ", 标准化值2 = " + str(standard2) + ", 均分 = " + str(average_score) + " where 城市 = '" + str(city) + "'"
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
        insert_basic(year)
        # 以后定时执行这个函数即可
        calculate(year)
    # 关闭连接池
    dbcon.close()
