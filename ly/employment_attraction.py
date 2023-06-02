"""
@Name: city_label.py
@Auth: LY
@Date: 2023/5/30-13:47
@Desc: 就业吸引
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
    employment_attraction_df = pd.read_excel('excel/指标体系_{}.xlsx'.format(year), sheet_name='就业吸引')
    row_count = len(employment_attraction_df)
    print(row_count)
    # year = 2020
    # 创建游标
    cursor = dbcon.cursor()
    for row_index in range(row_count):
        # 获取某一行的数据,不包含表头，0表示真正的第一行数据
        row = employment_attraction_df.loc[row_index]
        # 城市
        city = row[1]
        # 适龄人口
        age_appropriate_population = row[2]
        # 单位从业
        employed_by_company = row[3]
        # 私营从业
        employed_by_person = row[4]
        # 平均工资
        average_wage = row[8]
        # 房价
        housing_price = row[10]
        # 就业人口
        employed_population = employed_by_person * 10000 + employed_by_company
        # 就业率
        employment_rate = employed_population / age_appropriate_population
        sql = "insert into employment_attraction_" + str(
            year) + "(城市,适龄人口,单位从业,私营从业,就业人口,就业率,平均工资,房价)values('{}','{}','{}','{}','{}','{}','{}','{}')"
        sql1 = sql.format(city, age_appropriate_population, employed_by_company, employed_by_person,
                          employed_population,
                          employment_rate, average_wage, housing_price)
        print(sql1)
        cursor.execute(sql1)
    dbcon.commit()


def calculate(year):
    """
    一个脚本把所有年的都写入
    :param year:
    :return:
    """
    sql = "select 就业率 from employment_attraction_" + str(year)
    cursor = dbcon.cursor()
    cursor.execute(sql)
    # 就业率列表
    employment_rates = cursor.fetchall()
    employment_attraction_df = pd.read_excel('excel/指标体系_{}.xlsx'.format(year), sheet_name='就业吸引')
    # 行数
    row_count = len(employment_attraction_df)
    # 就业率的最大值最小值
    max_employment_rate = 0.0
    min_employment_rate = 1.0
    # 遍历获得就业率的最大值最小值
    for i in range(row_count-3):
        employlent_rate = employment_rates[i][0]
        max_employment_rate = max(max_employment_rate, employlent_rate)
        min_employment_rate = min(min_employment_rate, employlent_rate)
    # 计算平均工资的最大值
    max_average_wage = employment_attraction_df['平均工资'].max()
    min_average_wage = employment_attraction_df['平均工资'].min()
    # 计算房价的最大值
    max_housing_price = employment_attraction_df['房价'].max()
    min_housing_price = employment_attraction_df['房价'].min()
    # 创建游标
    cursor = dbcon.cursor()
    for row_index in range(row_count):
        # 对每行进行计算
        # 获取某一行的数据,不包含表头，0表示真正的第一行数据
        row = employment_attraction_df.loc[row_index]
        # 城市
        city = row[1]
        # 适龄人口
        age_appropriate_population = row[2]
        # 单位从业
        employed_by_company = row[3]
        # 私营从业
        employed_by_person = row[4]
        # 平均工资
        average_wage = row[8]
        # 房价
        housing_price = row[10]
        # 就业人口
        employed_population = employed_by_person * 10000 + employed_by_company
        # 就业率
        employment_rate = employed_population / age_appropriate_population
        # 标准化值1(就业率标准化值) 由计算得到
        standard1 = (employment_rate - min_employment_rate) / (max_employment_rate - min_employment_rate)
        # 标准化值2(平均工资标准化值) 由计算得到
        standard2 = (average_wage - min_average_wage) / (max_average_wage - min_average_wage)
        # 标准化值3(房价标准化值) 由计算得到
        standard3 = (max_housing_price - housing_price) / (max_housing_price - min_housing_price)
        # 均分
        average_score = (standard1 + standard2 + standard3) / 3
        if not pd.isna(standard1):
            print(standard1)
            sql1 = "update employment_attraction_" + str(year) + " set 标准化值1 = " + str(standard1) + " , 标准化值2 = " + str(
                standard2) + " , 标准化值3 = " + str(standard3) + " , 均分 = " + str(average_score) + " where 城市 = '" + str(
                city) + "'"
            print(sql1)
        cursor.execute(sql1)
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
