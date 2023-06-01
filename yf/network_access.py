import sys
import xlrd
import pymysql
from tqdm import tqdm


# 连接数据库
def connect_db():
    try:
        # 数据库连接
        database = pymysql.connect(host='cgo.culturecompute.com', user='root', passwd='Ccbupt1234!', db='china_data', port=3306,
                       charset='utf8')
        print("数据库连接成功")
        return database
    except pymysql.err.Error:
        print("数据库连接失败")


# 将数据插入到数据库中
def insert_to_db(path):
    print("连接数据库")
    database = connect_db()
    cursor = database.cursor()

    data = None
    sheet = None
    try:
        data = xlrd.open_workbook(path)
    except FileNotFoundError:
        print("excel文件{}打开失败".format(path))
    try:
        sheet = data.sheet_by_name("网络接入")
    except IOError:
        print("文件读取失败")

    cols = sheet.ncols
    column = {}
    for i in range(0, cols):
        column_name = sheet.cell(0, i).value
        column[column_name] = i
    print("开始读取{}数据...".format(sheet.name))
    for i in tqdm(range(1, 34)):
        # 读取excel数据
        city = sheet.cell(i, column["城市"]).value
        people = sheet.cell(i, column["常住人口"]).value
        network = sheet.cell(i, column["网络接入"]).value
        aver = sheet.cell(i, column["人均"]).value
        num = sheet.cell(i, column["标准化值"]).value
        value = (city, people, network, aver, num)
        insert = "INSERT INTO network_access_2021(城市,常住人口,网络接入,人均,标准化值) VALUES(%s,%s,%s,%s,%s)"
        cursor.execute(insert, value)
        print("\r数据{}插入成功".format(i), end="")
        database.commit()
    cursor.close()
    database.close()
    print("数据库关闭成功...")


def update(path):
    print("连接数据库")
    database = connect_db()
    cursor = database.cursor()

    data = None
    sheet = None
    try:
        data = xlrd.open_workbook(path)
    except FileNotFoundError:
        print("excel文件{}打开失败".format(path))
    try:
        sheet = data.sheet_by_name("网络接入")
    except IOError:
        print("文件读取失败")

    cols = sheet.ncols
    column = {}
    people_list = []
    network_list = []
    aver_list = []
    for i in range(0, cols):
        column_name = sheet.cell(0, i).value
        column[column_name] = i
    for i in tqdm(range(1, 34)):
        # 读取excel数据
        city = sheet.cell(i, column["城市"]).value
        people = sheet.cell(i, column["常住人口"]).value
        network = sheet.cell(i, column["网络接入"]).value
        people_list.append(people)
        network_list.append(network)
        aver = round(network*10000/people, 6)
        aver_list.append(aver)

    max_value = max(aver_list)
    min_value = min(aver_list)
    for i in range(0, 33):
        num = round((aver_list[i]-min_value)/(max_value-min_value), 6)
        network = network_list[i]
        people = people_list[i]
        aver = aver_list[i]
        sql = f'UPDATE network_access_2020 SET 常住人口={people}, 网络接入={network}, 人均={aver}, 标准化值={num} where 城市 = \'{city}\' '.format(num=num, people=people, network=network, aver=aver)
        # print(sql)
        cursor.execute(sql)  # 执行更新操作
        database.commit()
    cursor.close()
    database.close()
    print("数据库关闭成功...")


# insert_to_db("./data/指标体系_2021.xlsx")
update("./data/指标体系_2020.xlsx")