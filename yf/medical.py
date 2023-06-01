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
        sheet = data.sheet_by_name("医疗服务")
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
        rank = sheet.cell(i, column["医疗硬件环境（排名）"]).value
        num = sheet.cell(i, column["标准化值"]).value
        value = (city, rank, num)
        insert = "INSERT INTO medical_2021(城市,医疗硬件环境（排名）,标准化值) VALUES(%s,%s,%s)"
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
        sheet = data.sheet_by_name("医疗服务")
    except IOError:
        print("文件读取失败")

    cols = sheet.ncols
    column = {}
    city_list = []
    rank_list = []
    for i in range(0, cols):
        column_name = sheet.cell(0, i).value
        column[column_name] = i
    for i in tqdm(range(1, 34)):
        # 读取excel数据
        city = sheet.cell(i, column["城市"]).value
        rank = sheet.cell(i, column["医疗硬件环境（排名）"]).value
        city_list.append(city)
        rank_list.append(rank)

    max_value = max(rank_list)
    min_value = min(rank_list)
    for i in range(0, 33):
        num = round((max_value-rank_list[i])/(max_value-min_value), 6)
        city = city_list[i]
        rank = rank_list[i]
        sql = f'UPDATE medical_2020 SET 医疗硬件环境（排名）={rank}, 标准化值={num} where 城市 = \'{city}\' '.format(num=num, rank=rank, city=city)
        # print(sql)
        cursor.execute(sql)  # 执行更新操作
        database.commit()
    cursor.close()
    database.close()
    print("数据库关闭成功...")


# insert_to_db("./data/指标体系_2021.xlsx")
update("./data/指标体系_2020.xlsx")