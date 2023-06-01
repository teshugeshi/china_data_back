import sys
import xlrd
import pymysql
from tqdm import tqdm


# 连接数据库
def connect_db():
    try:
        # 数据库连接
        database = pymysql.connect(host='cgo.culturecompute.com', user='root', passwd='Ccbupt1234!', db='china_data',
                                   port=3306,
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
        sheet = data.sheet_by_name("文化服务")
    except IOError:
        print("文件读取失败")

    cols = sheet.ncols
    column = {'序号': 0, '城市': 1, '常住人口': 2, '博物馆数量': 3, '人均1': 4, '标准化值1': 5, '图书馆藏书': 6, '标准化值2': 8, '人均2': 7,
              '公园绿地面积': 9, '人均3': 10, '标准化值3': 11, '平均': 12}
    print("开始读取{}数据...".format(sheet.name))
    for i in tqdm(range(1, 34)):
        # 读取excel数据
        city = sheet.cell(i, column["城市"]).value
        people = sheet.cell(i, column["常住人口"]).value
        museum = sheet.cell(i, column["博物馆数量"]).value
        aver1 = sheet.cell(i, column["人均1"]).value
        num1 = sheet.cell(i, column["标准化值1"]).value
        book = sheet.cell(i, column["图书馆藏书"]).value
        aver2 = sheet.cell(i, column["人均2"]).value
        num2 = sheet.cell(i, column["标准化值2"]).value
        area = sheet.cell(i, column["公园绿地面积"]).value
        aver3 = sheet.cell(i, column["人均3"]).value
        num3 = sheet.cell(i, column["标准化值3"]).value
        aver = sheet.cell(i, column["平均"]).value
        value = (city, people, museum, aver1, num1, book, aver2, num2, area, aver3, num3, aver)
        insert = "INSERT INTO culture_2021(城市,常住人口,博物馆数量,人均1,标准化值1,图书馆藏书,人均2,标准化值2,公园绿地面积,人均3,标准化值3,平均) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
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
        sheet = data.sheet_by_name("文化服务")
    except IOError:
        print("文件读取失败")

    cols = sheet.ncols
    city_list = []
    people_list = []
    museum_list = []
    aver1_list = []
    aver2_list = []
    aver3_list = []
    area_list = []
    book_list = []
    column = {'序号': 0, '城市': 1, '常住人口': 2, '博物馆数量': 3, '人均1': 4, '标准化值1': 5, '图书馆藏书': 6, '人均2': 7, '标准化值2': 8,
              '公园绿地面积': 9, '人均3': 10, '标准化值3': 11, '平均': 12}
    for i in tqdm(range(1, 34)):
        # 读取excel数据
        city = sheet.cell(i, column["城市"]).value
        people = sheet.cell(i, column["常住人口"]).value
        museum = sheet.cell(i, column["博物馆数量"]).value
        aver1 = round(museum*1000000/people, 6)
        book = sheet.cell(i, column["图书馆藏书"]).value
        aver2 = book*10000/people
        area = sheet.cell(i, column["公园绿地面积"]).value
        aver3 = area*10000/people
        aver1_list.append(aver1)
        aver2_list.append(aver2)
        aver3_list.append(aver3)
        book_list.append(book)
        city_list.append(city)
        area_list.append(area)
        museum_list.append(museum)
        people_list.append(people)

    max_aver1 = max(aver1_list)
    min_aver1 = min(aver1_list)
    max_aver2 = max(aver2_list)
    min_aver2 = min(aver2_list)
    max_aver3 = max(aver3_list)
    min_aver3 = min(aver3_list)
    for i in range(0, 33):
        num1 = round((aver1_list[i] - min_aver1) / (max_aver1 - min_aver1), 6)
        num2 = round((aver2_list[i] - min_aver2) / (max_aver2 - min_aver2), 6)
        num3 = round((aver3_list[i] - min_aver3) / (max_aver3 - min_aver3), 6)
        city = city_list[i]
        people = people_list[i]
        museum = museum_list[i]
        book = book_list[i]
        area = area_list[i]
        aver = round((num3 + num2 + num1) / 3, 6)
        sql = f'UPDATE culture_2020 SET 常住人口={people},博物馆数量={museum},人均1={aver1},标准化值1={num1},图书馆藏书={book},人均2={aver2},标准化值2={num2},公园绿地面积={area},人均3={aver3},标准化值3={num3},平均={aver} where 城市 = \'{city}\' '.format(
            num1=num1, people=people, num2=num2, num3=num3, museum=museum, area=area, book=book, aver=aver, aver1=aver1, aver2=aver2, aver3=aver3)
        print(sql)
        cursor.execute(sql)  # 执行更新操作
        database.commit()
    cursor.close()
    database.close()
    print("数据库关闭成功...")


# insert_to_db("./data/指标体系_2021.xlsx")
update("./data/指标体系_2020.xlsx")
