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
        sheet = data.sheet_by_name("主流媒体")
    except IOError:
        print("文件读取失败")

    cols = sheet.ncols
    column = {'序号': 0, '城市': 1, '对象': 2, '融媒体阅读量': 3, '标准化值1': 4, '标准化值2': 6, '市级频道占有率': 5, '平均值': 7}
    print("开始读取{}数据...".format(sheet.name))
    for i in tqdm(range(1, 34)):
        # 读取excel数据
        city = sheet.cell(i, column["城市"]).value
        obj = sheet.cell(i, column["对象"]).value
        read = sheet.cell(i, column["融媒体阅读量"]).value
        num1 = sheet.cell(i, column["标准化值1"]).value
        occupancy = sheet.cell(i, column["市级频道占有率"]).value
        num2 = sheet.cell(i, column["标准化值2"]).value
        aver = sheet.cell(i, column["平均值"]).value
        value = (city, obj, read, num1, occupancy, num2, aver)
        insert = "INSERT INTO main_media_2020(城市,对象,融媒体阅读量,标准化值1,市级频道占有率,标准化值2,平均值) VALUES(%s,%s,%s,%s,%s,%s,%s)"
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
        sheet = data.sheet_by_name("主流媒体")
    except IOError:
        print("文件读取失败")

    cols = sheet.ncols
    city_list = []
    read_list = []
    occup_list = []
    column = {'序号': 0, '城市': 1, '对象': 2, '融媒体阅读量': 3, '标准化值1': 4, '标准化值2': 6, '市级频道占有率': 5, '平均值': 7}
    for i in tqdm(range(1, 34)):
        # 读取excel数据
        city = sheet.cell(i, column["城市"]).value
        print(city)
        read = sheet.cell(i, column["融媒体阅读量"]).value
        print(read)
        occupancy = sheet.cell(i, column["市级频道占有率"]).value
        print(occupancy)
        city_list.append(city)
        read_list.append(read)
        occup_list.append(occupancy)
    max_read = max(read_list)
    min_read = min(read_list)
    max_occup = max(occup_list)
    min_occup = min(occup_list)
    for i in range(0, 33):
        num1 = round((read_list[i]-min_read)/(max_read-min_read), 6)
        num2 = round((occup_list[i]-min_occup)/(max_occup-min_occup), 2)
        city = city_list[i]
        read = read_list[i]
        occupancy = occup_list[i]
        aver = round((num1+num2)/2, 2)
        sql = f'UPDATE main_media_2020 SET 融媒体阅读量={read},标准化值1={num1},市级频道占有率={occupancy},标准化值2={num2},平均值={aver} where 城市 = \'{city}\' '.format(num1=num1, read=read, num2=num2, occupancy=occupancy, aver=aver)
        print(sql)
        cursor.execute(sql)  # 执行更新操作
        database.commit()
    cursor.close()
    database.close()
    print("数据库关闭成功...")


# insert_to_db("./data/指标体系_2020.xlsx")
update("./data/指标体系_2020.xlsx")