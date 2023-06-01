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
        sheet = data.sheet_by_name("媒体影响")
    except IOError:
        print("文件读取失败")

    cols = sheet.ncols
    column = {'序号': 0, '城市': 1, '信息总量': 2, '标准化值1': 3, '正面占比': 4, '标准化值2': 5, '负面占比': 6, '标准化值4': 8, '标准化值3': 7}
    print("开始读取{}数据...".format(sheet.name))
    for i in tqdm(range(2, 35)):
        # 读取excel数据
        city = sheet.cell(i, column["城市"]).value
        infor = sheet.cell(i, column["信息总量"]).value
        num1 = sheet.cell(i, column["标准化值1"]).value
        pos = sheet.cell(i, column["正面占比"]).value
        num2 = sheet.cell(i, column["标准化值2"]).value
        neg = sheet.cell(i, column["负面占比"]).value
        num3 = sheet.cell(i, column["标准化值3"]).value
        num4 = sheet.cell(i, column["标准化值4"]).value
        value = (city, infor, num1, pos, num2, neg, num3, num4)
        insert = "INSERT INTO media_influ_2020(城市,信息总量,标准化值1,正面占比,标准化值2,负面占比,标准化值3,标准化值4) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"
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
        sheet = data.sheet_by_name("媒体影响")
    except IOError:
        print("文件读取失败")

    cols = sheet.ncols
    city_list = []
    infor_list = []
    pos_list = []
    neg_list = []
    column = {'序号': 0, '城市': 1, '信息总量': 2, '标准化值1': 3, '正面占比': 4, '标准化值2': 5, '负面占比': 6, '标准化值3': 7, '标准化值4': 8}
    for i in tqdm(range(2, 35)):
        # 读取excel数据
        city = sheet.cell(i, column["城市"]).value
        infor = sheet.cell(i, column["信息总量"]).value
        pos = sheet.cell(i, column["正面占比"]).value
        neg = sheet.cell(i, column["负面占比"]).value
        city_list.append(city)
        infor_list.append(infor)
        pos_list.append(pos)
        neg_list.append(neg)
    max_infor = max(infor_list)
    min_infor = min(infor_list)
    max_pos = max(pos_list)
    min_pos = min(pos_list)
    max_neg = max(neg_list)
    min_neg = min(neg_list)

    for i in range(0, 33):
        num1 = round((infor_list[i] - min_infor) / (max_infor - min_infor), 6)
        num2 = round((pos_list[i] - min_pos) / (max_pos - min_pos), 2)
        num3 = round((max_neg - neg_list[i]) / (max_neg - min_neg), 2)
        aver = round((num3+num2+num1)/3, 2)
        city = city_list[i]
        infor = infor_list[i]
        pos = pos_list[i]
        neg = neg_list[i]
        sql = f'UPDATE media_influ_2020 SET 信息总量={infor},标准化值1={num1},正面占比={pos},标准化值2={num2},负面占比={neg},标准化值3={num3},标准化值4={aver} where 城市 = \'{city}\' '.format(
            num1=num1, infor=infor, num2=num2, num3=num3, pos=pos, neg=neg, aver=aver)
        # print(sql)
        cursor.execute(sql)  # 执行更新操作
        database.commit()
    cursor.close()
    database.close()
    print("数据库关闭成功...")


# insert_to_db("./data/指标体系_2020.xlsx")
update("./data/指标体系_2020.xlsx")
