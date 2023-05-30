import mysql.connector

def get_standard_value(table,field,result):
    # 连接到MySQL数据库
    cnx = mysql.connector.connect(
        host="localhost",
        user="root",
        password="123456",
        database="china_data"
    )
    # 创建游标对象
    cursor = cnx.cursor()
    # 查询数据库中的数据
    query = "SELECT "+field+" FROM "+table
    cursor.execute(query)
    # 获取查询结果
    rows = cursor.fetchall()
    values = [row[0] for row in rows]
    min_value = min(values)
    max_value = max(values)
    # 更新每行数据中的新字段
    update_query = "UPDATE "+table+" SET "+result+" = (%s) WHERE "+field+" = (%s)"
    for row in rows:
        v = row[0]
        normalized_v = (v-min_value) / (max_value-min_value)
        values = (normalized_v, v)
        cursor.execute(update_query, values)
    # 提交更改到数据库
    cnx.commit()
    # 关闭游标和数据库连接
    cursor.close()
    cnx.close()

def get_average_value(table,field1,field2,result):
    # 连接到MySQL数据库
    cnx = mysql.connector.connect(
        host="localhost",
        user="root",
        password="123456",
        database="china_data"
    )
    # 创建游标对象
    cursor = cnx.cursor()
    # 查询数据库中的数据
    query = "SELECT "+field1+","+field2+","+field1+"+"+field2+" FROM "+table
    cursor.execute(query)
    # 获取查询结果
    rows = cursor.fetchall()
    # 更新每行数据中的新字段
    update_query = "UPDATE "+table+" SET "+result+" = (%s) WHERE "+field1+" = (%s)"+" AND "+field2+" = (%s)"
    for row in rows:
        values = (row[2]/2, row[0],row[1])
        cursor.execute(update_query, values)
    # # 提交更改到数据库
    cnx.commit()
    # 关闭游标和数据库连接
    cursor.close()
    cnx.close()

def get_division_value(table,field1,field2,result):
    # 连接到MySQL数据库
    cnx = mysql.connector.connect(
        host="localhost",
        user="root",
        password="123456",
        database="china_data"
    )
    # 创建游标对象
    cursor = cnx.cursor()
    # 查询数据库中的数据
    query = "SELECT "+field1+","+field2+","+field1+"/"+field2+" FROM "+table
    cursor.execute(query)
    # 获取查询结果
    rows = cursor.fetchall()
    # 更新每行数据中的新字段
    update_query = "UPDATE "+table+" SET "+result+" = (%s) WHERE "+field1+" = (%s)"+" AND "+field2+" = (%s)"
    for row in rows:
        values = (row[2], row[0],row[1])
        cursor.execute(update_query, values)
    # # 提交更改到数据库
    cnx.commit()
    # 关闭游标和数据库连接
    cursor.close()
    cnx.close()

def get_weight_value(table,weights,fields,result):
    # 连接到MySQL数据库
    cnx = mysql.connector.connect(
        host="localhost",
        user="root",
        password="123456",
        database="china_data"
    )
    # 创建游标对象
    cursor = cnx.cursor()
    num=len(weights)
    update_query = "UPDATE "+table+" SET "+'result'+"="
    for n in range(num):
        update_query=update_query+weights[n]+'*'+fields[n]+'+'
    update_query=update_query[:-1]
    print(update_query)
    cursor.execute(update_query)
    cnx.commit()
    # 关闭游标和数据库连接
    cursor.close()
    cnx.close()

if __name__ == '__main__':
    get_division_value('ecology_2020','水资源','常住人口','人均')
    get_standard_value('ecology_2020','人均','标准化值')
    get_average_value('ecology_2020','环境舒适度','标准化值','均分')
    get_standard_value('ecology_2020','均分','标准化得分')
    get_division_value('ecology_2021','水资源','常住人口','人均')
    get_standard_value('ecology_2021','人均','标准化值')
    get_average_value('ecology_2021','环境舒适度','标准化值','均分')
    get_standard_value('ecology_2021','均分','标准化得分')

    get_weight_value('cultural_resources_2020',['0.45','0.42','0.12','0.01'],['世界遗产','人类非遗','国家非遗','省级非遗'],'综合打分')
    get_weight_value('cultural_resources_2021',['0.45','0.42','0.12','0.01'],['世界遗产','人类非遗','国家非遗','省级非遗'],'综合打分')
    get_standard_value('cultural_resources_2020','综合打分','标准化值')
    get_standard_value('cultural_resources_2021','综合打分','标准化值')