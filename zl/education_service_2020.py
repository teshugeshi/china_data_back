import mysql.connector

# 连接到MySQL数据库
cnx = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="china_data"
)

# 创建游标对象
cursor = cnx.cursor()

# 查询数据库中的数据
query = "SELECT 标准化值, 标准化值1 FROM education_service_2020"
cursor.execute(query)

# 获取查询结果
rows = cursor.fetchall()

# 计算字段A和字段B的均值
avg_values = []
for row in rows:
    value_a = row[0]
    value_b = row[1]
    if value_a is not None and value_b is not None:
        avg = (value_a + value_b) / 2
        avg_values.append(avg)

# 更新每行数据中的新字段
update_query = "UPDATE education_service_2020 SET result = %s WHERE 标准化值 = %s AND 标准化值1 = %s"

for i, row in enumerate(rows):
    value_a = row[0]
    value_b = row[1]
    if value_a is not None and value_b is not None:
        values = (avg_values[i], value_a, value_b)
        cursor.execute(update_query, values)

# 提交更改到数据库
cnx.commit()

# 关闭游标和数据库连接
cursor.close()
cnx.close()
