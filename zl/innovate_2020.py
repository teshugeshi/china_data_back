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
query = "SELECT 创新指数 FROM innovate_2020"
cursor.execute(query)

# 获取查询结果
rows = cursor.fetchall()


# 获取GDP总量的最小值和最大值
gdp_values = [row[0] for row in rows]
print(gdp_values)
gdp_min = min(gdp_values)
gdp_max = max(gdp_values)

print("最大GDP总量:", gdp_max)
print("最小GDP总量:", gdp_min)

# 更新每行数据中的新字段
update_query = "UPDATE innovate_2020 SET result = (%s) WHERE 创新指数 = (%s)"
for row in rows:
    gdp = row[0]
    normalized_gdp = (gdp - gdp_min) / (gdp_max - gdp_min)
    values = (normalized_gdp, gdp)
    cursor.execute(update_query, values)

# 提交更改到数据库
cnx.commit()

# 关闭游标和数据库连接
cursor.close()
cnx.close()