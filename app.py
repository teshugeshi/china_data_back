from flask import Flask,request, jsonify
import pymysql
import pandas as pd
from flask_sqlalchemy import SQLAlchemy

# 创建Flask实例
app = Flask(__name__)

dbcon = pymysql.connect(
  host="localhost",
  user="root",
  password="123456",
  db = "city_data",
  port=3306,
  charset='utf8mb4'
 )

# 设置路由，装饰器绑定触发函数
@app.route("/")
def hello_world():
    sql = "select * from city_data"
    data = pd.read_sql(sql,dbcon).to_json(force_ascii=False)
    return data

if __name__ == "__main__":
    # debug=True 代码修改能运行时生效，app.run运行服务
    # host默认127.0.0.1 端口默认5000
    app.run(debug=True)