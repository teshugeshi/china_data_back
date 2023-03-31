from flask import Flask,request, jsonify
from flask_cors import CORS
import pymysql
import pandas as pd

# 创建Flask实例
app = Flask(__name__)
CORS(app, resources=r'/*')	# 注册CORS, "/*" 允许访问所有api

dbcon = pymysql.connect(
  host="39.107.97.152",
  user="root",
  password="Ccbupt1234!",
  db = "china_data",
  port=3306,
  charset='utf8mb4'
 )

# 设置路由，装饰器绑定触发函数
@app.route("/")
def data_provided_allcity():
    sql = "select * from china_data"
    data = pd.read_sql(sql,dbcon)
    data_provided=data[['城市','总分']]
    data_provided.columns = ['name','value']
    return data_provided.to_json(orient='records',force_ascii=False)

@app.route("/data_api")
def data_provided_city():
    city_name = request.args.get("city_name")
    city_value=['生态禀赋','文化资源','政策地位','经济规模','交通规模','创新能力','基本保障','生活水平','主流评价','教育服务','医疗服务','文化服务','主流媒体','网络接入','舆情干预','媒体影响','群体情绪','城市标签','就学吸引','就业吸引','旅游吸引','外资吸引','会展竞争']
    res_value=[]
    res_rank=[]
    for v in city_value:
        sql = "select "+v+" from china_data where 城市="+city_name
        sql_rank="SELECT aaa.rank from(select `城市`,`"+v+"`, @rk := @rk+1 as rank from china_data,(select @rk:=0)  a order by `"+v+"` desc ) as aaa where `城市` ="+city_name
        data = pd.read_sql(sql,dbcon)
        data_rank=pd.read_sql(sql_rank,dbcon)
        res_value.append(data.iloc[0, 0])
        res_rank.append(data_rank.iloc[0, 0])
    res={'value':res_value,'rank':res_rank}
    return res

if __name__ == "__main__":
    # debug=True 代码修改能运行时生效，app.run运行服务
    # host默认127.0.0.1 端口默认5000
    app.run(host="0.0.0.0",debug=True)