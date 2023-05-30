from flask import Flask,request, jsonify
from flask_cors import CORS
import pymysql
import pandas as pd
import threading

# 创建Flask实例
app = Flask(__name__)
CORS(app, resources=r'/*')	# 注册CORS, "/*" 允许访问所有api

# dbcon = pymysql.connect(
#   host="39.107.97.152",
#   user="root",
#   password="Ccbupt1234!",
#   db = "china_data",
#   port=3306,
#   charset='utf8mb4',
#   connect_timeout=10,
#  )

dbcon = pymysql.connect(
  host="127.0.0.1",
  user="root",
  password="root",
  db = "china_data",
  port=3306,
  charset='utf8mb4',
  connect_timeout=10,
 )

lock = threading.Lock()

# 设置路由，装饰器绑定触发函数
@app.route("/")
def data_provided_allcity():
    res=[]
    sql_2020 = "select * from china_data_2020"
    sql_2021 = "select * from china_data_2021"
    lock.acquire()
    data_2020 = pd.read_sql(sql_2020,dbcon)
    data_2021 = pd.read_sql(sql_2021,dbcon)
    lock.release()
    data_provided_2020=data_2020[['城市','总分']]
    data_provided_2021=data_2021[['城市','总分']]
    data_provided_2020.columns = ['name','value']
    data_provided_2021.columns = ['name','value']
    res.append({"year":2020,"data":data_provided_2020.to_dict(orient='records')})
    res.append({"year":2021,"data":data_provided_2021.to_dict(orient='records')})
    return jsonify(res)

@app.route("/radar")
def data_provided_city():
    city_name = request.args.get("city_name")
    year= request.args.get("year")
    if year not in ['2020','2021']:
        return jsonify([])
    table="china_data_"+year
    city_value=['生态禀赋','文化资源','政策地位','经济规模','交通规模','创新能力','基本保障','生活水平','主流评价','教育服务','医疗服务','文化服务','主流媒体','网络接入','舆情干预','媒体影响','群体情绪','城市标签','就学吸引','就业吸引','旅游吸引','外资吸引','会展竞争']
    res_value=[]
    res_rank=[]
    for v in city_value:
        sql = "select "+v+" from "+table+" where 城市="+city_name
        # Mysql8.x
        # sql_rank="WITH a AS(SELECT 城市,RANK( ) OVER (ORDER BY "+v+" DESC) city_rank FROM "+table+") SELECT city_rank FROM a WHERE 城市="+city_name
        #Mysql5.x
        sql_rank="SELECT aaa.rank from(select `城市`,`"+v+"`, @rk := @rk+1 as rank from "+table+",(select @rk:=0)  a order by `"+v+"` desc ) as aaa where `城市` ="+city_name
        lock.acquire()
        data = pd.read_sql(sql,dbcon)
        data_rank=pd.read_sql(sql_rank,dbcon)
        lock.release()
        res_value.append(format(data.iloc[0, 0],'.2f'))
        res_rank.append(float(data_rank.iloc[0, 0]))
    res=[{'name':city_name[1:-1],'value':res_value,'rank':res_rank}]
    # print(res)
    return jsonify(res)

@app.route("/softpower")
def data_provided_index():
    data_index = request.args.get("data_index")
    # year= request.args.get("year")
    result=[]
    res=[]
    res2=[]
    all_index=['生态禀赋','文化资源','政策地位','经济规模','交通规模','创新能力','基本保障','生活水平','主流评价','教育服务','医疗服务','文化服务','主流媒体','网络接入','舆情干预','媒体影响','群体情绪','城市标签','就学吸引','就业吸引','旅游吸引','外资吸引','会展竞争']
    all_city=['石家庄','太原','呼和浩特','沈阳','长春','哈尔滨','南京','杭州','合肥','福州','南昌','郑州','武汉','长沙','广州','南宁','海口','成都','贵阳','昆明','拉萨','西安','兰州','西宁','银川','乌鲁木齐','深圳','大连','宁波','青岛','厦门','苏州']
    # if year not in ['2020','2021']:
    #     return jsonify(res)
    if data_index not in all_index:
        return jsonify(res)
    table="china_data_2020"
    table2="china_data_2021"
    for city_name in all_city:
        city_name="\""+city_name+"\""
        sql = "select "+data_index+" from "+table+" where 城市="+city_name
        sql2 = "select "+data_index+" from "+table2+" where 城市="+city_name
        #Mysql8.x
        # sql_rank="WITH a AS(SELECT 城市,RANK( ) OVER (ORDER BY "+data_index+" DESC) city_rank FROM "+table+") SELECT city_rank FROM a WHERE 城市="+city_name
        # sql_rank2="WITH a AS(SELECT 城市,RANK( ) OVER (ORDER BY "+data_index+" DESC) city_rank FROM "+table2+") SELECT city_rank FROM a WHERE 城市="+city_name
        #Mysql5.x
        sql_rank="select aaa.rank from(select `城市`,`"+data_index+"`, @rk := @rk+1 as rank from "+table+",(select @rk:=0)  a order by `"+data_index+"` desc ) as aaa where `城市` ="+city_name
        sql_rank2="select aaa.rank from(select `城市`,`"+data_index+"`, @rk := @rk+1 as rank from "+table2+",(select @rk:=0)  a order by `"+data_index+"` desc ) as aaa where `城市` ="+city_name
        lock.acquire()
        data = pd.read_sql(sql,dbcon)
        data2 = pd.read_sql(sql2,dbcon)
        data_rank=pd.read_sql(sql_rank,dbcon)
        data_rank2=pd.read_sql(sql_rank2,dbcon)
        lock.release()
        temp_data={'name':city_name[1:-1],'data':float(data.iloc[0, 0]),'rank':float(data_rank.iloc[0, 0])}
        temp_data2={'name':city_name[1:-1],'data':float(data2.iloc[0, 0]),'rank':float(data_rank2.iloc[0, 0])}
        res.append(temp_data)
        res2.append(temp_data2)

    # print(res)
    result.append({"year": 2020, "data": res})
    result.append({"year": 2021, "data": res2})
    return jsonify(result)

@app.route("/scatter")
def data_provided_scatter():
    res=[]
    year= request.args.get("year")
    if year not in ['2020','2021']:
        return res
    sql= "select * from city_scatter_"+year
    lock.acquire()
    data = pd.read_sql(sql,dbcon)
    lock.release()
    for a,b,c,d in  zip(data['支撑性得分'], data['效应性得分'],data['城市'],data['常住人口']):
        res.append([a,b,c,d])
    return jsonify(res)

@app.route("/wordcloud")
def data_provided_wordcloud():
    res=[]
    year= request.args.get("year")
    if year not in ['2020','2021']:
        return res
    if year=='2020':
        res=[{'name':"舆情干预",'value':100},{'name':"创新能力",'value':100},{'name':"政策地位",'value':80},{'name':"主流评价",'value':80},{'name':"城市标签",'value':80},{'name':"媒体影响",'value':60},{'name':"生活水平",'value':60},{'name':"医疗服务",'value':60},{'name':"就业吸引",'value':40},{'name':"就学吸引",'value':20},{'name':"交通规模",'value':20}]
    if year=='2021':
        res=[{'name':"医疗服务",'value':100},{'name':"舆情干预",'value':100},{'name':"创新能力",'value':100},{'name':"政策地位",'value':80},{'name':"主流评价",'value':80},{'name':"城市标签",'value':80},{'name':"媒体影响",'value':60},{'name':"就学吸引",'value':40},{'name':"生活水平",'value':40},{'name':"交通规模",'value':20}]
    return jsonify(res)

# @app.route("/wordcloud")
# def data_provided_wordcloud():
#     res=[]
#     word_num=23
#     weight=[33,32,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1]
#     # weight=[1 for _ in range(33)]
#     all_index=['生态禀赋','文化资源','政策地位','经济规模','交通规模','创新能力','基本保障','生活水平','主流评价','教育服务','医疗服务','文化服务','主流媒体','网络接入','舆情干预','媒体影响','群体情绪','城市标签','就学吸引','就业吸引','旅游吸引','外资吸引','会展竞争']
#     all_value={'生态禀赋':0,'文化资源':0,'政策地位':0,'经济规模':0,'交通规模':0,'创新能力':0,'基本保障':0,'生活水平':0,'主流评价':0,'教育服务':0,'医疗服务':0,'文化服务':0,'主流媒体':0,'网络接入':0,'舆情干预':0,'媒体影响':0,'群体情绪':0,'城市标签':0,'就学吸引':0,'就业吸引':0,'旅游吸引':0,'外资吸引':0,'会展竞争':0}
#     year= request.args.get("year")
#     if year not in ['2020','2021']:
#         return jsonify(res)
#     table="china_data_"+year
#     sql= "select * from "+table
#     lock.acquire()
#     df= pd.read_sql(sql,dbcon)
#     lock.release()
#     df_data_order = df.sort_values(by=['总分'],ascending=[False])
#     df_data_order.reset_index(drop=True, inplace=True)
#     for ind,row in df_data_order.iterrows():
#         for index in all_index:
#             all_value[index]=all_value[index]+row[index]*weight[ind]
#     all_value_order=dict(sorted(all_value.items(),key=lambda x:x[1],reverse=True))
#     for k,v in all_value_order.items():
#         res.append({'name':k,'value':round(v, 0)})
#     return jsonify(res[:word_num])

if __name__ == "__main__":
    # 代码修改能运行时生效，app.run运行服务
    # debug=True 
    # host默认127.0.0.1 端口默认5000
    app.config['JSON_AS_ASCII'] = False
    app.config['JSONIFY_MIMETYPE'] = "application/json;charset=utf-8"
    app.run(host="0.0.0.0")