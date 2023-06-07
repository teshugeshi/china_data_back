import mysql.connector

cnx = mysql.connector.connect(
        host="39.107.97.152",
        user="root",
        password="Ccbupt1234!",
        database="china_data"
    )

def get_standard_value(table,field,result):
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
        print(update_query)
        cursor.execute(update_query, values)
    # 提交更改到数据库
    cnx.commit()
    # 关闭游标
    cursor.close()

def get_2_average_value(table,field1,field2,result):
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
        print(update_query)
        cursor.execute(update_query, values)
    # # 提交更改到数据库
    cnx.commit()
    # 关闭游标
    cursor.close()

def get_3_average_value(table,field1,field2,field3,result):
    # 创建游标对象
    cursor = cnx.cursor()
    # 查询数据库中的数据
    query = "SELECT "+field1+","+field2+","+field3+","+field1+"+"+field2+"+"+field3+" FROM "+table
    cursor.execute(query)
    # 获取查询结果
    rows = cursor.fetchall()
    # 更新每行数据中的新字段
    update_query = "UPDATE "+table+" SET "+result+" = (%s) WHERE "+field1+" = (%s)"+" AND "+field2+" = (%s)"+" AND "+field3+" = (%s)"
    for row in rows:
        values = (row[3]/3, row[0],row[1],row[2])
        print(update_query)
        cursor.execute(update_query, values)
    # # 提交更改到数据库
    cnx.commit()
    # 关闭游标
    cursor.close()

def get_division_value(table,field1,field2,magnification,result):
    # 创建游标对象
    cursor = cnx.cursor()
    # 查询数据库中的数据
    query = "SELECT "+field1+","+field2+","+field1+"*"+str(magnification)+"/"+field2+" FROM "+table
    cursor.execute(query)
    # 获取查询结果
    rows = cursor.fetchall()
    # 更新每行数据中的新字段
    update_query = "UPDATE "+table+" SET "+result+" = (%s) WHERE "+field1+" = (%s)"+" AND "+field2+" = (%s)"
    for row in rows:
        values = (row[2], row[0],row[1])
        print(update_query)
        cursor.execute(update_query, values)
    # # 提交更改到数据库
    cnx.commit()
    # 关闭游标
    cursor.close()

def get_weight_value(table,weights,fields,result):
    # 创建游标对象
    cursor = cnx.cursor()
    num=len(weights)
    update_query = "UPDATE "+table+" SET "+result+"="
    for n in range(num):
        update_query=update_query+weights[n]+'*'+fields[n]+'+'
    update_query=update_query[:-1]
    print(update_query)
    cursor.execute(update_query)
    cnx.commit()
    # 关闭游标
    cursor.close()

def get_allcity_data(years):
    field_num=23
    all_city=['石家庄','太原','呼和浩特','沈阳','长春','哈尔滨','南京','杭州','合肥','福州','南昌','郑州','武汉','长沙','广州','南宁','海口','成都','贵阳','昆明','拉萨','西安','兰州','西宁','银川','乌鲁木齐','深圳','大连','宁波','青岛','厦门','苏州']
    city_value=['生态禀赋','文化资源','政策地位','经济规模','交通规模','创新能力','基本保障','生活水平','主流评价','教育服务','医疗服务','文化服务','主流媒体','网络接入','舆情干预','媒体影响','群体情绪','城市标签','就学吸引','就业吸引','旅游吸引','外资吸引','会展竞争']
    table_name_prefix=['ecology_','cultural_resources_','policy_','economy_','traffic_','innovate_','basic_security_','living_standard_','main_evaluate_','education_service_','medical_','culture_','main_media_','network_access_','public_sentiment_','media_influ_','group_emotions_','city_label_','education_attraction_','employment_attraction_','tourist_attraction_','foreign_investment_attraction_','exhibition_competition_']
    score_field=['标准化得分','标准化值','评分','标准化值','标准化值','标准化值','标准化值','得分','标准化值','平均值','标准化值','平均','平均值','标准化值','标准化值','标准化值4','标准化值','标准化值','标准化值','均分','标准化值','均分','标准化值']
    # 创建游标对象
    cursor = cnx.cursor()
    for year in years:
        table='china_data_'+year
        for num in range(field_num):
            for city_name in all_city:
                update_query = "UPDATE "+table+" SET "+city_value[num]+"=(SELECT "+score_field[num]+" FROM "+table_name_prefix[num]+year+" WHERE 城市=\""+city_name+"\") "+"WHERE 城市=\""+city_name+"\""
                print(update_query)
                cursor.execute(update_query)
    cnx.commit()
    # 关闭游标
    cursor.close()

if __name__ == '__main__':
    # get_division_value('ecology_2020','水资源','常住人口',1,'人均')
    get_standard_value('ecology_2020','人均','标准化值')
    get_2_average_value('ecology_2020','环境舒适度','标准化值','均分')
    get_standard_value('ecology_2020','均分','标准化得分')
    # get_division_value('ecology_2021','水资源','常住人口',1,'人均')
    get_standard_value('ecology_2021','人均','标准化值')
    get_2_average_value('ecology_2021','环境舒适度','标准化值','均分')
    get_standard_value('ecology_2021','均分','标准化得分')

    get_weight_value('cultural_resources_2020',['0.45','0.42','0.12','0.01'],['世界遗产','人类非遗','国家非遗','省级非遗'],'综合打分')
    get_weight_value('cultural_resources_2021',['0.45','0.42','0.12','0.01'],['世界遗产','人类非遗','国家非遗','省级非遗'],'综合打分')
    get_standard_value('cultural_resources_2020','综合打分','标准化值')
    get_standard_value('cultural_resources_2021','综合打分','标准化值')

    get_standard_value('economy_2020','GDP总量','标准化值')
    get_standard_value('economy_2021','GDP总量','标准化值')

    get_standard_value('traffic_2020','客运总量','标准化值')
    get_standard_value('traffic_2021','客运总量','标准化值')

    get_standard_value('innovate_2020','创新指数','标准化值')
    get_standard_value('innovate_2021','创新指数','标准化值')

    get_standard_value('basic_security_2020','平均值','标准化值')
    get_standard_value('basic_security_2021','平均值','标准化值')

    get_2_average_value('living_standard_2020','标准化值','标准化值1','得分')
    get_2_average_value('living_standard_2021','标准化值','标准化值1','得分')

    get_standard_value('main_evaluate_2020','文明城市得分','标准化值')
    get_standard_value('main_evaluate_2021','文明城市得分','标准化值')

    get_2_average_value('education_service_2020','标准化值','标准化值1','平均值')
    get_2_average_value('education_service_2021','标准化值','标准化值1','平均值')

    get_standard_value('medical_2020','医疗硬件环境（排名）','标准化值')
    get_standard_value('medical_2021','医疗硬件环境（排名）','标准化值')

    get_division_value('culture_2020','博物馆数量','常住人口',1000000,'人均1')
    get_standard_value('culture_2020','人均1','标准化值1')
    get_division_value('culture_2020','图书馆藏书','常住人口',10000,'人均2')
    get_standard_value('culture_2020','人均2','标准化值2')
    get_division_value('culture_2020','公园绿地面积','常住人口',10000,'人均3')
    get_standard_value('culture_2020','人均3','标准化值3')
    get_3_average_value('culture_2020','标准化值1','标准化值2','标准化值3','平均')
    get_division_value('culture_2021','博物馆数量','常住人口',1000000,'人均1')
    get_standard_value('culture_2021','人均1','标准化值1')
    get_division_value('culture_2021','图书馆藏书','常住人口',10000,'人均2')
    get_standard_value('culture_2021','人均2','标准化值2')
    get_division_value('culture_2021','公园绿地面积','常住人口',10000,'人均3')
    get_standard_value('culture_2021','人均3','标准化值3')
    get_3_average_value('culture_2021','标准化值1','标准化值2','标准化值3','平均')

    get_standard_value('main_media_2020','融媒体阅读量','标准化值1')
    get_standard_value('main_media_2020','市级频道占有率','标准化值2')
    get_2_average_value('main_media_2020','标准化值1','标准化值2','平均值')
    get_standard_value('main_media_2021','融媒体阅读量','标准化值1')
    get_standard_value('main_media_2021','市级频道占有率','标准化值2')
    get_2_average_value('main_media_2021','标准化值1','标准化值2','平均值')

    get_division_value('network_access_2020','网络接入','常住人口',10000,'人均')
    get_standard_value('network_access_2020','人均','标准化值')
    get_division_value('network_access_2021','网络接入','常住人口',10000,'人均')
    get_standard_value('network_access_2021','人均','标准化值')

    get_standard_value('public_sentiment_2020','清朗指数','标准化值')
    get_standard_value('public_sentiment_2021','清朗指数','标准化值')

    get_standard_value('media_influ_2020','信息总量','标准化值1')
    get_standard_value('media_influ_2020','正面占比','标准化值2')
    get_standard_value('media_influ_2020','负面占比','标准化值3')
    get_3_average_value('media_influ_2020','标准化值1','标准化值2','标准化值3','标准化值4')
    get_standard_value('media_influ_2021','信息总量','标准化值1')
    get_standard_value('media_influ_2021','正面占比','标准化值2')
    get_standard_value('media_influ_2021','负面占比','标准化值3')
    get_3_average_value('media_influ_2021','标准化值1','标准化值2','标准化值3','标准化值4')

    get_standard_value('group_emotions_2020','正面情绪占比','标准化值')
    get_standard_value('group_emotions_2021','正面情绪占比','标准化值')

    get_standard_value('city_label_2020','城市标签打分','标准化值')
    get_standard_value('city_label_2021','城市标签打分','标准化值')

    get_weight_value('employment_attraction_2020',['10000','1'],['私营从业','单位从业'],'就业人口')
    get_division_value('employment_attraction_2020','就业人口','适龄人口',1,'就业率')
    get_standard_value('employment_attraction_2020','就业率','标准化值1')
    get_standard_value('employment_attraction_2020','平均工资','标准化值2')
    get_standard_value('employment_attraction_2020','房价','标准化值3')
    get_3_average_value('employment_attraction_2020','标准化值1','标准化值2','标准化值3','均分')
    get_weight_value('employment_attraction_2021',['10000','1'],['私营从业','单位从业'],'就业人口')
    get_division_value('employment_attraction_2021','就业人口','适龄人口',1,'就业率')
    get_standard_value('employment_attraction_2021','就业率','标准化值1')
    get_standard_value('employment_attraction_2021','平均工资','标准化值2')
    get_standard_value('employment_attraction_2021','房价','标准化值3')
    get_3_average_value('employment_attraction_2021','标准化值1','标准化值2','标准化值3','均分')

    get_weight_value('education_attraction_2020',['0.5','0.35','0.1','0.05'],['985数量','211数量','普通高等学校','中职数量'],'综合分')
    get_standard_value('education_attraction_2020','综合分','标准化值')
    get_weight_value('education_attraction_2021',['0.5','0.35','0.1','0.05'],['985数量','211数量','普通高等学校','中职数量'],'综合分')
    get_standard_value('education_attraction_2021','综合分','标准化值')

    get_standard_value('tourist_attraction_2020','年游客数量','标准化值')
    get_standard_value('tourist_attraction_2021','年游客数量','标准化值')

    get_standard_value('foreign_investment_attraction_2020','实际使用外资','标准化值1')
    get_standard_value('foreign_investment_attraction_2020','外商直接投资项目','标准化值2')
    get_2_average_value('foreign_investment_attraction_2020','标准化值1','标准化值2','均分')
    get_standard_value('foreign_investment_attraction_2021','实际使用外资','标准化值1')
    get_standard_value('foreign_investment_attraction_2021','外商直接投资项目','标准化值2')
    get_2_average_value('foreign_investment_attraction_2021','标准化值1','标准化值2','均分')


    get_standard_value('exhibition_competition_2020','会展竞争力','标准化值')
    get_standard_value('exhibition_competition_2021','会展竞争力','标准化值')

    get_allcity_data(['2020','2021'])
    
    print('数据库更新完成')
    cnx.close()