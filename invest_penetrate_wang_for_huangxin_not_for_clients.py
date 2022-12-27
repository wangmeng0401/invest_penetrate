# coding:utf8
import importlib
import sys
import pymysql
import xlsxwriter

importlib.reload(sys)

# f1 = open("cname", "a")
# 业务主库 只读
MYSQL_MAIN_HOST = '0bb0147d82fe49e6953138da0eea61dbin01.internal.cn-north-4.mysql.rds.myhuaweicloud.com'
MYSQL_MAIN_PORT = 3306
MYSQL_MAIN_USER = 'jdhw_other_r'
MYSQL_MAIN_PASSWORD = 'ckHS0dedmWPjpryST3gD'
MYSQL_MAIN_DB = 'prism'

# 受益股东库 只读
MYSQL_SHAREHOLDER_HOST = '45be16af451d41a5aa2d16c15ee00070in01.internal.cn-north-4.mysql.rds.myhuaweicloud.com'
MYSQL_SHAREHOLDER_PORT = 3306
MYSQL_SHAREHOLDER_USER = 'jdhw_other_r'
MYSQL_SHAREHOLDER_PASSWORD = 'ckHS0dedmWPjpryST3gD'
MYSQL_SHAREHOLDER_DB = 'prism_shareholder_path'

# 公司属性库 只读
MYSQL_COMPANY_HOST = '617c4b82baf9444d8dc7401b30cbe26bin01.internal.cn-north-4.mysql.rds.myhuaweicloud.com'
MYSQL_COMPANY_PORT = 3306
MYSQL_COMPANY_USER = 'jdhw_other_r'
MYSQL_COMPANY_PASSWORD = 'ckHS0dedmWPjpryST3gD'
MYSQL_COMPANY_DB = 'prism'

# 历史名城库 只读
MYSQL_HISTORY_HOST = '23869950dcaa41ada058a8a05b126046in01.internal.cn-north-4.mysql.rds.myhuaweicloud.com'
MYSQL_HISTORY_PORT = 3306
MYSQL_HISTORY_USER = 'jdhw_other_r'
MYSQL_HISTORY_PASSWORD = 'ckHS0dedmWPjpryST3gD'
MYSQL_HISTORY_DB = 'prism_timeline'

main_conn = pymysql.connect(host=MYSQL_MAIN_HOST, user=MYSQL_MAIN_USER, password=MYSQL_MAIN_PASSWORD,
                            database=MYSQL_MAIN_DB, charset="utf8", autocommit=True)
main_cursor = main_conn.cursor(cursor=pymysql.cursors.DictCursor)

shareholder_conn = pymysql.connect(host=MYSQL_SHAREHOLDER_HOST, user=MYSQL_SHAREHOLDER_USER,
                                   password=MYSQL_SHAREHOLDER_PASSWORD, database=MYSQL_SHAREHOLDER_DB, charset="utf8",
                                   autocommit=True)
shareholder_cursor = shareholder_conn.cursor(cursor=pymysql.cursors.DictCursor)

company_conn = pymysql.connect(host=MYSQL_COMPANY_HOST, user=MYSQL_COMPANY_USER, password=MYSQL_COMPANY_PASSWORD,
                               database=MYSQL_COMPANY_DB, charset="utf8", autocommit=True)
company_cursor = company_conn.cursor(cursor=pymysql.cursors.DictCursor)

history_conn = pymysql.connect(host=MYSQL_HISTORY_HOST, user=MYSQL_HISTORY_USER, password=MYSQL_HISTORY_PASSWORD,
                               database=MYSQL_HISTORY_DB, charset="utf8", autocommit=True)
history_cursor = history_conn.cursor(cursor=pymysql.cursors.DictCursor)

MAX_LEVEL = 0
special_company_name_set = (u"")
stop_company_name_set = (u"")
biggest_shareholder_name_set = ["中国交通建设集团有限公司"]


# 根据gid获取最近一次股东退出时间（By wang）
def get_latest_investor_time_by_cgid(cgid):
    sql = """
        select 
            end_time, source_name
        from 
            edges_all 
        where 
            target_id = '%s' 
            and relation in (2,3) 
            and end_time <> '9999-09-09 00:00:00'
        order by 
            end_time desc
    """ % cgid
    history_cursor.execute(sql)
    data = history_cursor.fetchall()
    if data:
        # print(data)
        if len(data) == 1:
            for item in range(len(data)):
                return str(data[item]["end_time"]), str(data[item]["source_name"]), "", "", "", ""
        if len(data) == 2:
            for item in range(len(data)):
                return str(data[item]["end_time"]), str(data[item]["source_name"]), str(data[item]["end_time"]), \
                       str(data[item]["source_name"]), "", ""
        else:
            for item in range(3):
                return str(data[0]["end_time"]), str(data[0]["source_name"]), str(data[1]["end_time"]), \
                       str(data[1]["source_name"]), str(data[2]["end_time"]), str(data[2]["source_name"])
    return '', '', '', '', '', ''


# 根据gid获取对外投资数量
def get_invest_count_by_cgid(cgid):
    sql = """
        select
            count(shareholder_graph_id) as invest_count
        from
            equity_ratio
        where
            shareholder_graph_id = '%s'
            and deleted = 0
    """ % cgid
    main_cursor.execute(sql)
    data = main_cursor.fetchall()
    if data:
        return data[0]["invest_count"]
    return ''


# 根据gid获取历史对外投资数量
def get_history_invest_count_by_cgid(cgid):
    sql = """
        select
            count(id) as history_invest_count 
        from
            edges_all
        where
            source_id = '%s'
            and source_type = 1
            and relation = 3
            and end_time <> '9999-09-09 00:00:00'
            and deleted = 0
    """ % cgid
    history_cursor.execute(sql)
    data = history_cursor.fetchall()
    if data:
        return data[0]["history_invest_count"]
    return ''


# 根据gid获取实际控制权数（取的数据可能不对，逻辑不清楚，取得是type是公司还是自然人？）
def get_control_count_by_cgid(cgid):
    cgid_convert = str(int(cgid) % 10000 % 64).zfill(3)
    cgid = int(cgid)
    sql = """
        select count(1) control_count
        from
            (SELECT
                company_graph_id
            FROM
                ratio_path_company_%s
            WHERE
                deleted = 0
                AND shareholder_type = 1
                AND percent >= 0.05
                AND shareholder_id = '%s'
                limit 1000) ta
        """ % (cgid_convert, cgid)
    shareholder_cursor.execute(sql)
    data = shareholder_cursor.fetchall()
    if data:
        return data[0]["control_count"]
    return ''


# 根据cgid或者历史名称数据（问题没解决，没有取到数据）
def get_history_name_by_cgid(cgid):
    cgid_convert = int(cgid) % 16
    if cgid_convert < 10:
        cgid_convert
    elif cgid_convert == 10:
        cgid_convert = 'a'
    elif cgid_convert == 11:
        cgid_convert = 'b'
    elif cgid_convert == 12:
        cgid_convert = 'c'
    elif cgid_convert == 13:
        cgid_convert = 'd'
    elif cgid_convert == 14:
        cgid_convert = 'e'
    else:
        cgid_convert = 'f'
    cgid = int(cgid)
    sql = """
        SELECT
            change_time, change_content
        FROM
            company_change_insight_%s
        WHERE
            deleted = 0
            AND change_item_group = "历史曾用名"
            AND company_gid = '%s'
        order by change_time desc
        """ % (cgid_convert, cgid)
    history_cursor.execute(sql)
    data = history_cursor.fetchall()
    if data:
        return data[0]["change_time"], data[0]["change_content"]
    return '', ''


# 根据cgid获取历史注册资本数据
def get_history_reg_capital_by_cgid(cgid):
    cgid_convert = int(cgid) % 16
    if cgid_convert < 10:
        cgid_convert
    elif cgid_convert == 10:
        cgid_convert = 'a'
    elif cgid_convert == 11:
        cgid_convert = 'b'
    elif cgid_convert == 12:
        cgid_convert = 'c'
    elif cgid_convert == 13:
        cgid_convert = 'd'
    elif cgid_convert == 14:
        cgid_convert = 'e'
    else:
        cgid_convert = 'f'
    cgid = int(cgid)
    sql = """
        SELECT
            change_time, change_content
        FROM
            company_change_insight_%s
        WHERE
            deleted = 0
            AND change_item_group = "历史注册资本"
            AND company_gid = '%s'
        order by change_time desc
        """ % (cgid_convert, cgid)
    history_cursor.execute(sql)
    data = history_cursor.fetchall()
    if data:
        return data[0]["change_time"], data[0]["change_content"]
    return '', ''


# 根据企业信息获取投资数据
def get_invest_data(shareholder_data):
    global MAX_LEVEL
    resultList = []
    content = shareholder_data.split("#@#")
    data_index = content[0]
    shareholder_graph_id = content[1]
    # shareholder_name = content[2]
    # shareholder_type = content[3]
    # shareholder_percent = content[4]
    sql = """
        SELECT
            company_graph_id,
            company_name,
            percent 
        FROM
            equity_ratio e
            left join company c 
            on c.name = e.company_name and c.reg_status not like '%注销%' and name not like '%合伙%'
        WHERE
            shareholder_graph_id = {}
            AND deleted = 0 
        ORDER BY
            percent DESC 
    """.format(shareholder_graph_id)  # 为什么这里用left join
    main_cursor.execute(sql)
    data = main_cursor.fetchall()
    if data:
        count = 1
        for item in data:
            if data_index:
                data_index_new = data_index + "-" + str(count)
            else:
                data_index_new = str(count)
            if len(data_index_new.split("-")) > MAX_LEVEL:
                MAX_LEVEL = len(data_index_new.split("-"))
            company_graph_id_new = str(item["company_graph_id"])
            company_name_new = item["company_name"]
            company_type_new = "2"
            percent = str(item["percent"] * 100) + "%"
            count += 1
            tmpStr = "#@#".join([data_index_new, company_graph_id_new, company_name_new, company_type_new, percent])
            #        print tmpStr
            resultList.append(tmpStr)
    return resultList


# 获取企业的上市信息
def get_stock_info(cid):
    sql = 'select * from company_bond_plates where company_id = %s and listing_status not in ("暂停上市", "IPO终止", ' \
          '"退市整理", "终止上市") and type in ("A股", "科创板", "港股") and deleted = 0' % cid
    main_cursor.execute(sql)
    data = main_cursor.fetchall()
    return data


# 使用company_graph表，根据gid获取company_id
def get_cid_by_cgid(cgid):
    sql = "select company_id from company_graph where graph_id = %s and deleted = 0" % cgid
    main_cursor.execute(sql)
    data = main_cursor.fetchone()
    if data:
        return data["company_id"]
    return data


# 根据gid获取企业基本信息
def get_estiblish_time_and_company_org_type_and_score_by_cid(cgid):
    sql = """
        select
            estiblish_time,
            cancel_date,
            legal_person_name,
            company.reg_capital,
            base,
            reg_location,
            percentile_Score,
            reg_status
        from
            company
        join company_graph 
            on company.id = company_graph.company_id
        join company_score 
            on company.id = company_score.company_id
        left join company_other_info 
            on company.id = company_other_info.id
        where 
            deleted = 0
            and graph_id = %s
    """ % cgid
    main_cursor.execute(sql)
    data = main_cursor.fetchone()
    if data:
        return str(data["estiblish_time"]), str(data["cancel_date"]), data["legal_person_name"], data["reg_capital"], \
               data["reg_status"], data["base"], data["reg_location"], data["percentile_Score"] / 100.0
    return '', '', '', '', '', '', '', ''


# 根据gid获取企业行业信息
def get_category_by_cid(cgid):
    sql = "select cate_1 from company_category_all_code_v2017, company_category_v2017" \
          "where company_category_v2017.category_code = company_category_code_20170411.category_code " \
          "and company_category_all_code_v2017.company_id = graph_id = %s and deleted = 0" % cgid
    main_cursor.execute(sql)
    data = main_cursor.fetchone()
    if data:
        return data["cate_1"]
    return ''


# 国资委股权占比
def get_gzw_percent(cgid):
    sql = "select percent from ratio_path_company_042 where shareholder_graph_id = 25942218 and shareholder_type = 2 " \
          "and company_graph_id = %s and deleted = 0" % cgid
    shareholder_cursor.execute(sql)
    data = shareholder_cursor.fetchone()
    if data:
        return str(data["percent"] * 100) + "%"
    return ''


# 根据gid获取最大的股东名称
def get_biggest_shareholder_by_gid(gid):
    sql = """
        select
            shareholder_name
        from
            equity_ratio
        where
            company_graph_id = {}
            and deleted =0
        ORDER BY
            percent desc
        limit 1
    """.format(gid)  # 如果有两个投资比例相同的股东，如何确保我们选的这个大股东是我们想要的这家？
    main_cursor.execute(sql)
    data = main_cursor.fetchone()
    if data:
        return data["shareholder_name"]
    return data


# 输入的数据处理过程
def process(dataList):
    cgid_set = set()
    listNum = len(dataList)
    count = 0
    while count < listNum:
        content = dataList[count].split("#@#")
        shareholder_graph_id = content[1]
        shareholder_type = content[3]
        shareholder_name = content[2]
        if shareholder_type == "2":
            if shareholder_graph_id not in cgid_set:
                cgid_set.add(shareholder_graph_id)
            else:
                count += 1
                continue
            # f1.write(shareholder_name+"\n")
            # f1.flush()
            biggest_shareholder_name = get_biggest_shareholder_by_gid(shareholder_graph_id)
            # print(biggest_shareholder_name_set)
            # print(
            # "=======sss" + shareholder_name + "ssss=====" + shareholder_graph_id + "sss=====" + biggest_shareholder_name)
            if shareholder_name not in "中国交通建设集团有限公司" and biggest_shareholder_name not in biggest_shareholder_name_set:
                count += 1
                continue
            # if u"银行" in shareholder_name or u"证券" in shareholder_name or u"信托" in shareholder_name:
            #     count += 1
            #     continue
            if shareholder_name in stop_company_name_set:
                count += 1
                continue
            shareholder_id = get_cid_by_cgid(shareholder_graph_id)  # 使用了函数
            # if shareholder_id and shareholder_name not in special_company_name_set:
            #     if get_stock_info(shareholder_id):  # 使用了函数
            #         count += 1
            #         continue
            tmpList = get_invest_data(dataList[count])
            tmpBefore = dataList[:count + 1]
            tmpAfter = dataList[count + 1:]
            dataList = []
            dataList.extend(tmpBefore)
            dataList.extend(tmpList)
            dataList.extend(tmpAfter)
            listNum = len(dataList)
            biggest_shareholder_name_set.append(shareholder_name)
        count += 1

    print("##### 穿透完毕")
    print("###穿透总数：%s" % len(dataList))
    return dataList


dataList = process(["#@#4337683#@#中国交通建设集团有限公司#@#2"])
# dataList = []
titleList = list()
invest_List = list()
xls = xlsxwriter.Workbook("中国交通建设集团有限公司.xlsx")
sheet = xls.add_worksheet('sheet1')
# f1.close()
if len(dataList) > 1:
    print(MAX_LEVEL)
    for item in range(MAX_LEVEL):
        title = u"%s级" % (item + 1)
        titleList.append(title)
    titleList.append(u"历史注册资本时间")
    titleList.append(u"历史注册资本")
    titleList.append(u"历史名称时间")
    titleList.append(u"历史名称")
    titleList.append(u"持股比例")
    titleList.append(u"实际控制权数量")
    titleList.append(u"历史对外投资数量")
    titleList.append(u"对外投资数量")
    titleList.append(u"成立时间")
    titleList.append(u"注销时间")
    titleList.append(u"法定代表人")
    titleList.append(u"注册资本")
    titleList.append(u"企业状态")
    titleList.append(u"注册城市")
    titleList.append(u"注册地址")
    titleList.append(u"天眼评分")
    titleList.append(u"国资委占比")
    titleList.append(u"最近一次股东退出时间")
    titleList.append(u"最近一次股东名称")
    titleList.append(u"最近一次股东退出2时间")
    titleList.append(u"最近一次股东2名称")
    titleList.append(u"最近一次股东退出3时间")
    titleList.append(u"最近一次股东3名称")
    # print(titleList)
    # print(dataList)

    for item in dataList[1:]:
        tmpList = []
        shareholder_index = item.split("#@#")[0]
        shareholder_graph_id = item.split("#@#")[1]
        shareholder_name = item.split("#@#")[2]
        shareholder_percent = item.split("#@#")[4]
        index = 0
        while index < len(titleList):
            tmp = ""
            if index == len(shareholder_index.split("-")) - 1:
                tmp = shareholder_name
            tmpList.append(tmp)
            index += 1
        # print(shareholder_index, shareholder_name, shareholder_percent)
        tmpList[-23], tmpList[-22] = get_history_reg_capital_by_cgid(shareholder_graph_id)
        tmpList[-21], tmpList[-20] = get_history_name_by_cgid(shareholder_graph_id)
        tmpList[-19] = shareholder_percent
        tmpList[-18] = get_control_count_by_cgid(shareholder_graph_id)
        tmpList[-17] = get_history_invest_count_by_cgid(shareholder_graph_id)
        tmpList[-16] = get_invest_count_by_cgid(shareholder_graph_id)
        tmpList[-15], tmpList[-14], tmpList[-13], tmpList[-12], tmpList[-11], tmpList[-10], tmpList[-9], tmpList[
            -8] = get_estiblish_time_and_company_org_type_and_score_by_cid(shareholder_graph_id)
        tmpList[-7] = get_gzw_percent(shareholder_graph_id)
        tmpList[-6], tmpList[-5], tmpList[-4], tmpList[-3], tmpList[-2], tmpList[-1] = get_latest_investor_time_by_cgid(
            shareholder_graph_id)
        invest_List.append(tmpList)

    ls = 0
    for title in titleList:
        sheet.write(0, ls, title)
        ls += 1

    i = 1
    for item in invest_List:
        j = 0
        for data in item:
            sheet.write(i, j, data)
            j += 1
        i += 1
    xls.close()
