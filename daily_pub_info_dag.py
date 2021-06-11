import xml.etree.ElementTree as ET
import time
from datetime import datetime, timedelta
import pandas as pd
import pymysql
import requests
import base64
import json
from multiprocessing import Process, Queue
import xmltodict

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


def make_token():
    key_list = [b'o2TgZqLMPnGxmiFk7rUUB0bTq9VZDbe1:ZHYxcGxi9UprBTUD',
                b'6tRY0lOyN90Av48jbFQhMso05YAh5TNg:1AvwoAq1WuRM3zsG',
                b'belZJR2TJWCGqsyocGyeq1DfNjZEhfXq:guMA6W6F2uRUFxNt',
                b'GjbDBTXUizFTAI8WS79lgBOroQGfGZE3:cQheAdotEKQkLdEA']

    for keyss in key_list:

        token_url = 'https://ops.epo.org/3.2/auth/accesstoken'

        key = 'Basic %s' % base64.b64encode(keyss).decode('ascii')
        data = {'grant_type': 'client_credentials'}
        headers = {'Authorization': key, 'Content-Type': 'application/x-www-form-urlencoded',
                   'User-Agent': f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36'}
        r = requests.post(token_url, data=data, headers=headers)
        rs = r.content.decode()
        response = json.loads(rs)
        token = response['access_token']
        try:
            header = {'Authorization': "Bearer " + token}  # 헤더에 위에서 만든 토큰을 포함시켜 requests 요청을 보낼때 토큰정보가 전송되도록 해준다.
            date = '20200101'
            cql1 = 'A'
            service = 'published-data'
            begin_range = 1
            end_range = 1
            ipc_cpc = cql1
            myUrl = 'http://ops.epo.org/3.2/rest-services/' + service + '/search/' + '?Range=' + str(
                begin_range) + '-' + str(end_range) + '&q=pd%3D' + date + '%20not%20CN%20(ipc%3D' + str(
                ipc_cpc) + '%20or%20cpc%3D' + str(ipc_cpc) + ')'
            response = requests.get(myUrl, headers=header)
            content = response.content
            dict_type = xmltodict.parse(content)
            pub_info = dict_type['ops:world-patent-data']
            break
        except Exception as ex:
            print(ex)
    return token
def lv2_fun(Lv_1_cpc):  # lv2 구해주는 함수. lv2 의 존재유무가 각각 달라서 최적화한 코드를 진행시키기위해.
    cpc_lv1_all = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'Y']

    cpc_lv2_A = ['A01', 'A21', 'A22', 'A23', 'A24', 'A41', 'A42', 'A43', 'A44', 'A45', 'A46', 'A47', 'A61', 'A62',
                 'A63', 'A99']
    cpc_lv2_B = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B09', 'B21', 'B22', 'B23', 'B24', 'B25',
                 'B26', 'B27', 'B28', 'B29', 'B30', 'B31', 'B32', 'B33', 'B41', 'B42', 'B43', 'B44', 'B60', 'B61',
                 'B62', 'B63', 'B64', 'B65', 'B66', 'B67', 'B68', 'B81', 'B82', 'B99']
    cpc_lv2_C = ['C01', 'C02', 'C03', 'C04', 'C05', 'C06', 'C07', 'C08', 'C09', 'C10', 'C11', 'C12', 'C13', 'C14',
                 'C21', 'C22', 'C23', 'C25', 'C30', 'C40', 'C99']
    cpc_lv2_D = ['D01', 'D02', 'D03', 'D04', 'D05', 'D06', 'D07', 'D10', 'D21', 'D99']
    cpc_lv2_E = ['E01', 'E02', 'E03', 'E04', 'E05', 'E06', 'E21']
    cpc_lv2_F = ['F01', 'F02', 'F03', 'F04', 'F05', 'F15', 'F16', 'F17', 'F21', 'F22', 'F23', 'F24', 'F25', 'F26',
                 'F27', 'F28', 'F41', 'F42', 'F99']
    cpc_lv2_G = ['G01', 'G02', 'G03', 'G04', 'G05', 'G06', 'G07', 'G08', 'G09', 'G10', 'G11', 'G12', 'G16', 'G21',
                 'G99']
    cpc_lv2_H = ['H01', 'H02', 'H03', 'H04', 'H05', 'H99']
    cpc_lv2_Y = ['Y02', 'Y04', 'Y10']

    cpc_lv3_all = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'V', 'W',
                   'Y', 'Z']

    if Lv_1_cpc == 'A':
        return cpc_lv2_A
    elif Lv_1_cpc == 'B':
        return cpc_lv2_B
    elif Lv_1_cpc == 'C':
        return cpc_lv2_C
    elif Lv_1_cpc == 'D':
        return cpc_lv2_D
    elif Lv_1_cpc == 'E':
        return cpc_lv2_E
    elif Lv_1_cpc == 'F':
        return cpc_lv2_F
    elif Lv_1_cpc == 'G':
        return cpc_lv2_G
    elif Lv_1_cpc == 'H':
        return cpc_lv2_H
    elif Lv_1_cpc == 'Y':
        return cpc_lv2_Y
def data(start_date,end_date): # 코드 본문.
    host_ip = '35.236.54.169'
    user_id = 'root'
    #  EPO 회원 key 를 받아 넣어준다 EPO 서버 트래픽 관리차원.


    #  https://register.epo.org/help?lng=en&topic=countrycodes  epo 나라 분류 코드  반목문 작업시 시 참고
    #  cpc 정보가 있는 파일을 중복제거 , 슬라이스 하여 Lv 별로 나누어서 리스트로 놓았다.
    #  이유는 해당 코드로 총 결과(total result)가 2000개 이하로 나오게하기위한 cql 조건에 필요하기 때문이다.

    country_code = ['AL', 'AP', 'AR', 'AT', 'AU', 'BA', 'BE', 'BG', 'BR', 'CA', 'CH', 'CL', 'CN', 'CO', 'CR', 'CS',
                    'CU', 'CY', 'CZ', 'DD', 'DE', 'DK', 'DZ', 'EA', 'EC', 'EE', 'EG', 'EP', 'ES', 'FI', 'FR', 'GB',
                    'GC', 'GE', 'GR', 'GT', 'HK', 'HR', 'HU', 'ID', 'IE', 'IL', 'IN', 'IS', 'IT', 'JP', 'KE', 'KR',
                    'LI', 'LT', 'LU', 'LV', 'MA', 'MC', 'MD', 'MK', 'MN', 'MT', 'MW', 'MX', 'MY', 'NC', 'NI', 'NL',
                    'NO', 'NZ', 'OA', 'PA', 'PE', 'PH', 'PL', 'PT', 'RO', 'RU', 'SE', 'SG', 'SI', 'SK', 'SU', 'SV ',
                    'TJ', 'TR', 'TT', 'TW', 'UA', 'US', 'UY', 'VN', 'WO', 'YU', 'ZA', 'ZM', 'ZW']
    # 수집을 원하는 날짜를 반복입력 받는 리스트 함수.
    # dt_index = pandas.date_range(start='20190609', end='20190610')
    # daet_list = list(dt_index.strftime("%Y%m%d"))

    dt_index = pd.date_range(start=start_date, end=end_date)
    # pandas.date_range(start='20160901', end='20161031',freq='W-MON')
    # 을 하면 해당 기간 매주 월요일들만 추출합니다.

    # type(dt_index) => DatetimeIndex
    # DatetimeIndex => list(str)
    dt_list = dt_index.strftime("%Y%m%d").tolist()
    #  해당 리스트는 total_result 가 2000 이하 예를들어 300 개 일때 20번의 반복문을 돌면 비효율적이기때문에 아래의 수식으로 최적의 반복문만 돌기위한 사전작업이다.
    begin_max_num_list = ['1', '100', '200', '300', '400', '500', '600', '700', '800', '900', '1000', '1100', '1200',
                          '1300',
                          '1400', '1500', '1600', '1700', '1800', '1900']
    end_max_num_list = ['99', '199', '299', '399', '499', '599', '699', '799', '899', '999', '1099', '1199', '1299',
                        '1399',
                        '1499', '1599', '1699', '1799', '1899', '1999']

    pub_all_list = []
    begin_num_list = []
    end_num_list = []

    cpc_lv1_list = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'Y']
    cpc_lv3_all = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'V', 'W',
                   'Y']

    #  원하는 date list 를 time 모듈로 받고
    day_total_count_list = []
    for date in dt_list:

        try:
            conn = pymysql.connect(
                host=host_ip  # 외부
                , user='root'
                , password='hermes00!!'
                , database='TD'
                , port=3306
            )
            cur = conn.cursor()
            sql = """CREATE TABLE test.pub_num_data_no_CN_{} (family_id varchar(200),pub_country varchar(200),pub_doc_number varchar(200),pub_kind_code varchar(200),docdb_pub_num varchar(200));""".format(
                date)
            cur.execute(sql)
            conn.commit()
        except Exception as ex:
            print(ex, "DB CREATE 접속 오류.")
        finally:
            conn.close()
        print(cpc_lv1_list)

        for cql1 in cpc_lv1_list: # lv1 분류코드를 반복문돌린다.
            print(cql1)
            print('lv1 시작',date)
            time.sleep(15)
            header = {
                'Authorization': "Bearer " + str(make_token())}  # 헤더에 위에서 만든 토큰을 포함시켜 requests 요청을 보낼때 토큰정보가 전송되도록 해준다. EPO 메뉴얼 참고.
            try:
                # url 입력칸에 입력될 검색조건들.
                service = 'published-data' #검색형식
                begin_range = 1 #시작점
                end_range = 1 #끝점
                ipc_cpc = cql1

                myUrl = 'http://ops.epo.org/3.2/rest-services/' + service + '/search/' + '?Range=' + str(
                    begin_range) + '-' + str(end_range) + '&q=pd%3D' + date + '%20not%20CN%20(ipc%3D' + str(
                    ipc_cpc) + '%20or%20cpc%3D' + str(ipc_cpc) + ')' # url 정보.
                response = requests.get(myUrl, headers=header)
            except Exception as ex: # 만약 아무런 정보가 없다면 에러를 발생시켜 반복문을 탈출시킨다.
                print(ex)
            else: #try,except,else 구문 공부.
                xmlStr = response.text # 요청받은 xml 형식의 데이터를 str(xml) 형태로 만들어 준다.
                roots = ET.fromstring(xmlStr) # 다시 트리구조로 만들어서 파싱을 할수있도록 구조를 짜준다.

                lv1_begin_num_list = []
                lv1_end_num_list = []

                for total_result1 in roots.iter('{http://ops.epo.org}biblio-search'):

                    total_result_count1 = int(total_result1.attrib.get('total-result-count'))
                    print('lv1 total', total_result_count1, ' date=', date, ' cql=', cql1)

                    if total_result_count1 < 2000: # 확인이 가능한 결과수를 2000개 이하의 결과이기때문에 2000개 이하일때만 추출코드 시작. 아니라면 조건을 더추가하여 다시 2000개 카운팅.
                        day_total_count_list.append([date, cql1, total_result_count1])
                        num_list = int(total_result_count1 / 100)
                        begin_num = begin_max_num_list[:num_list + 1]
                        end_num = end_max_num_list[:num_list + 1]

                        lv1_begin_num_list.extend(begin_num)
                        lv1_end_num_list.extend(end_num)
                        pub_list_lv1 = []
                        for begin_num, end_num in zip(lv1_begin_num_list, lv1_end_num_list):
                            time.sleep(15)
                            header = {'Authorization': "Bearer " + str(make_token())}
                            try:

                                service = 'published-data'
                                begin_range = begin_num
                                end_range = end_num
                                ipc_cpc = cql1
                                myUrl = 'http://ops.epo.org/3.2/rest-services/' + service + '/search/' + '?Range=' + str(
                                    begin_range) + '-' + str(end_range) + '&q=pd%3D' + date + '%20not%20CN%20(ipc%3D' + str(
                                    ipc_cpc) + '%20or%20cpc%3D' + str(ipc_cpc) + ')'
                                response = requests.get(myUrl, headers=header)
                            except Exception as ex:
                                print(ex)
                            xmlStr = response.text
                            roots = ET.fromstring(xmlStr)

                            for publication_info in roots.iter('{http://ops.epo.org}publication-reference'):
                                family_id = publication_info.attrib.get('family-id')
                                pub_country = publication_info[0][0].text
                                pub_number = publication_info[0][1].text
                                pub_kind = publication_info[0][2].text
                                docdb_pub_number = pub_country + str(pub_number) + pub_kind
                                print(docdb_pub_number)
                                pub_list_lv1.append([family_id, pub_country, pub_number, pub_kind, docdb_pub_number])

                        try: # DB 저장 코드 해당 코드를 함수로 만든다면 코드가 더 간결해 질수있음.
                            conn = pymysql.connect(
                                host=host_ip
                                , user=user_id
                                , port=3306
                                , password='admin'
                                , database='test'
                                , write_timeout=300
                            )
                            cur = conn.cursor()

                            sql = """INSERT INTO pub_num_data_no_CN_{} VALUES(%s,%s,%s,%s,%s);""".format(date)
                            val = tuple(pub_list_lv1)
                            cur.executemany(sql, val)
                            conn.commit()
                        except Exception as ex:
                            print(ex, "Lv1 insert DB 오류")
                        finally:
                            conn.close()

                    else:
                        lv2_list = list(lv2_fun(cql1))
                        print(lv2_list)
                        for cql2 in lv2_list:
                            print('lv2 시작',date)
                            time.sleep(15)
                            header = {'Authorization': "Bearer " + str(make_token())}
                            try:

                                service = 'published-data'
                                begin_range = 1
                                end_range = 1
                                ipc_cpc = cql2
                                myUrl = 'http://ops.epo.org/3.2/rest-services/' + service + '/search/' + '?Range=' + str(
                                    begin_range) + '-' + str(end_range) + '&q=pd%3D' + date + '%20not%20CN%20(ipc%3D' + str(
                                    ipc_cpc) + '%20or%20cpc%3D' + str(ipc_cpc) + ')'
                                response = requests.get(myUrl, headers=header)
                            except Exception as ex:
                                print(ex, " : lv2 count response 에러")
                            else:
                                xmlStr = response.text
                                roots = ET.fromstring(xmlStr)

                            lv2_begin_num_list = []
                            lv2_end_num_list = []

                            for total_result2 in roots.iter('{http://ops.epo.org}biblio-search'):
                                total_result_count2 = int(total_result2.attrib.get('total-result-count'))
                                print('lv2 total', total_result_count2, ' date=', date, ' cql=', cql2)

                                if total_result_count2 < 2000:
                                    day_total_count_list.append([date, cql2, total_result_count2])
                                    num_list = int(total_result_count2 / 100)
                                    begin_num = begin_max_num_list[:num_list + 1]
                                    end_num = end_max_num_list[:num_list + 1]

                                    lv2_begin_num_list.extend(begin_num)
                                    lv2_end_num_list.extend(end_num)
                                    pub_list_lv2 = []

                                    for begin_num, end_num in zip(lv2_begin_num_list, lv2_end_num_list):
                                        time.sleep(15)
                                        header = {'Authorization': "Bearer " + str(make_token())}
                                        try:

                                            service = 'published-data'
                                            begin_range = begin_num
                                            end_range = end_num
                                            ipc_cpc = cql2
                                            myUrl = 'http://ops.epo.org/3.2/rest-services/' + service + '/search/' + '?Range=' + \
                                                    str(begin_range) + '-' + str(
                                                end_range) + '&q=pd%3D' + date + '%20not%20CN%20(ipc%3D' + str(
                                                ipc_cpc) + '%20or%20cpc%3D' + str(ipc_cpc) + ')'
                                            response = requests.get(myUrl, headers=header)
                                        except Exception as ex:
                                            print(ex, "lv2 range 반복문 에러")
                                        xmlStr = response.text
                                        roots = ET.fromstring(xmlStr)

                                        for publication_info in roots.iter('{http://ops.epo.org}publication-reference'):
                                            family_id = publication_info.attrib.get('family-id')
                                            pub_country = publication_info[0][0].text
                                            pub_number = publication_info[0][1].text
                                            pub_kind = publication_info[0][2].text
                                            docdb_pub_number = pub_country + str(pub_number) + pub_kind
                                            print(docdb_pub_number)
                                            pub_list_lv2.append(
                                                [family_id, pub_country, pub_number, pub_kind, docdb_pub_number])
                                    try:
                                        conn = pymysql.connect(
                                            host=host_ip
                                            , user=user_id
                                            , port=3306
                                            , password='admin'
                                            , database='test'
                                            , write_timeout=300

                                        )
                                        cur = conn.cursor()

                                        sql = """INSERT INTO pub_num_data_no_CN_{} VALUES(%s,%s,%s,%s,%s);""".format(date)
                                        val = tuple(pub_list_lv2)
                                        cur.executemany(sql, val)
                                        conn.commit()
                                    except Exception as ex:
                                        print(ex, "Lv2 insert 오류")
                                    finally:
                                        conn.close()

                                else:
                                    re_cql3_list = []
                                    for cql in cpc_lv3_all:
                                        re_cql = cql2 + cql
                                        re_cql3_list.append(re_cql)
                                    print(re_cql3_list)

                                    for cql3 in re_cql3_list:
                                        time.sleep(15)
                                        print('lv3 시작',date)
                                        header = {'Authorization': "Bearer " + str(make_token())}
                                        try:

                                            service = 'published-data'
                                            begin_range = 1
                                            end_range = 1
                                            ipc_cpc = cql3
                                            myUrl = 'http://ops.epo.org/3.2/rest-services/' + service + '/search/' + '?Range=' + str(
                                                begin_range) + '-' + str(
                                                end_range) + '&q=pd%3D' + date + '%20not%20CN%20(ipc%3D' + str(
                                                ipc_cpc) + '%20or%20cpc%3D' + str(ipc_cpc) + ')'
                                            response = requests.get(myUrl, headers=header)
                                        except Exception as ex:
                                            print(ex)
                                        else:
                                            xmlStr = response.text
                                            roots = ET.fromstring(xmlStr)

                                            lv3_begin_num_list = []
                                            lv3_end_num_list = []
                                            pub_list_lv3 = []
                                            for total_result3 in roots.iter('{http://ops.epo.org}biblio-search'):
                                                total_result_count3 = int(
                                                    total_result3.attrib.get('total-result-count'))
                                                print('lv3 total', total_result_count3, ' date=', date, ' cql=', cql3)

                                                if total_result_count3 < 2000:
                                                    day_total_count_list.append([date, cql3, total_result_count3])
                                                    num_list = int(total_result_count3 / 100)
                                                    begin_num = begin_max_num_list[:num_list + 1]
                                                    end_num = end_max_num_list[:num_list + 1]

                                                    lv3_begin_num_list.extend(begin_num)
                                                    lv3_end_num_list.extend(end_num)
                                                    for begin_num, end_num in zip(lv3_begin_num_list, lv3_end_num_list):
                                                        time.sleep(15)
                                                        header = {'Authorization': "Bearer " + str(make_token())}
                                                        try:

                                                            service = 'published-data'
                                                            begin_range = begin_num
                                                            end_range = end_num
                                                            ipc_cpc = cql3
                                                            myUrl = 'http://ops.epo.org/3.2/rest-services/' + service + '/search/' + '?Range=' + str(
                                                                begin_range) + '-' + str(
                                                                end_range) + '&q=pd%3D' + date + '%20not%20CN%20(ipc%3D' + str(
                                                                ipc_cpc) + '%20or%20cpc%3D' + str(ipc_cpc) + ')'
                                                            response = requests.get(myUrl, headers=header)
                                                        except Exception as ex:
                                                            print(ex)
                                                        xmlStr = response.text
                                                        roots = ET.fromstring(xmlStr)
                                                        for publication_info in roots.iter(
                                                                '{http://ops.epo.org}publication-reference'):
                                                            family_id = publication_info.attrib.get('family-id')
                                                            pub_country = publication_info[0][0].text
                                                            pub_number = publication_info[0][1].text
                                                            pub_kind = publication_info[0][2].text
                                                            docdb_pub_number = pub_country + str(pub_number) + pub_kind
                                                            print(docdb_pub_number)
                                                            pub_list_lv3.append(
                                                                [family_id, pub_country, pub_number, pub_kind,
                                                                 docdb_pub_number])
                                                    try:
                                                        conn = pymysql.connect(
                                                            host=host_ip
                                                            , user=user_id
                                                            , port=3306
                                                            , password='admin'
                                                            , database='test'
                                                            , write_timeout=300

                                                        )
                                                        cur = conn.cursor()

                                                        sql = """INSERT INTO pub_num_data_no_CN_{} VALUES(%s,%s,%s,%s,%s);""".format(
                                                            date)
                                                        val = tuple(pub_list_lv3)
                                                        cur.executemany(sql, val)
                                                        conn.commit()
                                                    except Exception as ex:
                                                        print(ex, "lv3 insert 오류")
                                                    finally:
                                                        conn.close()

                                                else:

                                                    for coun in country_code:
                                                        time.sleep(15)
                                                        header = {
                                                            'Authorization': "Bearer " + str(make_token())}
                                                        try:

                                                            service = 'published-data'
                                                            begin_range = 1
                                                            end_range = 1
                                                            ipc_cpc = cql3
                                                            myUrl = 'http://ops.epo.org/3.2/rest-services/' + service + '/search/' + '?Range=' + str(
                                                                begin_range) + '-' + str(
                                                                end_range) + '&q=pd%3D' + date + '%20not%20CN%20(ipc%3D' + str(
                                                                ipc_cpc) + '%20or%20cpc%3D' + str(
                                                                ipc_cpc) + '%20and%20ap%20%3D%20' + str(coun) + ')'
                                                            response = requests.get(myUrl, headers=header)
                                                        except Exception as ex:
                                                            print(ex)
                                                        else:
                                                            xmlStr = response.text
                                                            roots = ET.fromstring(xmlStr)

                                                            coun_begin_num_list = []
                                                            coun_end_num_list = []

                                                            for total_result in roots.iter(
                                                                    '{http://ops.epo.org}biblio-search'):
                                                                total_result_count = int(
                                                                    total_result.attrib.get('total-result-count'))
                                                                print('lv4 total', total_result_count, ' date=',
                                                                      date, ' cql=', cql3, ' coun=', coun)
                                                                day_total_count_list.append([date, cql3 + coun,
                                                                                            total_result_count3])
                                                                num_list = int(total_result_count / 100)

                                                                begin_num = begin_max_num_list[:num_list + 1]
                                                                end_num = end_max_num_list[:num_list + 1]

                                                                coun_begin_num_list.extend(begin_num)
                                                                coun_end_num_list.extend(end_num)
                                                                pub_list_lv3_country = []

                                                                for begin_num, end_num in zip(coun_begin_num_list,
                                                                                              coun_end_num_list):
                                                                    time.sleep(15)
                                                                    header = {
                                                                        'Authorization': "Bearer " + str(make_token())}
                                                                    try:

                                                                        service = 'published-data'
                                                                        begin_range = begin_num
                                                                        end_range = end_num
                                                                        ipc_cpc = cql3
                                                                        myUrl = 'http://ops.epo.org/3.2/rest-services/' + service + '/search/' + '?Range=' + str(
                                                                            begin_range) + '-' + str(
                                                                            end_range) + '&q=pd%3D' + date + '%20not%20CN%20(ipc%3D' + str(
                                                                            ipc_cpc) + '%20or%20cpc%3D' + str(
                                                                            ipc_cpc) + '%20and%20ap%20%3D%20' + str(
                                                                            coun) + ')'
                                                                        response = requests.get(myUrl,
                                                                                                headers=header)
                                                                    except Exception as ex:
                                                                        print(ex, 'lv4 반복문에러')

                                                                    xmlStr = response.text
                                                                    roots = ET.fromstring(xmlStr)

                                                                    for publication_info in roots.iter(
                                                                            '{http://ops.epo.org}publication-reference'):
                                                                        family_id = publication_info.attrib.get(
                                                                            'family-id')
                                                                        pub_country = publication_info[0][0].text
                                                                        pub_number = publication_info[0][1].text
                                                                        pub_kind = publication_info[0][2].text
                                                                        docdb_pub_number = pub_country + str(
                                                                            pub_number) + pub_kind
                                                                        print(docdb_pub_number)
                                                                        pub_list_lv3_country.append(
                                                                            [family_id, pub_country, pub_number,
                                                                             pub_kind, docdb_pub_number])

                                                                try:
                                                                    conn = pymysql.connect(
                                                                        host=host_ip
                                                                        , user=user_id
                                                                        , port=3306
                                                                        , password='admin'
                                                                        , database='test'
                                                                        , write_timeout=300
                                                                    )
                                                                    cur = conn.cursor()

                                                                    sql = """INSERT INTO pub_num_data_no_CN_{} VALUES(%s,%s,%s,%s,%s);""".format(
                                                                        date)
                                                                    val = tuple(pub_list_lv3_country)
                                                                    cur.executemany(sql, val)
                                                                    conn.commit()
                                                                except Exception as ex:
                                                                    print(ex, "lv4 insert 오류")
                                                                finally:
                                                                    conn.close()

    try: # 검증을 위한 total count 를 db 에 저장 하는 코드.
        conn = pymysql.connect(
            host=host_ip  # 외부
            , user='root'
            , password='hermes00!!'
            , database='TD'
            , port=3306
        )
        cur = conn.cursor()
        sql = """delete from date_total_count where `date` = {};""".format(date)
        cur.execute(sql)
        conn.commit()
    except Exception as ex:
        print(ex, "DB delete 접속 오류.")
    finally:
        conn.close()
    try:
        conn = pymysql.connect(
            host=host_ip
            , user=user_id
            , port=3306
            , password='admin'
            , database='test'
            , write_timeout=300
        )
        cur = conn.cursor()

        sql = """INSERT INTO date_total_count VALUES(%s,%s,%s);""".format(
            date)
        val = tuple(day_total_count_list)
        cur.executemany(sql, val)
        conn.commit()
    except Exception as ex:
        print(ex, "date_total_count")
    finally:
        conn.close()
def biblio(start_date,end_date):
    yesterday = datetime.today() - timedelta(1)
    date_m = yesterday.strftime("%Y%m")

    # ====================================DB 접속 및 publication-number 데이터 가져오기

    host_ip = '35.236.54.169'  # 내가 접속하려는 DB서버의 ip
    user_id = 'root'  # DB 서버에서 외부 접근 권한을 부여한 user id 본인은 root 계정으로 이름을 만들고 0.0.0.0 (모든 아이피 접근 권한) 을주었다.
    user_password = 'hermes00!!'  # 외부 권한 접근시(user_id : root) 필요한 password.
    port_number = 3306  # 외부 ip 가 접근할때 열어주는 포트번호 mariaDB 는 기본적으로 3306 포트를 사용한다.
    DB_database = 'TD'  # mariaDB 에서 작업할 database 이름.

    dt_index = pd.date_range(start=start_date, end=end_date)
    # yesterday = datetime.today() - timedelta(7) # 매일 매일 어제의 데이터(태이블)를 가지고 와야 할때
    # pandas.date_range(start='20200901', end='20201031',freq='W-MON') 을 하면 해당 기간 매주 월요일들만 추출합니다.
    # type(dt_index) => DatetimeIndex
    # DatetimeIndex => list(str)
    dt_list = dt_index.strftime("%Y%m%d").tolist()

    for date in dt_list:
        # db 에 저장되어있는 publication 정보를 가져오는 코드
        try:
            conn = pymysql.connect(
                host=host_ip
                , user=user_id
                , password=user_password
                , database=DB_database
                , port = port_number
                , connect_timeout = 300
            )
            cur = conn.cursor()
            sql = "SELECT DISTINCT * FROM pub_num_data_no_cn_{}".format(date) # date 날짜에 있는 pub_info 를 중복제거해서 가져와야함.(cpc,ipc 별로 가지고 왔기 때문에 중복되는 기술분류가 있을수 있음.)
            cur.execute(sql)
            conn.commit()
        except Exception as ex:
            print(ex,"DB 접속 오류.")
        finally:
            conn.close()
        datas = cur.fetchall()

        pub_doc_num_list = []
        country_list = []
        kind_list = []
        family_id_list = []
        for data in datas: # pandas 를 이용해 데이터를 로드라고 컬럼별로 list에 담는 작업.
            family_id = data[0]
            pub_country = data[1]
            pub_number = data[2]
            pub_kind = data[3]

            pub_doc_num_list.append(pub_number)
            country_list.append(pub_country)
            kind_list.append(pub_kind)
            family_id_list.append(family_id)
        # ====================================EPO 데이터 출력


        docdb_pub_num_list = []
        for coun,num,kind in zip(country_list,pub_doc_num_list,kind_list):
            docdb_pub_num = coun+str(num)+kind
            docdb_pub_num_list.append(docdb_pub_num)
        total_count = len(docdb_pub_num_list)

        # docdb_pub_num_join = ','.join(docdb_pub_num_list[:99])
        # print(docdb_pub_num_join)
        biblio_all_list = []
        for i in range(0, total_count+100 , 99): # 리스트를 100개 씩 잘라서 url 에 넣기위한 코드.
            docdb_pub_num_join = ','.join(docdb_pub_num_list[i:i+99]) # 100개씩 잘라진 str 묶음 형태의 list 를 join으로 한개의 str 으로 바꿔줌.
            print(docdb_pub_num_join)
            service = 'published-data'
            text_type = 'biblio'
            DB_type = 'docdb'
            header = {'Authorization': "Bearer " + str(make_token())} # 헤더에 위에서 만든 토큰을 포함시켜 requests 요청을 보낼때 토큰정보가 전송되도록 해준다.
            myUrl = 'http://ops.epo.org/3.2/rest-services/'+service+'/publication/'+DB_type+'/'+str(docdb_pub_num_join)+'/'+text_type
            time.sleep(15)
            response = requests.get(myUrl, headers=header) # url , header , proxies 정보를 담아서 요정을 보낸다.
            xmlStr = response.text
            root = ET.fromstring(xmlStr)
    #------------------------------------------파싱작업-----------------------------------------------------------
            for roots in root.iter('{http://www.epo.org/exchange}exchange-document'):
                # ---------------------family_id----------------------------

                family_id = roots.attrib.get('family-id')
                # ---------------------publication 정보----------------------------
                for pub_info in roots.iter('{http://www.epo.org/exchange}publication-reference'):
                    pub_country_docdb = pub_info[0][0].text
                    pub_doc_number_docdb = pub_info[0][1].text
                    pub_kind_docdb = pub_info[0][2].text
                    pub_date_docdb = pub_info[0][3].text
                    pub_doc_number_epo = pub_info[1][0].text
                    pub_date_epo = pub_info[1][1].text

                # ---------------------ipc 정보----------------------------
                ipcr_list = []
                for classifications_ipcr in roots.iter('{http://www.epo.org/exchange}classifications-ipcr'):

                    for ipcr in classifications_ipcr.iter('{http://www.epo.org/exchange}text'):
                        ipcr_text = ipcr.text.replace(' ', '')
                        ipcr_list.extend([ipcr_text])
                if len(ipcr_list) == 0:
                    ipcr_list.append('None')
                else:
                    pass
                ipcr_join = '|'.join(ipcr_list)


                # ---------------------cpc 정보----------------------------
                cpci_list = []
                for patent_classifications in roots.iter('{http://www.epo.org/exchange}patent-classifications'):

                    for section, classification_class, subclass, main_group, subgroup, classification_value in zip(
                            patent_classifications.iter('{http://www.epo.org/exchange}section')
                            , patent_classifications.iter('{http://www.epo.org/exchange}class')
                            , patent_classifications.iter('{http://www.epo.org/exchange}subclass')
                            , patent_classifications.iter('{http://www.epo.org/exchange}main-group')
                            , patent_classifications.iter('{http://www.epo.org/exchange}subgroup')
                            , patent_classifications.iter('{http://www.epo.org/exchange}classification-value')
                    ):
                        cpci_list.extend([
                            section.text
                            + classification_class.text
                            + subclass.text
                            + main_group.text
                            + '/'
                            + subgroup.text
                            + classification_value.text
                        ])
                if len(cpci_list) == 0:
                    cpci_list.append('None')
                else:
                    pass
                cpci_join = ('|').join(cpci_list)

                # ---------------------application 정보----------------------------
                for app_info in roots.iter('{http://www.epo.org/exchange}application-reference'):
                    app_doc_id = app_info.attrib.get('doc-id')
                    app_country_docdb = app_info[0][0].text
                    app_doc_number_docdb = app_info[0][1].text
                    app_kind_docdb = app_info[0][2].text
                    app_doc_number_epo = app_info[1][0].text
                    app_date_epo = app_info[1][1].text
                pc_app_doc_num_list = []
                pc_app_date_list = []
                for priority_claims in roots.iter('{http://www.epo.org/exchange}priority-claims'):
                    for priority_claim in priority_claims.iter('{http://www.epo.org/exchange}priority-claim'):
                        priority_claim_kind = priority_claim.attrib.get('kind')
                        if priority_claim_kind.startswith('n'):
                            for priority_claim_num in priority_claim.iter('{http://www.epo.org/exchange}doc-number'):
                                pc_app_doc_num = priority_claim_num.text
                                pc_app_doc_num_list.append(pc_app_doc_num)
                            for priority_claim_date in priority_claim.iter('{http://www.epo.org/exchange}date'):
                                pc_app_date = priority_claim_date.text
                                pc_app_date_list.append(pc_app_date)
                        else:
                            pass
                if len(pc_app_doc_num_list) == 0:
                    pc_app_doc_num_list.append('None')
                if len(pc_app_date_list) == 0:
                    pc_app_date_list.append('None')
                else:
                    pass
                pc_app_doc_num_list_join = '|'.join(pc_app_doc_num_list)
                pc_app_date_list_join = '|'.join(pc_app_date_list)
                applicants_epo_list = []
                applicants_ol_list = []
                for applicants in roots.iter('{http://www.epo.org/exchange}applicants'):
                    for applicant in applicants.iter('{http://www.epo.org/exchange}applicant'):
                        applicant_data_format = applicant.attrib.get('data-format')
                        if applicant_data_format.startswith('e'):
                            applicant_epo = applicant[0][0].text
                            applicants_epo_list.extend([applicant_epo])
                        elif applicant_data_format.startswith('o'):
                            applicant_ol = applicant[0][0].text
                            applicants_ol_list.extend([applicant_ol])
                if len(applicants_epo_list) == 0:
                    applicants_epo_list.append('None')
                if len(applicants_ol_list) == 0:
                    applicants_ol_list.append('None')
                else:
                    pass
                applicants_epo_list_join = '|'.join(applicants_epo_list)
                applicants_ol_list_join = '|'.join(applicants_ol_list)


                # ---------------------inventors 정보----------------------------

                inventors_epo_list = []
                inventors_ol_list = []
                for inventor in roots.iter('{http://www.epo.org/exchange}inventor'):
                    inventor_data_format = inventor.attrib.get('data-format')
                    if inventor_data_format.startswith('e'):
                        for inventor_text in inventor.iter('{http://www.epo.org/exchange}name'):
                            inventor_epo = inventor_text.text
                            inventors_epo_list.extend([inventor_epo])
                    elif inventor_data_format.startswith('o'):
                        for inventor_text in inventor.iter('{http://www.epo.org/exchange}name'):
                            inventor_epo = inventor_text.text
                            inventors_ol_list.extend([inventor_epo])

                if len(inventors_epo_list) == 0:
                    inventors_epo_list.append('None')
                if len(inventors_ol_list) == 0:
                    inventors_ol_list.append('None')
                else:
                    pass
                inventors_epo_list_join = '|'.join(inventors_epo_list)
                inventors_ol_list_join = '|'.join(inventors_ol_list)


                # ---------------------invent 정보----------------------------
                invent_title_en_list = []
                invent_title_ol_list = []
                invent_title_else_list = []
                for exchange_document in roots.iter('{http://www.epo.org/exchange}exchange-document'):

                    for invent_title in exchange_document.iter('{http://www.epo.org/exchange}invention-title'):
                        invent_title_lang = invent_title.attrib.get('lang')
                        if invent_title_lang.startswith('e'):
                            invent_title_en_text = invent_title.text
                            invent_title_en_list.extend([invent_title_en_text])
                        elif invent_title_lang.startswith('o'):
                            invent_title_ol_text = invent_title.text
                            invent_title_ol_list.extend([invent_title_ol_text])
                        else:
                            invent_title_else_text = invent_title.text
                            invent_title_else_list.extend([invent_title_else_text])
                if len(invent_title_en_list) == 0:
                    invent_title_en_list.append('None')
                if len(invent_title_ol_list) == 0:
                    invent_title_ol_list.append('None')
                if len(invent_title_else_list) == 0:
                    invent_title_else_list.append('None')
                else:
                    pass
                invent_title_en_list_join = '|'.join(invent_title_en_list)
                invent_title_ol_list_join = '|'.join(invent_title_ol_list)
                invent_title_else_list_join = '|'.join(invent_title_else_list)


                # ---------------------abstract 정보----------------------------
                abstract_en_list = []
                abstract_ol_list = []
                abstract_else_list = []
                for exchange_document in roots.iter('{http://www.epo.org/exchange}exchange-document'):

                    for abstract in exchange_document.iter('{http://www.epo.org/exchange}abstract'):
                        abstract_lang = abstract.attrib.get('lang')
                        if abstract_lang.startswith('e'):
                            for abstract_text in abstract.iter('{http://www.epo.org/exchange}p'):
                                abstract_text_en = abstract_text.text
                                abstract_en_list.extend([abstract_text_en])
                        elif abstract_lang.startswith('o'):
                            for abstract_text in abstract.iter('{http://www.epo.org/exchange}p'):
                                abstract_text_ol = abstract_text.text
                                abstract_ol_list.extend([abstract_text_ol])
                        else:
                            for abstract_text in abstract.iter('{http://www.epo.org/exchange}p'):
                                abstract_text_else = abstract_text.text
                                abstract_else_list.extend([abstract_text_else])
                if len(abstract_en_list) == 0:
                    abstract_en_list.append('None')
                if len(abstract_ol_list) == 0:
                    abstract_ol_list.append('None')
                if len(abstract_else_list) == 0:
                    abstract_else_list.append('None')
                else:
                    pass
                abstract_en_list_join = '|'.join(abstract_en_list)
                abstract_ol_list_join = '|'.join(abstract_ol_list)
                abstract_else_list_join = '|'.join(abstract_else_list)

                # ------------------------------------citation  추출 영역 심사관일때------------------------------------
                citaion_e_list = []
                for citaion in roots.iter('{http://www.epo.org/exchange}citation'):
                    test = citaion.attrib.get('cited-by')
                    if test.startswith('e') == True:
                        try:
                            for patcit in citaion.iter('{http://www.epo.org/exchange}patcit'):
                                country = patcit[1][0].text
                                doc_number = patcit[1][1].text
                                kind = patcit[1][2].text
                                citaion_e_list.extend([country + doc_number + kind])
                        except Exception as ex:
                            print(ex,"심사관 인용정보 추출 오류")
                    else:
                        pass
                my_e_set = list(set(citaion_e_list))
                count_citaion_e_list = str(len(my_e_set))

                # ------------------------------------citation  추출 영역 본인 일때------------------------------------

                citaion_a_list = []
                for citaion in roots.iter('{http://www.epo.org/exchange}citation'):
                    test = citaion.attrib.get('cited-by')

                    if test.startswith('a') == True:
                        try:
                            for patcit in citaion.iter('{http://www.epo.org/exchange}patcit'):
                                country = patcit[1][0].text
                                doc_number = patcit[1][1].text
                                kind = patcit[1][2].text
                                if kind == None:
                                    kind = ' '
                                citaion_a_list.extend([country + doc_number + kind])
                        except Exception as ex:
                            print(ex,"인용정보 추출 오류")
                    else:
                        pass

                if len(citaion_a_list) == 0:
                    citaion_a_list.append('None')

                if len(citaion_e_list) == 0:
                    citaion_e_list.append('None')
                else:
                    pass
                my_a_set = list(set(citaion_a_list))
                count_citaion_a_list = str(len(my_a_set))

                citaion_e_list_join = '|'.join(my_e_set)
                count_citaion_e_list_join = '|'.join(count_citaion_e_list)

                citaion_a_list_join = '|'.join(my_a_set)
                count_citaion_a_list_join = '|'.join(count_citaion_a_list)


                biblio_all_list.append([app_doc_id
                                       , app_country_docdb
                                       , app_doc_number_docdb
                                       , app_kind_docdb
                                       , app_doc_number_epo
                                       , app_date_epo
                                        , pub_country_docdb
                                        , pub_doc_number_docdb
                                        , pub_kind_docdb
                                        , pub_date_docdb
                                        , pub_doc_number_epo
                                        , pub_date_epo
                                        , ipcr_join
                                        , cpci_join
                                       , applicants_epo_list_join
                                       , applicants_ol_list_join
                                       , inventors_epo_list_join
                                       , inventors_ol_list_join
                                       , invent_title_en_list_join
                                       , invent_title_ol_list_join
                                       , invent_title_else_list_join
                                       , abstract_en_list_join
                                       , abstract_ol_list_join
                                       , abstract_else_list_join
                                        , citaion_e_list_join
                                        , count_citaion_e_list_join
                                        , citaion_a_list_join
                                        , count_citaion_a_list_join
                                        ,pc_app_doc_num_list_join
                                        ,pc_app_date_list_join
                                        ,family_id
                                        ])



            # =============================DB insurt
        try:
            conn = pymysql.connect(
                host=host_ip
                , user=user_id
                , password=user_password
                , database=DB_database
                , port = port_number
            )

            cur = conn.cursor()
            sql_1 = "INSERT INTO biblio_info_{} VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);".format(date_m)
            val = tuple(biblio_all_list)
            cur.executemany(sql_1,val)
            conn.commit()
        except Exception as ex:
            print(ex,"DB insert 오류")
        finally:
            conn.close()

#------------------------------------------daily-----------------------------

def task_1():
    yesterday = datetime.today() - timedelta(7)
    yesterday_date = yesterday.strftime("%Y%m%d")
    data(yesterday_date,yesterday_date)
def task_2():
    yesterday = datetime.today() - timedelta(7)
    yesterday_date = yesterday.strftime("%Y%m%d")
    biblio(yesterday_date, yesterday_date)

default_dag_args = {
    "owner": "airflow",
    "start_date": datetime.today()
}
dag = DAG(
    dag_id='daily_epo'
    , default_args=default_dag_args
    , schedule_interval='@daily'
    # , schedule_interval=timedelta(1)
)
task1 = PythonOperator(
    task_id='pub_num_DB'
    , python_callable= task_1
    , dag=dag
)

task2 = PythonOperator(
    task_id='biblio'
    , python_callable= task_2
    , dag=dag
)

task1 >> task2
