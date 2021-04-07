import xml.etree.ElementTree as ET
import epo_ops
import csv
import time
from datetime import datetime, timedelta
import pandas
import pymysql
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

def lv2_fun(Lv_1_cpc):
    cpc_lv1_all = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'Y']

    cpc_lv2_A = ['A01', 'A21', 'A22', 'A23', 'A24', 'A41', 'A42', 'A43', 'A44', 'A45', 'A46', 'A47', 'A61', 'A62',
                 'A63']
    cpc_lv2_B = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B09', 'B21', 'B22', 'B23', 'B24', 'B25',
                 'B26', 'B27', 'B28', 'B29', 'B30', 'B31', 'B32', 'B33', 'B41', 'B42', 'B43', 'B44', 'B60', 'B61',
                 'B62', 'B63', 'B64', 'B65', 'B66', 'B67', 'B68', 'B81', 'B82']
    cpc_lv2_C = ['C01', 'C02', 'C03', 'C04', 'C05', 'C06', 'C07', 'C08', 'C09', 'C10', 'C11', 'C12', 'C13', 'C14',
                 'C21', 'C22', 'C23', 'C25', 'C30', 'C40']
    cpc_lv2_D = ['D01', 'D02', 'D03', 'D04', 'D05', 'D06', 'D07', 'D10', 'D21']
    cpc_lv2_E = ['E01', 'E02', 'E03', 'E04', 'E05', 'E06', 'E21']
    cpc_lv2_F = ['F01', 'F02', 'F03', 'F04', 'F05', 'F15', 'F16', 'F17', 'F21', 'F22', 'F23', 'F24', 'F25', 'F26',
                 'F27', 'F28', 'F41', 'F42']
    cpc_lv2_G = ['G01', 'G02', 'G03', 'G04', 'G05', 'G06', 'G07', 'G08', 'G09', 'G10', 'G11', 'G12', 'G16', 'G21']
    cpc_lv2_H = ['H01', 'H02', 'H03', 'H04', 'H05']
    cpc_lv2_Y = ['Y02', 'Y04', 'Y10']

    cpc_lv3_all = ['H', 'N', 'L', 'J', 'Q', 'Y', 'B', 'D', 'P', 'M', 'V', 'C', 'F', 'K', 'G']

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

def pub_num_db():
    start = time.time()
    host_ip = '34.68.250.64'
    user_id = 'root'
    #  EPO 회원 key 를 받아 넣어준다 EPO 서버 트래픽 관리차원.
    client = epo_ops.Client(
        key='o2TgZqLMPnGxmiFk7rUUB0bTq9VZDbe1',
        secret='ZHYxcGxi9UprBTUD'
    )

    #  https://register.epo.org/help?lng=en&topic=countrycodes  epo 나라 분류 코드  반목문 작업시 시 참고
    #  cpc 정보가 있는 파일을 중복제거 , 슬라이스 하여 Lv 별로 나누어서 리스트로 놓았다.
    #  이유는 해당 코드로 총 결과(total result)가 2000개 이하로 나오게하기위한 cql 조건에 필요하기 때문이다.



    country_code =['AL','AP','AR','AT','AU','BA','BE','BG','BR','CA','CH','CL','CN','CO','CR','CS','CU','CY','CZ','DD','DE','DK','DZ','EA','EC','EE','EG','EP','ES','FI','FR','GB','GC','GE','GR','GT','HK','HR','HU','ID','IE','IL','IN','IS','IT','JP','KE','KR','LI','LT','LU','LV','MA','MC','MD','MK','MN','MT','MW','MX','MY','NC','NI','NL','NO','NZ','OA','PA','PE','PH','PL','PT','RO','RU','SE','SG','SI','SK','SU','SV ','TJ','TR','TT','TW','UA','US','UY','VN','WO','YU','ZA','ZM','ZW']
    # 수집을 원하는 날짜를 반복입력 받는 리스트 함수.
    # dt_index = pandas.date_range(start='20190609', end='20190610')
    # daet_list = list(dt_index.strftime("%Y%m%d"))

    yesterday = datetime.today() - timedelta(7)

    # date = yesterday.strftime("%Y%m%d")
    date = '20210324'
    #  해당 리스트는 total_result 가 2000 이하 예를들어 300 개 일때 20번의 반복문을 돌면 비효율적이기때문에 아래의 수식으로 최적의 반복문만 돌기위한 사전작업이다.
    begin_max_num_list = ['1', '100', '200', '300', '400', '500', '600', '700', '800', '900', '1000', '1100', '1200', '1300',
                     '1400', '1500', '1600', '1700', '1800', '1900']
    end_max_num_list = ['99', '199', '299', '399', '499', '599', '699', '799', '899', '999', '1099', '1199', '1299', '1399',
                   '1499', '1599', '1699', '1799', '1899', '1999']

    pub_all_list = []
    begin_num_list = []
    end_num_list = []

    cpc_lv1_list =['A','B','C','D','E','F','G','H','Y']
    cpc_lv3_all = ['A','B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'V', 'W', 'Y']

    #  원하는 date list 를 time 모듈로 받고

    conn = pymysql.connect(
        host=host_ip
        , user=user_id
        , port=3306
        , password='admin'
        , database='test'
    )
    cur = conn.cursor()
    sql = """CREATE TABLE test.pub_num_data_{} (family_id varchar(200),pub_country varchar(200),pub_doc_number varchar(200),pub_kind_code varchar(200));""".format(date)
    cur.execute(sql)
    conn.commit()
    conn.close()
    print(cpc_lv1_list)

    for cql1 in cpc_lv1_list:
        print(cql1)
        print('lv1 시작')
        try:
            response = client.published_data_search(
                cql='pd={date} and (ipc={cql} or cpc={cql})'.format(date=date,cql=cql1)
                , range_begin= 1
                , range_end= 1
                , constituents=['biblio']
            )
        except Exception as ex:
            print(ex)
            pass
        else:
            xmlStr = response.text
            roots = ET.fromstring(xmlStr)

            lv1_begin_num_list = []
            lv1_end_num_list = []

            for total_result1 in roots.iter('{http://ops.epo.org}biblio-search'):
                total_result_count1 = int(total_result1.attrib.get('total-result-count'))
                print('lv1 total', total_result_count1, ' date=', date, ' cql=', cql1)

                if total_result_count1 < 2000:
                    num_list = int(total_result_count1 / 100)
                    begin_num = begin_max_num_list[:num_list + 1]
                    end_num = end_max_num_list[:num_list + 1]

                    lv1_begin_num_list.extend(begin_num)
                    lv1_end_num_list.extend(end_num)
                    pub_list_lv1 = []
                    for begin_num, end_num in zip(lv1_begin_num_list, lv1_end_num_list):
                        response = client.published_data_search(
                            cql='pd={date} and (ipc={cql} or cpc={cql})'.format(date=date, cql=cql1)
                            , range_begin=begin_num
                            , range_end=end_num
                            , constituents=['biblio']
                        )
                        xmlStr = response.text
                        roots = ET.fromstring(xmlStr)

                        for exchange_document in roots.iter('{http://www.epo.org/exchange}exchange-document'):
                            doc_simple_id = exchange_document.attrib.get('family-id')
                            pub_country = exchange_document.attrib.get('country')
                            pub_doc_number = exchange_document.attrib.get('doc-number')
                            pub_kind = exchange_document.attrib.get('kind')
                            pub_list_lv1.append([doc_simple_id
                                                 ,pub_country
                                                 ,pub_doc_number
                                                 ,pub_kind])

                    try:
                        conn = pymysql.connect(
                            host=host_ip
                            , user=user_id
                            , port=3306
                            , password='admin'
                            , database='test'

                        )
                        cur = conn.cursor()
                        for row in pub_list_lv1:
                            sql = """INSERT INTO pub_num_data_{} VALUES(%s,%s,%s,%s);""".format(date)
                            val = row
                            cur.execute(sql, val)
                            conn.commit()
                    except Exception as ex:
                        print(ex,"Lv1 insert DB 오류")
                    finally:
                        conn.close()

                else:
                    lv2_list = list(lv2_fun(cql1))
                    print(lv2_list)
                    for cql2 in lv2_list:
                        print('lv2 시작')
                        try:
                            response = client.published_data_search(
                                cql='pd={date} and (ipc={cql} or cpc={cql})'.format(date=date, cql=cql2)
                                , range_begin=1
                                , range_end=1
                                , constituents=['biblio']
                            )
                        except:
                            pass
                        else:

                            xmlStr = response.text
                            roots = ET.fromstring(xmlStr)

                            lv2_begin_num_list = []
                            lv2_end_num_list = []

                            for total_result2 in roots.iter('{http://ops.epo.org}biblio-search'):
                                total_result_count2 = int(total_result2.attrib.get('total-result-count'))
                                print('lv2 total', total_result_count2, ' date=', date, ' cql=', cql2)
                                if total_result_count2 < 2000:
                                    num_list = int(total_result_count2 / 100)
                                    begin_num = begin_max_num_list[:num_list + 1]
                                    end_num = end_max_num_list[:num_list + 1]

                                    lv2_begin_num_list.extend(begin_num)
                                    lv2_end_num_list.extend(end_num)
                                    pub_list_lv2 = []
                                    for begin_num, end_num in zip(lv2_begin_num_list, lv2_end_num_list):
                                        response = client.published_data_search(
                                            cql='pd={date} and (ipc={cql} or cpc={cql})'.format(date=date, cql=cql2)
                                            , range_begin=begin_num
                                            , range_end=end_num
                                            , constituents=['biblio']
                                        )
                                        xmlStr = response.text
                                        roots = ET.fromstring(xmlStr)

                                        for exchange_document in roots.iter('{http://www.epo.org/exchange}exchange-document'):
                                            doc_simple_id = exchange_document.attrib.get('family-id')
                                            pub_country = exchange_document.attrib.get('country')
                                            pub_doc_number = exchange_document.attrib.get('doc-number')
                                            pub_kind = exchange_document.attrib.get('kind')

                                            pub_list_lv2.append([doc_simple_id
                                                                 ,pub_country
                                                                 ,pub_doc_number
                                                                 ,pub_kind])
                                    try:
                                        conn = pymysql.connect(
                                            host=host_ip
                                            , user=user_id
                                            , port=3306
                                            , password='admin'
                                            , database='test'

                                        )
                                        cur = conn.cursor()
                                        for row in pub_list_lv2:
                                            sql = """INSERT INTO pub_num_data_{} VALUES(%s,%s,%s,%s);""".format(date)
                                            val = row
                                            cur.execute(sql, val)
                                            conn.commit()
                                    except Exception as ex:
                                        print(ex,"Lv2 insert 오류")
                                    finally:
                                        conn.close()

                                else:
                                    re_cql3_list =[]
                                    for cql in cpc_lv3_all:
                                        re_cql = cql2+cql
                                        re_cql3_list.append(re_cql)
                                    print(re_cql3_list)
                                    for cql3 in re_cql3_list:

                                        print('lv3 시작')
                                        try:
                                            response = client.published_data_search(
                                                cql='pd={date} and (ipc={cql} or cpc={cql})'.format(date=date, cql=cql3)
                                                , range_begin=1
                                                , range_end=1
                                                , constituents=['biblio']
                                            )
                                        except:
                                            pass
                                        else:

                                            xmlStr = response.text
                                            roots = ET.fromstring(xmlStr)

                                            lv3_begin_num_list = []
                                            lv3_end_num_list = []
                                            pub_list_lv3 = []
                                            for total_result3 in roots.iter('{http://ops.epo.org}biblio-search'):
                                                total_result_count3 = int(total_result3.attrib.get('total-result-count'))
                                                print('lv3 total', total_result_count3, ' date=', date, ' cql=', cql3)
                                                if total_result_count3 < 2000:
                                                    num_list = int(total_result_count3 / 100)
                                                    begin_num = begin_max_num_list[:num_list + 1]
                                                    end_num = end_max_num_list[:num_list + 1]

                                                    lv3_begin_num_list.extend(begin_num)
                                                    lv3_end_num_list.extend(end_num)
                                                    for begin_num, end_num in zip(lv3_begin_num_list, lv3_end_num_list):
                                                        response = client.published_data_search(
                                                            cql='pd={date} and (ipc={cql} or cpc={cql})'.format(date=date, cql=cql3)
                                                            , range_begin=begin_num
                                                            , range_end=end_num
                                                            , constituents=['biblio']
                                                        )
                                                        xmlStr = response.text
                                                        roots = ET.fromstring(xmlStr)
                                                        for exchange_document in roots.iter(
                                                                '{http://www.epo.org/exchange}exchange-document'):
                                                            doc_simple_id = exchange_document.attrib.get(
                                                                'family-id')
                                                            pub_country = exchange_document.attrib.get('country')
                                                            pub_doc_number = exchange_document.attrib.get(
                                                                'doc-number')
                                                            pub_kind = exchange_document.attrib.get('kind')
                                                            pub_list = [doc_simple_id, pub_country, pub_doc_number,
                                                                        pub_kind]
                                                            pub_list_lv3.append(doc_simple_id
                                                                                ,pub_country
                                                                                ,pub_doc_number
                                                                                ,pub_kind)
                                                    try:
                                                        conn = pymysql.connect(
                                                            host=host_ip
                                                            , user=user_id
                                                            , port=3306
                                                            , password='admin'
                                                            , database='test'


                                                        )
                                                        cur = conn.cursor()
                                                        for row in pub_list_lv3:
                                                            sql = """INSERT INTO pub_num_data_{} VALUES(%s,%s,%s,%s);""".format(date)
                                                            val = row
                                                            cur.execute(sql, val)
                                                            conn.commit()
                                                    except Exception as ex:
                                                        print(ex,"lv3 insert 오류")
                                                    finally:
                                                        conn.close()

                                                else:
                                                    for cql3 in re_cql3_list:
                                                        for coun in country_code:
                                                            try:
                                                                response = client.published_data_search(
                                                                    cql='pd={date} and (ipc={cql} or cpc={cql}) and ap={coun}'.format(
                                                                        date=date, cql=cql3, coun=coun)
                                                                    , range_begin=1
                                                                    , range_end=1
                                                                    , constituents=['biblio']
                                                                )
                                                            except:
                                                                pass
                                                            else:

                                                                xmlStr = response.text
                                                                roots = ET.fromstring(xmlStr)

                                                                coun_begin_num_list = []
                                                                coun_end_num_list = []

                                                                for total_result in roots.iter('{http://ops.epo.org}biblio-search'):
                                                                    total_result_count = int(
                                                                        total_result.attrib.get('total-result-count'))
                                                                    print('lv4 total', total_result_count, ' date=', date, ' cql=',
                                                                          cql3, ' coun=', coun)
                                                                    num_list = int(total_result_count / 100)

                                                                    begin_num = begin_max_num_list[:num_list + 1]
                                                                    end_num = end_max_num_list[:num_list + 1]

                                                                    coun_begin_num_list.extend(begin_num)
                                                                    coun_end_num_list.extend(end_num)
                                                                    pub_list_lv3_country = []
                                                                    for begin_num, end_num in zip(coun_begin_num_list, coun_end_num_list):

                                                                        response = client.published_data_search(
                                                                            cql='pd={date} and (ipc={cql} or cpc={cql} and ap={coun})'.format(
                                                                                date=date, cql=cql3, coun=coun)
                                                                            , range_begin=begin_num
                                                                            , range_end=end_num
                                                                            , constituents=['biblio']
                                                                        )

                                                                        xmlStr = response.text
                                                                        roots = ET.fromstring(xmlStr)

                                                                        for exchange_document in roots.iter(
                                                                                '{http://www.epo.org/exchange}exchange-document'):
                                                                            doc_simple_id = exchange_document.attrib.get('family-id')
                                                                            pub_country = exchange_document.attrib.get('country')
                                                                            pub_doc_number = exchange_document.attrib.get('doc-number')
                                                                            pub_kind = exchange_document.attrib.get('kind')

                                                                            pub_list_lv3_country.append([doc_simple_id
                                                                                                         ,pub_country
                                                                                                         ,pub_doc_number
                                                                                                         ,pub_kind])

                                                                    try:
                                                                        conn = pymysql.connect(
                                                                            host=host_ip
                                                                            , user=user_id
                                                                            , port=3306
                                                                            , password='admin'
                                                                            , database='test'
                                                                        )
                                                                        cur = conn.cursor()
                                                                        for row in pub_list_lv3_country:
                                                                            sql = """INSERT INTO pub_num_data_{} VALUES(%s,%s,%s,%s,%s);""".format(date)
                                                                            val = row
                                                                            cur.execute(sql, val)
                                                                            conn.commit()
                                                                    except Exception as ex:
                                                                        print(ex,"lv4 insert 오류")
                                                                    finally:
                                                                        conn.close()
    print(time.time()-start)



def bibio_citaion_DB():
    host_ip = '34.68.250.64'
    biblio_all_list = []
    yesterday = datetime.today() - timedelta(7)

    date = '20210324'
    # db 에 저장되어있는 publication 정보를 가져오는 코드
    try:
        conn = pymysql.connect(
            host=host_ip
            , user='root'
            , password='admin'
            , database='test'
            , port = 3306
            , connect_timeout = 3000
        )
        cur = conn.cursor()
        sql = "SELECT DISTINCT * FROM pub_num_data_{} LIMIT10".format(date)
        cur.execute(sql)
        conn.commit()
    except Exception as ex:
        print(ex,"DB 접속 오류.")
    finally:
        conn.close()
    datas = cur.fetchall()

    app_doc_num_list = []
    country_list = []
    kind_list = []
    family_id_list = []
    for data in datas:
        family_id = data[1]
        pub_country = data[2]
        pub_number = data[3]
        pub_kind = data[4]

        app_doc_num_list.append(pub_number)
        country_list.append(pub_country)
        kind_list.append(pub_kind)
        family_id_list.append(family_id)

    client = epo_ops.Client(key='o2TgZqLMPnGxmiFk7rUUB0bTq9VZDbe1', secret='ZHYxcGxi9UprBTUD')
    for doc_num, country, kind in zip(app_doc_num_list, country_list, kind_list):
        print(doc_num)
        try:
            response = client.published_data(
                reference_type='publication'
                , input=epo_ops.models.Docdb(str(doc_num), country, kind)
                , endpoint='biblio'
            )
        except Exception as ex:
            print(ex)
        else:
            xmlStr = response.text
            roots = ET.fromstring(xmlStr)
            # ---------------------family_id----------------------------
            for family_info in roots.iter('{http://www.epo.org/exchange}exchange-document'):
                family_id = family_info.attrib.get('family-id')
            # ---------------------publication 정보----------------------------
            for pub_info in roots.iter('{http://www.epo.org/exchange}publication-reference'):
                pub_country_docdb = pub_info[0][0].text
                pub_doc_number_docdb = pub_info[0][1].text
                pub_kind_docdb = pub_info[0][2].text
                pub_date_docdb = pub_info[0][3].text
                pub_doc_number_epo = pub_info[1][0].text
                pub_date_epo = pub_info[1][1].text


            # ---------------------ipc 정보----------------------------
            for classifications_ipcr in roots.iter('{http://www.epo.org/exchange}classifications-ipcr'):
                ipcr_list = []
                for ipcr in classifications_ipcr.iter('{http://www.epo.org/exchange}text'):
                    ipcr_text = ipcr.text.replace(' ', '')
                    ipcr_list.extend([ipcr_text])
                if len(ipcr_list) == 0:
                    ipcr_list.append('None')
                else:
                    pass
                ipcr_join = '|'.join(ipcr_list)

            # ---------------------cpc 정보----------------------------
            for patent_classifications in roots.iter('{http://www.epo.org/exchange}patent-classifications'):
                cpci_list = []
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


            # for priority_claim in roots.iter('{http://www.epo.org/exchange}priority-claim'):
            #     pc_doc_number_epo = priority_claim[0][0].text
            #     pc_date_epo = priority_claim[0][1].text

            for applicants in roots.iter('{http://www.epo.org/exchange}applicants'):
                applicants_epo_list = []
                applicants_ol_list = []
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
            for inventors in roots.iter('{http://www.epo.org/exchange}inventors'):
                inventors_epo_list = []
                inventors_ol_list = []
                for inventor in inventors.iter('{http://www.epo.org/exchange}inventor'):
                    inventor_data_format = inventor.attrib.get('data-format')
                    if inventor_data_format.startswith('e'):
                        inventor_epo = inventor[0][0].text
                        inventors_epo_list.extend([inventor_epo])
                    elif inventor_data_format.startswith('o'):
                        inventor_epo = inventor[0][0].text
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
            for exchange_document in roots.iter('{http://www.epo.org/exchange}exchange-document'):
                invent_title_en_list = []
                invent_title_ol_list = []
                invent_title_else_list = []
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
            for exchange_document in roots.iter('{http://www.epo.org/exchange}exchange-document'):
                abstract_en_list = []
                abstract_ol_list = []
                abstract_else_list = []
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
            biblio_list = []
            xmlStr = response.text
            roots = ET.fromstring(xmlStr)
            pub_doc_number = country + str(doc_num) + kind

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
            my_a_set = list(set(citaion_a_list))
            count_citaion_a_list = str(len(my_a_set))

            citaion_e_list_join = '|'.join(my_e_set)
            count_citaion_e_list_join = '|'.join(count_citaion_e_list)

            citaion_a_list_join = '|'.join(my_a_set)
            count_citaion_a_list_join = '|'.join(count_citaion_a_list)


            biblio_all_list.append([
                                        app_doc_id
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
                                    ])
        # db 에 저장되어있는 publication 정보를 가져오는 코드
    try:
        conn = pymysql.connect(
            host=host_ip
            , user='root'
            , password='admin'
            , database='test'
            , port=3306
        )
        cur = conn.cursor()
        for row in biblio_all_list:
            print(row)
            cur = conn.cursor()
            sql_1 = "INSERT INTO biblio_citation_info_test VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
            val = row
            cur.execute(sql_1,val)
            conn.commit()
    except Exception as ex:
        print(ex,"DB insert 오류")
    finally:
        conn.close()
def claim_DB():
    client = epo_ops.Client(key='o2TgZqLMPnGxmiFk7rUUB0bTq9VZDbe1', secret='ZHYxcGxi9UprBTUD')

    yesterday = datetime.today() - timedelta(8)

    # date = yesterday.strftime("%Y%m%d")
    date = '20210324'

    host_ip = '34.68.250.64'

    # DB 에서 pubilcation 정보를 불러오는 코드.
    try:
        conn =pymysql.connect(host=host_ip
                              , user='root'
                              , password= 'admin'
                              , database='test'
                              , write_timeout= 3000)
        cur = conn.cursor()
        sql = "SELECT * FROM pub_num_data_{}".format(date)
        cur.execute(sql)
        conn.commit()
    except Exception as ex:
        print(ex,"DB 문제")
    finally:
        conn.close()
    datas = cur.fetchall()

    # 불러온 publication 정보를 for 반목문을 돌리기위해 list 에 넣는 코드.
    app_doc_num_list = []
    country_list = []
    kind_list = []
    simple_family_id = []
    for data in datas:

        family_id = data[1]
        pub_country = data[2]
        pub_number = data[3]
        pub_kind = data[4]

        app_doc_num_list.append(pub_number)
        country_list.append(pub_country)
        kind_list.append(pub_kind)
        simple_family_id.append(family_id)

    claims_all_list = []
    for doc_num,country,kind in zip(app_doc_num_list,country_list,kind_list):
        pub_num = country+str(doc_num)+kind
        try:
            response = client.published_data(
                    reference_type = 'publication'
                    , input = epo_ops.models.Docdb(str(doc_num),country,kind)
                    ,endpoint='claims'
            )
        except Exception as ex:
            print(ex,"EPO 연결 문제")
        else:
            xmlStr = response.text
            roots = ET.fromstring(xmlStr)

            claims_list = []
            for claims in roots.iter('{http://www.epo.org/fulltext}claim-text'):
                claims_text = claims.text
                claims_list.extend([claims_text])
            claims_all_list.extend([pub_num,claims_list])

    try:
        conn = pymysql.connect(
            host=host_ip
            , user='root'
            , password='admin'
            , database='test'
        )
        cur = conn.cursor()
        for row in claims_all_list:
            sql_1 = "INSERT INTO citation_info_test_1 VALUES(%s,%s,%s,%s,%s);"
            val = row
            cur.execute(sql_1, val)
            conn.commit()
    except Exception as ex:
        print(ex,"DB 연결 문제")
    finally:
        conn.close()
def New_familt_DB():
    host_ip = '34.68.250.64'
    yesterday = datetime.today() - timedelta(7)

    date = '20210324'
    # db 에 저장되어있는 publication 정보를 가져오는 코드
    try:
        conn = pymysql.connect(
            host=host_ip
            , user='root'
            , password='admin'
            , database='test'
        )
        cur = conn.cursor()
        sql = "SELECT * FROM pub_num_data_{} LIMIT 10".format(date)
        cur.execute(sql)
        conn.commit()
    except Exception as ex:
        print(ex,"DB select 문제")
    finally:
        conn.close()

    datas = cur.fetchall()

    app_doc_num_list = []
    country_list = []
    kind_list = []
    family_id_list = []
    for data in datas:
        family_id = data[1]
        pub_country = data[2]
        pub_number = data[3]
        pub_kind = data[4]

        app_doc_num_list.append(pub_number)
        country_list.append(pub_country)
        kind_list.append(pub_kind)
        family_id_list.append(family_id)


    client = epo_ops.Client(key='o2TgZqLMPnGxmiFk7rUUB0bTq9VZDbe1', secret='ZHYxcGxi9UprBTUD')
    family_all_list = []

    for simple_family_id_one, doc_num, country_list, kind_list in zip(family_id_list, app_doc_num_list, country_list,
                                                                      kind_list):
        try:
            response = client.family(
                reference_type='publication'
                , input=epo_ops.models.Docdb(str(doc_num), country_list, kind_list)
            )
        except Exception as ex:
            print(ex,"EPO 연결문제")
        else:
            xmlStr = response.text
            roots = ET.fromstring(xmlStr)

            # ----------------------------simple doc family-id--------------------------------
            # 처음 pub_info 를 가지고 올때 해당 family_id 도 같이 추출하여 simple_family_id 를 변수에 담는다 후에 inpadoc_family_id 를 골라 내기 위함이다.
            sim_fam_id = simple_family_id_one

            # ----------------------------원출원 번호 출력 --------------------------------

            doc_number = country_list + str(doc_num) + kind_list
            family_id_list = []
            simple_family_pub_number_list = []
            simple_family_pub_country_list = []
            inp_family_pub_number_list = []
            inp_family_pub_country_list = []
            # ----------------------------simple family 출력------------------------------
            #  root.iter 를 이용해서 root 이하의 자식 태그를 모두 불러온다. 그리고 원하는 태그들만 추출하여 text 로 변환해 변수에 저장한다.
            for family_member in roots.iter('{http://ops.epo.org}family-member'):
                family_member_id = family_member.attrib.get('family-id')
                family_member_id_set = list(set(family_member_id))
                family_id_list.extend([family_member_id_set])
                # simple_fam_id 과 inpadoc_fam_id 를 나누기위한 if 조건문.
                # 각각의 패밀리id 를 기존 simple_fam_id 와 비교하여 맞는것은 simple  아닌것은 inpadoc 으로 판단하고 각각 따로 변수에 담는다.
                if str(simple_family_id_one) == str(family_member_id):
                    pub_country = family_member[0][0][0].text
                    pub_doc_number = family_member[0][0][1].text
                    pub_kind = family_member[0][0][2].text
                    simple_family_pub_number_list.extend([
                        pub_country
                        + pub_doc_number
                        + pub_kind
                    ])
                    simple_family_pub_country_list.extend([pub_country])
                # ----------------------------inpadoc family 출력------------------------------
                else:
                    pub_country = family_member[0][0][0].text
                    pub_doc_number = family_member[0][0][1].text
                    pub_kind = family_member[0][0][2].text
                    inp_family_pub_number_list.extend([
                        pub_country
                        + pub_doc_number
                        + pub_kind
                    ])
                    inp_family_pub_country_list.extend([pub_country])

            count_simple_family_pub_number_list = len(simple_family_pub_number_list)
            count_simple_family_pub_country_list = len(set(simple_family_pub_country_list))
            simple_family_pub_country_set_list = list(set(simple_family_pub_country_list))
            count_inp_family_pub_number_list = len(inp_family_pub_number_list)
            count_inp_family_pub_country_list = len(set(inp_family_pub_country_list))
            inp_family_pub_country_set_list = list(set(inp_family_pub_country_list))

            if count_simple_family_pub_number_list == 0:
                count_simple_family_pub_number_list = 'null'

            if count_simple_family_pub_country_list == 0:
                count_simple_family_pub_country_list = 'null'

            if count_inp_family_pub_number_list == 0:
                count_inp_family_pub_number_list = 'null'

            if count_inp_family_pub_country_list == 0:
                count_inp_family_pub_country_list = 'null'
            else:
                pass

            # DB 에 적재 하려면 콤마구분자를 바꾸어줘야 함. 이유는 DB에서의 콤마명령어가 존재하기때문에 언어 명령이 꼬일수 있기때문.

            simple_family_pub_number_list_join = '|'.join(simple_family_pub_number_list)
            simple_family_pub_country_set_list_join = '|'.join(simple_family_pub_country_set_list)

            inp_family_pub_number_list_join = '|'.join(inp_family_pub_number_list)
            inp_family_pub_country_set_list_join = '|'.join(inp_family_pub_country_set_list)

            family_all_list.append([doc_number
                                       ,simple_family_pub_number_list_join
                                       ,count_simple_family_pub_number_list
                                       ,simple_family_pub_country_set_list_join
                                       ,count_simple_family_pub_country_list
                                       ,inp_family_pub_number_list_join
                                       ,count_inp_family_pub_number_list
                                       ,inp_family_pub_country_set_list_join
                                       ,count_inp_family_pub_country_list])
    try:
        conn = pymysql.connect(
            host=host_ip
            , user='root'
            , password='admin'
            , database='test'
        )
        cur = conn.cursor()
        for row in family_all_list:
            sql_1 = "INSERT INTO family_info_test_1 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);"
            val = row
            cur.execute(sql_1, val)
            conn.commit()
    except Exception as ex:
        print(ex,"DB insert 문제")
    finally:
        conn.close()


default_dag_args = {
    "owner": "airflow",
    "start_date": datetime.today()
}
dag = DAG(
    dag_id='all_patent'
    , default_args=default_dag_args
    , schedule_interval='0 1 * * *'
)

task1 = PythonOperator(
    task_id='pub_num_db'
    , python_callable=pub_num_db
    , dag=dag
)

task2 = PythonOperator(
    task_id='bibio_citaion_DB'
    , python_callable=bibio_citaion_DB
    , dag=dag
)
task3 = PythonOperator(
    task_id='New_familt_DB'
    , python_callable=New_familt_DB
    , dag=dag
)

task1 >> task2 >> task3 
