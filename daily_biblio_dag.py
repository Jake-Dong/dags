import requests
import base64
import json
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import pandas
import pymysql
import time
import pendulum
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

def bibio_citaion_DB():
    start = time.time()
    # ====================================EPO 토큰 발행
    def make_token():
        token_url='https://ops.epo.org/3.2/auth/accesstoken'

        key = 'Basic %s' % base64.b64encode(b'o2TgZqLMPnGxmiFk7rUUB0bTq9VZDbe1:ZHYxcGxi9UprBTUD').decode('ascii')
        data = {'grant_type': 'client_credentials'}
        headers = {'Authorization': key, 'Content-Type': 'application/x-www-form-urlencoded'}

        r = requests.post(token_url, data=data, headers=headers)

        rs= r.content.decode()
        response = json.loads(rs)

        token = response['access_token']
        return token
    #=====================================tor 프록시 설정.
    proxies = {'http': 'socks5://127.0.0.1:9050',
               'https': 'socks5://127.0.0.1:9050'}
    # ====================================DB 접속 및 publication-number 데이터 가져오기

    host_ip = '34.68.250.64'  # 내가 접속하려는 DB서버의 ip
    user_id = 'root'  # DB 서버에서 외부 접근 권한을 부여한 user id 본인은 root 계정으로 이름을 만들고 0.0.0.0 (모든 아이피 접근 권한) 을주었다.
    user_password = 'admin'  # 외부 권한 접근시(user_id : root) 필요한 password.
    port_number = 3306  # 외부 ip 가 접근할때 열어주는 포트번호 mariaDB 는 기본적으로 3306 포트를 사용한다.
    DB_database = 'test'  # mariaDB 에서 작업할 database 이름.

    #dt_index = pandas.date_range(start='20210223', end='20210223')
    # yesterday = datetime.today() - timedelta(7) # 매일 매일 어제의 데이터(태이블)를 가지고 와야 할때
    # pandas.date_range(start='20200901', end='20201031',freq='W-MON') 을 하면 해당 기간 매주 월요일들만 추출합니다.
    # type(dt_index) => DatetimeIndex
    # DatetimeIndex => list(str)
    #dt_list = dt_index.strftime("%Y%m%d").tolist()
    yesterday = datetime.today() - timedelta(7)
    dt_list = [yesterday.strftime("%Y%m%d")]

    for date in dt_list:
        # db 에 저장되어있는 publication 정보를 가져오는 코드
        try:
            conn = pymysql.connect(
                host=host_ip
                , user=user_id
                , password=user_password
                , database=DB_database
                , port = port_number
                , connect_timeout = 3000
            )
            cur = conn.cursor()
            sql = "SELECT DISTINCT * FROM pub_num_data_{}".format(date)
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
        for data in datas:
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
        for i in range(0, total_count+100 , 99):
            docdb_pub_num_join = ','.join(docdb_pub_num_list[i:i+99])
            service = 'published-data'
            text_type = 'biblio'
            DB_type = 'docdb'
            # 가지고온 정보를 for문을 이용해 number 변수에 넣는다.
            header = {'Authorization': "Bearer " + str(make_token())} # 헤더에 위에서 만든 토큰을 포함시켜 requests 요청을 보낼때 토큰정보가 전송되도록 해준다.
            myUrl = 'http://ops.epo.org/3.2/rest-services/'+service+'/publication/'+DB_type+'/'+str(docdb_pub_num_join)+'/'+text_type
            response = requests.get(myUrl, headers=header,proxies=proxies)
            xmlStr = response.text
            root = ET.fromstring(xmlStr)

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
                    print(pub_doc_number_epo)
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

                for priority_claims in roots.iter('{http://www.epo.org/exchange}priority-claims'):
                    pc_app_doc_num_list = []
                    pc_app_date_list = []
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
            
            sql_1 = "INSERT INTO biblio_info VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
            val = tuple(biblio_all_list)
            cur.executemany(sql_1,val)
            conn.commit()
        except Exception as ex:
            print(ex,"DB insert 오류")
        finally:
            conn.close()

    print(time.time()-start)

local_tz = pendulum.timezone('Asia/Seoul')
today = datetime.today()

default_dag_args = {
    "owner": "airflow",
    "start_date": datetime(today.year, today.month, today.day, tzinfo=local_tz) - timedelta(hours=25)
}
dag = DAG(
    dag_id='daily_bibio_citaion_DB'
    , default_args=default_dag_args
    , schedule_interval='0 5 * * *'
    # , schedule_interval=timedelta(1)
)
task1 = PythonOperator(
    task_id='bibio_citaion_DB'
    , python_callable=bibio_citaion_DB
    , dag=dag
)
