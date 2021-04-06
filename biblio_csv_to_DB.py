import xml.etree.ElementTree as ET
import epo_ops
import csv
import time
from datetime import datetime, timedelta
import pandas as pd
import pymysql
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


def biblio_csv():
    yesterday = datetime.today() - timedelta(9)
    date = yesterday.strftime("%Y%m%d")
    # DB 에서 pubilcation 정보를 불러오는 코드.
    conn =pymysql.connect(host='172.19.240.1', user='root', password= 'admin', database='test')
    cur = conn.cursor()
    sql = "SELECT * FROM pub_num_data_{}".format(date)
    cur.execute(sql)
    conn.commit()
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

    # load_csv = pd.read_csv('./date_pub_num20190617.csv').head(20)
    #
    # country_list = load_csv['pub_country']
    # app_doc_num_list = load_csv['pub_doc_number']
    # kind_list = load_csv['pub_kind_code']

    client = epo_ops.Client(key='o2TgZqLMPnGxmiFk7rUUB0bTq9VZDbe1', secret='ZHYxcGxi9UprBTUD')
    biblio_all_list = []
    for doc_num,country,kind in zip(app_doc_num_list,country_list,kind_list):
        try:
            response = client.published_data(
                reference_type='publication'
                ,input=epo_ops.models.Docdb(str(doc_num),country,kind)
                ,endpoint='biblio'
            )
        except Exception as ex:
            print(ex)
        else:
            biblio_list = []
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


            for priority_claim in roots.iter('{http://www.epo.org/exchange}priority-claim'):
                pc_doc_number_epo = priority_claim[0][0].text
                pc_date_epo = priority_claim[0][1].text



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
                if len(applicants_epo_list) == 0 :
                    applicants_epo_list.append('None')
                if len(applicants_ol_list) == 0 :
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
                if len(inventors_epo_list) == 0 :
                    inventors_epo_list.append('None')
                if len(inventors_ol_list) == 0 :
                    inventors_ol_list.append('None')
                else:
                    pass
                inventors_epo_list_join = '|'.join(inventors_epo_list)
                inventors_ol_list_join = '|'.join(inventors_ol_list)


            # ---------------------invent 정보----------------------------
            for exchange_document in roots.iter('{http://www.epo.org/exchange}exchange-document'):
                invent_title_en_list =[]
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
                if len(invent_title_en_list) == 0 :
                    invent_title_en_list.append('None')
                if len(invent_title_ol_list) == 0 :
                    invent_title_ol_list.append('None')
                if len(invent_title_else_list) == 0 :
                    invent_title_else_list.append('None')
                else:
                    pass
                invent_title_en_list_join ='|'.join(invent_title_en_list)
                invent_title_ol_list_join = '|'.join(invent_title_ol_list)
                invent_title_else_list_join = '|'.join(invent_title_else_list)

            # ---------------------abstract 정보----------------------------
            for exchange_document in roots.iter('{http://www.epo.org/exchange}exchange-document'):
                abstract_en_list =[]
                abstract_ol_list =[]
                abstract_else_list =[]
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
                if len(abstract_en_list) == 0 :
                    abstract_en_list.append('None')
                if len(abstract_ol_list) == 0 :
                    abstract_ol_list.append('None')
                if len(abstract_else_list) == 0 :
                    abstract_else_list.append('None')
                else:
                    pass
                abstract_en_list_join ='|'.join(abstract_en_list)
                abstract_ol_list_join = '|'.join(abstract_ol_list)
                abstract_else_list_join = '|'.join(abstract_else_list)
                print(pub_country_docdb+str(pub_doc_number_docdb),pub_kind_docdb)
                biblio_list.extend([app_doc_id
                                       , app_country_docdb
                                       , app_doc_number_docdb
                                       , app_kind_docdb
                                       , app_doc_number_epo
                                       , app_date_epo

                                       ,pub_country_docdb
                                       , pub_doc_number_docdb
                                       , pub_kind_docdb
                                       , pub_date_docdb
                                       , pub_doc_number_epo
                                       , pub_date_epo

                                       , ipcr_join
                                       , cpci_join

                                       , pc_doc_number_epo
                                       , pc_date_epo

                                       , applicants_epo_list_join
                                       , applicants_ol_list_join

                                       , inventors_epo_list_join
                                       , inventors_ol_list_join

                                       ,invent_title_en_list_join
                                       ,invent_title_ol_list_join
                                       ,invent_title_else_list_join

                                       ,abstract_en_list_join
                                       ,abstract_ol_list_join
                                       ,abstract_else_list_join
                                        ,family_id
                                      ])
            biblio_all_list.extend([biblio_list])
        Refilename = './mnt/C/User/ehd5538/CSV_test/biblio_csv_test_{}.csv'.format(date)
        f = open(Refilename, 'w', encoding='utf-8', newline='')
        csvWriter = csv.writer(f)
        csvWriter.writerow([ 'app_doc_id'
                            ,'app_country_docdb'
                            ,'app_doc_number_docdb'
                            ,'app_kind_docdb'
                            ,'app_doc_number_epo'
                            ,'app_date_epo'

                            ,'pub_country_docdb'
                            ,'pub_doc_number_docdb'
                            ,'pub_kind_docdb'
                            ,'pub_date_docdb'
                            ,'pub_doc_number_epo'
                            ,'pub_date_epo'

                            ,'ipcr_list'
                            ,'cpci_list'

                            ,'pc_doc_number_epo'
                            ,'pc_date_epo'

                            ,'applicants_epo_list'
                            ,'applicants_ol_list'
                            ,'inventors_epo_list'
                            ,'inventors_ol_list'

                            ,'invent_title_en_list'
                            ,'invent_title_ol_list'
                            ,'invent_title_else_list'

                            ,'abstract_en_list'
                            ,'abstract_ol_list'
                            ,'abstract_else_list'
                             ,'family_id'
                              ])
        for w in biblio_all_list:
            csvWriter.writerow(w)
        f.close()
        print('완료')

def biblio_csv_to_DB():
    yesterday = datetime.today() - timedelta(9)
    date = yesterday.strftime("%Y%m%d")

    data = pd.read_csv('./mnt/C/User/ehd5538/CSV_test/biblio_csv_test_{}.csv'.format(date))
    df = pd.DataFrame(data)

    conn = pymysql.connect(
        host='172.19.240.1'
        , user='root'
        , password='admin'
        , database='test'
    )
    cur = conn.cursor()
    for row in df.itertuples():
        sql = "INSERT INTO biblio_info_test1 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        val = row.app_doc_id, row.app_country_docdb, row.app_doc_number_docdb, row.app_kind_docdb, row.app_doc_number_epo, row.app_date_epo, row.pub_country_docdb, row.pub_doc_number_docdb, row.pub_kind_docdb, row.pub_date_docdb, \
              row.pub_doc_number_epo, row.pub_date_epo, row.ipcr_join, row.cpci_join, row.pc_doc_number_epo, row.pc_date_epo, row.applicants_epo_list_join, row.applicants_ol_list_join, row.inventors_epo_list_join, row.inventors_ol_list_join, \
              row.invent_title_en_list_join, row.invent_title_ol_list_join, row.invent_title_else_list_join, row.abstract_en_list_join, row.abstract_ol_list_join, row.abstract_else_list_join, row.family_id
        cur.execute(sql, val)
        conn.commit()

    conn.close()

default_dag_args = {
    "owner": "airflow",
    "start_date": datetime.today()
}
dag = DAG(
    dag_id='biblio_csv_db'
    , default_args=default_dag_args
    , schedule_interval='0 9 * * *'
    # , schedule_interval=timedelta(1)
)
task1 = PythonOperator(
    task_id='biblio_csv'
    , python_callable=biblio_csv
    , dag=dag
)
task2 = PythonOperator(
    task_id='biblio_insert_Db'
    , python_callable=biblio_csv_to_DB
    , dag=dag
)

task1 >> task2