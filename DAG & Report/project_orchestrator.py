#!/usr/bin/env python
# coding: utf-8

# In[1]:


from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os

def merge_html(path):
    empty_html = '<html><head></head><body></body></html>'
    ls_files = os.listdir(path)
    ls_files.sort()
    for report in ls_files:
        if report.endswith('.html'):
            with open(path+report, 'r') as f:
                html = f.read()
                empty_html = empty_html.replace('</body></html>', html + '</body></html>')
    
    with open("/usr/local/spark/resources/data/business_report.html",'w') as f_1:
        f_1.write(empty_html)


spark_master = "spark://spark:7077"
spark_app_name = "Spark MapReducers - Project"

orders_file = "/usr/local/spark/resources/data/commerce/olist_orders_dataset.csv"
order_payments_file = "/usr/local/spark/resources/data/commerce/olist_order_payments_dataset.csv"
customers_file = "/usr/local/spark/resources/data/commerce/olist_customers_dataset.csv"
product_category_name_transaltion_file = "/usr/local/spark/resources/data/commerce/product_category_name_translation.csv"
order_items_file = "/usr/local/spark/resources/data/commerce/olist_order_items_dataset.csv"
products_file = "/usr/local/spark/resources/data/commerce/olist_products_dataset.csv"
sellers_file = "/usr/local/spark/resources/data/commerce/olist_sellers_dataset.csv"
states_file = "/usr/local/spark/resources/data/commerce/states_name.csv"
regions_file = "/usr/local/spark/resources/data/commerce/region_names.csv"




now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="Buisness_Analysis_DAG", 
        description="This DAG runs four Spark jobs corresponding the analyzed cases and then creates HTML raport for buisness purposes",
        default_args=default_args, 
        schedule_interval= '0 12 1 1 *'
)


task_max_spending = SparkSubmitOperator(
    task_id="find_maximum_spending_customers",
    application="/usr/local/spark/app/Project/use_case_3.py", # Spark application path created in airflow and spark cluster
    name=spark_app_name,
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    application_args=[orders_file,order_payments_file,customers_file,states_file],
    dag=dag
)

task_top3_products_category = SparkSubmitOperator(
    task_id="find_top3_products_for_each_category",
    application="/usr/local/spark/app/Project/use_case_2.py", # Spark application path created in airflow and spark cluster
    name=spark_app_name,
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    application_args=[order_items_file,products_file,product_category_name_transaltion_file,orders_file],
    dag=dag
)

task_deleyed_orders = SparkSubmitOperator(
    task_id="find_deleyed_orders",
    application="/usr/local/spark/app/Project/use_case_1.py", # Spark application path created in airflow and spark cluster
    name=spark_app_name,
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    application_args=[orders_file,customers_file,states_file],
    dag=dag
)

task_top2_highest_earining_sellers = SparkSubmitOperator(
    task_id="find_top2_highest_earning_sellers",
    application="/usr/local/spark/app/Project/use_case_4.py", # Spark application path created in airflow and spark cluster
    name=spark_app_name,
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    application_args=[orders_file,order_items_file,sellers_file,regions_file],
    dag=dag
)


task_html_raport = PythonOperator(
    task_id="create_html_raport",
    python_callable = merge_html,
    op_kwargs = {'path':"/usr/local/spark/resources/data/html_reports/"},
    dag=dag
)

# use_case_2_empty = DummyOperator(task_id="case_2_idiot", dag=dag)
# use_case_1_empty = DummyOperator(task_id="case_1_idiot", dag=dag)
# use_case_4_empty = DummyOperator(task_id="case_4_idiot", dag=dag)
# use_case_3_empty = DummyOperator(task_id="case_3_idiot", dag=dag)



[task_deleyed_orders, task_top3_products_category, task_max_spending, task_top2_highest_earining_sellers] >> task_html_raport 

