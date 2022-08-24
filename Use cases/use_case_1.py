from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession \
    .builder \
    .getOrCreate()

orders = spark.read.option("header", "true") \
                   .option("inferSchema", "True") \
                   .csv("../notebooks/ecommerce_data/olist_orders_dataset.csv")
customers = spark.read.option("header","true") \
                   .option("inferSchema", "True") \
                   .csv("../notebooks/ecommerce_data/olist_customers_dataset.csv")

orders = orders.withColumn("order_approved_at", F.to_timestamp(F.col("order_approved_at"))) \
               .withColumn("order_delivered_carrier_date", F.to_timestamp(F.col("order_delivered_carrier_date"))) \
               .withColumn("order_delivered_customer_date", F.to_timestamp(F.col("order_delivered_customer_date"))) \
               .withColumn("order_approved_at", F.to_timestamp(F.col("order_approved_at"))) \
               .withColumn("order_estimated_delivery_date", F.to_timestamp(F.col("order_estimated_delivery_date"))) 

orders_delivered = orders.where((F.col("order_delivered_customer_date").isNotNull())) \
                       .withColumn("delayed [h]", (F.col("order_delivered_customer_date").cast("bigint") - F.col("order_estimated_delivery_date").cast("bigint")) / 3600) \
                       .select("order_id", "customer_id", F.round(F.col("delayed [h]"),2).alias("delayed [h]"), 
                               F.month("order_delivered_customer_date").alias("month"),
                               F.year("order_delivered_customer_date").alias("year"))
number_of_delivered_orders = orders_delivered.count()

orders_delayed = orders_delivered.where(F.col("delayed [h]") > 0) \
                                 .select("order_id", "delayed [h]", "customer_id").orderBy(F.col("delayed [h]").desc())

orders_delivered_geo = orders_delivered.join(customers, ["customer_id"]) \
                                      .select("customer_unique_id","customer_id", "order_id", "delayed [h]","customer_state", "customer_city", "month", "year")
orders_delayed = orders_delivered_geo.where(F.col("delayed [h]") > 0) \
                                 .select("order_id", "delayed [h]", "customer_id", "customer_unique_id").orderBy(F.col("delayed [h]").desc())
customers_with_more_delays = orders_delayed.groupBy("customer_unique_id") \
                          .agg(F.count("customer_id").alias("number_of_delays")) \
                          .where(F.col("number_of_delays") > 1) \
                          .orderBy(F.col("number_of_delays").desc())
orders_delayed = orders_delayed.drop("customer_id","customer_unique_id")
#orders_delayed.show(5)
number_of_delayed_orders = orders_delayed.count()
states = spark.read.option("header", "true") \
                   .option("inferSchema", "True") \
                   .csv("../notebooks/ecommerce_data/states_name.csv")
orders_delivered_geo = orders_delivered_geo.join(states, ['customer_state']).drop('customer_state').withColumnRenamed("state_name", "customer_state")

avg_delay_by_state = orders_delivered_geo.where(F.col("delayed [h]") > 0) \
                    .groupBy("customer_state") \
                   .agg(F.round(F.mean("delayed [h]"),2).alias("average_delay_[h]"),
                        F.count("delayed [h]").alias("number_of_delays")) \
                   .orderBy(F.col("number_of_delays").desc())

avg_order_by_state = orders_delivered_geo.groupBy("customer_state") \
                   .agg(F.round(F.mean("delayed [h]"),2).alias("delivered_average_delay_[h]"),
                        F.count("delayed [h]").alias("number_of_orders"),
                        F.round(F.percentile_approx("delayed [h]" ,0.5),2).alias("median_delay_[h]")) \
                   .orderBy(F.col("number_of_orders").desc())

avg_delay_by_state_percent = avg_delay_by_state.join(avg_order_by_state, ['customer_state']) \
                   .withColumn("delays_%", F.round(F.col("number_of_delays") / F.col("number_of_orders")*100, 2))

import plotly.express as px
df_avg_delay_by_state_percent = avg_delay_by_state_percent.select("customer_state", "delays_%").toPandas()
fig = px.bar(df_avg_delay_by_state_percent, x='customer_state', y='delays_%'  )
fig.update_xaxes(title_text='State')
fig.update_yaxes(title_text='Percentage of delayed orders')
fig.update_layout(paper_bgcolor="rgb(245,245,245)")
percentage_delay_by_state = fig.to_html()

df_avg_delay_by_state_h = avg_delay_by_state_percent.select("customer_state", F.col("median_delay_[h]").alias("Median")) \
                                                    .toPandas()
fig = px.bar(df_avg_delay_by_state_h, x='customer_state', y="Median")

fig.update_xaxes(title_text='State')
fig.update_yaxes(title_text='Delay time [h]')
fig.update_layout(paper_bgcolor="rgb(245,245,245)", barmode='group',
                  legend=dict(yanchor="top", y=0.3, xanchor="left", x=0.01), legend_title_text='Type')
delay_rate_in_states = fig.to_html()

import pandas as pd
d = {"Number of orders": [number_of_delayed_orders, number_of_delivered_orders - number_of_delayed_orders], 
     "State": ["Delayed", "On time"]}
df = pd.DataFrame(data = d)
fig = px.pie(df, values = 'Number of orders', names='State')
fig.update_layout(paper_bgcolor="rgb(245,245,245)")
delay_rate = fig.to_html()

base_html = """
    <!doctype html>
    <html>
        <head >
            <meta http-equiv="Content-type" content="text/html; charset=utf-8">
            <script type="text/javascript" src="https://ajax.googleapis.com/ajax/libs/jquery/2.2.2/jquery.min.js"></script>
            <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.10.16/css/jquery.dataTables.css">
            <script type="text/javascript" src="https://cdn.datatables.net/1.10.16/js/jquery.dataTables.js"></script>
        </head>
        <body style='white-space:nowrap;'>%s
            <script type="text/javascript">$(document).ready(function(){$('table').DataTable({
                "pageLength": 10,
                "retrieve": true
            });});
            </script>
        </body>
    </html>
"""


df_customers_with_more_delays = base_html % customers_with_more_delays.withColumnRenamed("customer_unique_id", "Customer ID") \
                                                                      .withColumnRenamed("number_of_delays", "Number of delayed orders") \
                                                                      .toPandas().to_html()
df_orders_delayed = base_html % orders_delayed.withColumnRenamed("delayed [h]", "Delay time [h]") \
                                              .withColumnRenamed("order_id", "Order ID") \
                                              .toPandas().to_html()

html_string = '''
<!doctype html>
<html>
    <head>
        <meta charset="UTF-8">
        <title>Dashboard</title>
        <style>body{ margin:0 100; background:whitesmoke; }
              .row {display: flex;}
              .column {flex: 33.33%; padding: 5px;} 
              .center {display: block;  margin-left: auto;  margin-right: auto;}
        </style>
    </head>
    
    <body>
        <h1 style='text-align: center;'>Case 1</h1>      
        <!-- *** Section 1 *** --->
        
            <h2 style='text-align: center;'>Orders which were delayed</h2>
            ''' + df_orders_delayed + '''
               
               <h2 style='text-align: center;'>Delay rate of packages</h2>
            ''' + delay_rate + '''       
       
    
     <!-- *** Section 2 *** --->
    <h2 style='text-align: center;'>Customers whose packages were delayed more than once</h2>
        ''' + df_customers_with_more_delays + '''
        <!-- *** Section 3 *** --->
        <h2 style='text-align: center;'>Median delay by state</h2>
        ''' + delay_rate_in_states + '''
        <p style='text-align: center;'>The value below 0 means, that orders were delivered before estimated delivery date.</p>
        <!-- *** Section 4 *** --->
    <h2 style='text-align: center;'>Propotion of delayed orders by states</h2>
            ''' + percentage_delay_by_state + ''' 
            
    </body>
</html>'''

with open('use_case_1.html', 'w', encoding = 'utf8') as f:
    f.write(html_string)