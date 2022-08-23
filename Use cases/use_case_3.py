from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pandas as pd
import calendar
from pyspark.sql.functions import *
import plotly.express as px
import plotly.graph_objects as go
from IPython.display import display, HTML

spark = SparkSession \
    .builder \
    .appName("ecommerce project") \
    .getOrCreate()
	
#Read data
orders = spark.read.option("header","true") \
                    .option("inferSchema", "true") \
                    .csv("../notebooks/ecommerce_data/olist_orders_dataset.csv")

customers = spark.read.option("header","true") \
                      .option("inferSchema", "true") \
                      .csv("../notebooks/ecommerce_data/olist_customers_dataset.csv")

order_payments = spark.read.option("header","true") \
                    .option("inferSchema", "true") \
                    .csv("../notebooks/ecommerce_data/olist_order_payments_dataset.csv")

states = spark.read.option("header","true") \
                    .option("inferSchema", "true") \
                    .csv("../notebooks/ecommerce_data/states_name.csv")
					
order_payment_join = orders.join(order_payments).where(orders['order_id'] == order_payments['order_id']) \
                   .drop(order_payments['order_id'])
   
order_payment_join = order_payment_join.withColumn('order_purchase_timestamp', to_timestamp(col('order_purchase_timestamp'))) \
                        .withColumn('order_approved_at', to_timestamp(col('order_approved_at'))) \
                        .withColumn('order_delivered_carrier_date', to_timestamp(col('order_delivered_carrier_date'))) \
                        .withColumn('order_delivered_customer_date', to_timestamp(col('order_delivered_customer_date'))) \
                        .withColumn('order_estimated_delivery_date', to_timestamp(col('order_estimated_delivery_date')))

order_payment_join = order_payment_join.withColumn('month', month(col('order_estimated_delivery_date'))) \
                                       .withColumn('year', year(col('order_estimated_delivery_date')))
									   
years = order_payment_join.agg(max(year(order_payment_join.order_estimated_delivery_date)).alias('year'))

date = year(current_date()) - years.collect()[0][0]

order_payment_join = order_payment_join \
.where((col('order_status') == 'delivered')) \
.select('customer_id', 'payment_value', 'month', 'year')

windowSpec = Window.partitionBy('month', 'year') \
                    .orderBy(col('full_cost_sum').desc())

order_payment_join = order_payment_join \
               .groupBy('customer_id', 'month', 'year').agg(sum('payment_value').alias('full_cost_sum')) \
               .withColumn('rank', dense_rank().over(windowSpec)) \
               .where(col('rank') == 1) \
               .orderBy('month')
			   
order_payment_join = order_payment_join \
                               .withColumn('full_cost_sum', round('full_cost_sum',2))
   
order_payment_join = order_payment_join.join(customers, ['customer_id'], 'inner')

order_payment_join = order_payment_join.join(states, ['customer_state'], 'inner')

order_payment_join_previous = order_payment_join \
                             .select('customer_unique_id', 'month', 'full_cost_sum', 'state_name') \
                             .where(col('year') == (year(current_date())-date)) \
                             .orderBy('month')

order_payment_join_all = order_payment_join \
                             .select('customer_unique_id', 'month', 'full_cost_sum', 'state_name', 'year').orderBy('month')
							 
order_payment_join_previous = order_payment_join_previous.toPandas()

order_payment_join_previous.sort_values(by=['month'])

order_payment_join_previous['month'] = order_payment_join_previous['month'].apply(lambda x: calendar.month_abbr[x])

arr = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
	  
fig = px.bar(order_payment_join_previous, x='month', y='full_cost_sum', color = 'state_name' ,\
             hover_name  = 'customer_unique_id')
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.update_layout(coloraxis={"colorbar":{"dtick":1}})
fig.update_xaxes(type='category')
fig.update_xaxes(categoryorder='array', categoryarray=arr)
fig.update_xaxes(title = 'Month')
fig.update_yaxes(title = 'Money spent')
fig.update_layout(legend_title = 'State')
fig.show()
# html file
#fig.write_html('use_case_3_plot.html')
use_case_3_plot = fig.to_html()

order_payment_join_all = order_payment_join_all.toPandas()

order_payment_join_all.sort_values(by=['month'])

order_payment_join_all['month'] = order_payment_join_all['month'].apply(lambda x: calendar.month_abbr[x])

order_payment_join_all['full_cost_sum'] = order_payment_join_all['full_cost_sum'].round(decimals = 3)

df = order_payment_join_all

fig = go.Figure(go.Table(header={"values": df.columns}, cells={"values": df.T.values}))
fig.update_layout(
    updatemenus=[
        {
            "x": 0.2 - (i / 8),
            "y": 1.15,
            "buttons": [
                {
                    "label": c,
                    "method": "update",
                    "args": [
                        {
                            "cells": {
                                "values": df.T.values
                                if c == "All"
                                else df.loc[df[menu].eq(c)].T.values
                            }
                        }
                    ],
                }
                for c in ["All"] + df[menu].sort_values().unique().tolist()
            ],
        }
        for i, menu in enumerate(["year", "state_name"])
    ]
)
fig.data[0]['columnwidth'] = [30, 10];fig.update_layout(autosize=True)
#fig.update_layout(width=900, height=500)
fig.show()
#fig.write_html('table_use_case_3.html')
use_case_3_table = fig.to_html()

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
            <h1 style='text-align: center;'>Case 3</h1> 
            <h2 style='text-align: center;'>Maximum spending customers with states for every month of last year</h2>
            ''' + use_case_3_plot + ''' 
            <h2 style='text-align: center;'>Maximum spending customers by state and year together with months</h2>
            ''' + use_case_3_table + ''' 
    </body>
</html>'''

with open('use_case_3_report.html', 'w', encoding = 'utf8') as f:
    f.write(html_string)