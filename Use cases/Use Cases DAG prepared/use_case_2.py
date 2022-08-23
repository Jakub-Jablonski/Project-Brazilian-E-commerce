import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pandas as pd
import calendar
import pyspark.sql.functions as F
from pyspark.sql.functions import *
import plotly.express as px
import plotly.graph_objects as go
from IPython.display import display, HTML
import json
from urllib.request import urlopen

spark = (SparkSession.builder.getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("WARN")

order_items_file = sys.argv[1]
products_file = sys.argv[2]
products_en_file = sys.argv[3]
orders_file = sys.argv[4]
customers_file = sys.argv[5]
states_file = sys.argv[6]
brazil_geo_file = sys.argv[7]

order_items = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema",True)
    .load(order_items_file)
)

products = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema",True)
    .load(products_file)
)

products_en = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema",True)
    .load(products_en_file)
)

orders = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema",True)
    .load(orders_file)
)

customers = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema",True)
    .load(customers_file)
)

states = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema",True)
    .load(states_file)
)

### PLACE FOR UR TRANSFORMATION ###

with open(brazil_geo_file) as brazil_geo:
 Brazil = json.load(brazil_geo) # Javascrip object notation 
 
state_id_map = {}
for feature in Brazil ['features']:
 feature['id'] = feature['properties']['name']
 state_id_map[feature['properties']['sigla']] = feature['id']

prod_ord = order_items.join(products, ['product_id'], 'inner')

prod_ord = prod_ord.join(orders, ['order_id'], 'inner')

prod_ord = prod_ord.withColumn('month', F.month(F.col('order_estimated_delivery_date'))) \
                   .withColumn('year', F.year(F.col('order_estimated_delivery_date')))
				   
prod_ord = prod_ord.select('product_category_name', 'product_id', 'price', 'year', 'month') \
                   .where(F.col('order_status') == 'delivered')
				   
windowSpec = Window.partitionBy('product_category_name').orderBy(F.col('cnt').desc())

products_count = prod_ord.groupBy('product_category_name', 'product_id') \
                                .agg(F.count('product_category_name').alias('cnt'), \
                                F.round(F.sum('price'),2).alias('revenue')) \
                         .withColumn('rank', F.rank().over(windowSpec)) \
                         .select(F.col('product_category_name'), F.col('cnt'), 'product_id', 'rank', 'revenue') \
                         .distinct() \
                         .where(F.col('rank') < 4) \
                         .orderBy(F.col('product_category_name').desc(), F.col('rank'))
						 
products_count_en = products_count.join(products_en, ['product_category_name'], 'inner')

products_count_en = products_count_en \
                .select(F.col('product_category_name_english'), F.col('cnt').alias('number_of_sales'), 'product_id', 'rank', \
                       'revenue')
					   
products_count_en_df = products_count_en.toPandas()

products_count_en_df['rank'] = products_count_en_df['rank'].astype(str)

fig = px.bar(products_count_en_df, x='product_category_name_english', y='number_of_sales', color = 'rank',\
             hover_name  = 'product_id', hover_data=["product_id", "revenue"])
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.update_layout(coloraxis={"colorbar":{"dtick":1}})
fig.update_xaxes(type='category')
fig.update_xaxes(title = 'Name of the product category')
fig.update_yaxes(title = 'Number of units sold')
fig.update_layout(legend_title = 'Rank')
fig.show()
# html file
#fig.write_html('use_case_2_plot.html')
use_case_2_plot = fig.to_html()

products_count = prod_ord.groupBy('product_category_name', 'month') \
                                .agg(F.count('product_category_name').alias('cnt'), \
                                F.round(F.sum('price'),2).alias('revenue')) \
                         .withColumn('rank', F.rank().over(windowSpec)) \
                         .select(F.col('product_category_name'), F.col('cnt'),  'revenue', 'month') \
                         .distinct() \
                         .where(F.col('rank') < 4) \
                         .orderBy(F.col('product_category_name').desc())
						 
products_count_en = products_count.join(products_en, ['product_category_name'], 'inner')

products_count_en = products_count_en \
                .select(F.col('product_category_name_english').alias('category_name'), F.col('cnt').alias('number_of_sales'), \
                            'revenue', 'month').orderBy('month')
							
products_count_en_df = products_count_en.toPandas()

products_count_en_df['month'] = products_count_en_df['month'].apply(lambda x: calendar.month_abbr[x])

df = products_count_en_df

fig = go.Figure(go.Table(header={"values": df.columns}, cells={"values": df.T.values}))
fig.update_layout(
    updatemenus=[
        {
            "y": 1.15,
            "x":0.36,
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
                for c in ["All"] + df[menu].unique().tolist()
            ],
        }
        for i, menu in enumerate(["category_name", "month"])
    ]
)
#fig.data[0]['columnwidth'] = [30, 20];fig.update_layout(autosize=False)
layout = dict(autosize=True)
fig.update_layout(autosize=True)
#fig.update_layout(width=980, height=500)
fig.show()
#fig.write_html('table_use_case_2.html')
use_case_2_table = fig.to_html()

# ADDED MAP
popular_prod_map = customers.join(orders, ['customer_id'], 'inner') \
                                   .drop('order_estimated_delivery_date', 'order_delivered_customer_date', \
                                        'order_delivered_carrier_date', 'order_approved_at', 'order_purchase_timestamp') \
                                   .select('*').where(col('order_status') == 'delivered')
								   
popular_prod_map = popular_prod_map.join(states, ['customer_state'], 'inner')

popular_prod_map = popular_prod_map.join(order_items, ['order_id'], 'inner') \
                                   .drop('seller_id', 'shipping_limit_date', 'price', 'freight_value')
								   
popular_prod_map = popular_prod_map.join(products, ['product_id'], 'inner')
popular_prod_map = popular_prod_map.join(products_en, ['product_category_name'], 'inner')

popular_prod_map = popular_prod_map.drop('product_category_name', 'product_name_lenght', 'product_description_lenght', \
                                        'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm',
                                        'product_width_cm')
										
popular_prod_map.count()

windowSpec = Window.partitionBy('state_name').orderBy(col('amount_of_orders').desc())

popular_prod_map = popular_prod_map.groupBy('state_name', 'product_category_name_english') \
                                    .agg(count('product_category_name_english').alias('amount_of_orders')) \
                                    .withColumn('rank', rank().over(windowSpec)) \
                                    .select('state_name', 'product_category_name_english', 'amount_of_orders', 'rank') \
                                    .where(col('rank') == 1)
                                    
df = popular_prod_map.toPandas()

fig = px.choropleth(
 df,
 locations = 'state_name',
 geojson = Brazil,
 color = 'product_category_name_english',
 hover_name = 'product_category_name_english',
 hover_data =["state_name","amount_of_orders"],
)
fig.update_geos(fitbounds = "locations", visible = False)
fig.update_layout(legend_title = 'Product category names')
#fig.show()
use_case_2_map = fig.to_html()


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
            <h1 style='text-align: center;'>Case 2</h1> 
            <h2 style='text-align: center;'>Top 3 selling products of each category</h2>
            ''' + use_case_2_plot + '''
            <h2 style='text-align: center;'>Top 3 best selling months for each category</h2>
            ''' + use_case_2_table + '''
            <h2 style='text-align: center;'>Map of most popular categories per state</h2>
            ''' + use_case_2_map + '''
    </body>
</html>'''


with open('/usr/local/spark/resources/data/html_reports/2c_report.html', 'w', encoding = 'utf8') as f:
    f.write(html_string)