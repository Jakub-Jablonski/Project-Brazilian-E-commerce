# Create Spark Context
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pandas as pd
import plotly.express as px
from pyspark.sql.window import Window
import plotly.graph_objects as go
import calendar
from IPython.display import display, HTML

spark = SparkSession \
    .builder \
    .appName("ecommerce project") \
    .getOrCreate()
	
#Read data
order_items = spark.read.option("header","true") \
                    .option("inferSchema", "true") \
                    .csv("../notebooks/ecommerce_data/olist_order_items_dataset.csv")

orders = spark.read.option("header","true") \
                    .option("inferSchema", "true") \
                    .csv("../notebooks/ecommerce_data/olist_orders_dataset.csv")

products = spark.read.option("header","true") \
                     .option("inferSchema", "true") \
                     .csv("../notebooks/ecommerce_data/olist_products_dataset.csv")

products_en = spark.read.option("header","true") \
                    .option("inferSchema", "true") \
                    .csv("../notebooks/ecommerce_data/product_category_name_translation.csv")
					
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
            <h2 style='text-align: center;'>Top 3 selling categories with months</h2>
            ''' + use_case_2_table + ''' 
    </body>
</html>'''

with open('use_case_2_report.html', 'w', encoding = 'utf8') as f:
    f.write(html_string)