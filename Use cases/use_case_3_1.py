from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pandas as pd
import calendar
from pyspark.sql.functions import *
import plotly.express as px
import plotly.graph_objects as go
from IPython.display import display, HTML
import json
from urllib.request import urlopen
from IPython.display import display, HTML

with urlopen('https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson') as response:
 Brazil = json.load(response) # Javascrip object notation 
 
state_id_map = {}
for feature in Brazil ['features']:
 feature['id'] = feature['properties']['name']
 state_id_map[feature['properties']['sigla']] = feature['id']
 
spark = SparkSession \
    .builder \
    .appName("ecommerce project") \
    .getOrCreate()
	
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

customers = spark.read.option("header","true") \
                      .option("inferSchema", "true") \
                      .csv("../notebooks/ecommerce_data/olist_customers_dataset.csv")

states = spark.read.option("header","true") \
                    .option("inferSchema", "true") \
                    .csv("../notebooks/ecommerce_data/states_name.csv")
					
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
use_case_3_1_plot = fig.to_html()

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
            <h1 style='text-align: center;'>Case 3_1</h1> 
            <h2 style='text-align: center;'>Map of most popular categories per state</h2>
            ''' + use_case_3_1_plot + '''
    </body>
</html>'''

with open('use_case_3_1_report.html', 'w', encoding = 'utf8') as f:
    f.write(html_string)