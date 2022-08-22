import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import plotly.express as px
import plotly
import pandas as pd

spark = (SparkSession.builder.getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("WARN")

orders_file = sys.argv[1]
order_items_file = sys.argv[2]
sellers_file = sys.argv[3]
region_file = sys.argv[4]


orders = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema",True)
    .load(orders_file)
)

order_items = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema",True)
    .load(order_items_file)
)

sellers = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema",True)
    .load(sellers_file)
)

region_names = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema",True)
    .load(region_file)
)


### PLACE FOR UR TRANSFORMATION ###

#prepare data
order_items_useful = order_items.select('order_id','seller_id','price')
sellers_usefull = sellers.select('seller_id','seller_city','seller_state')
delivered_ids = orders.select('order_id').where(F.col('order_status') == 'delivered')

#     VVVV
delivered_orders = order_items_useful.join(delivered_ids, ['order_id'],'inner')
task_4 = delivered_orders.join(F.broadcast(sellers_usefull),['seller_id'],'inner')

# make answer
window1  = Window.partitionBy("seller_state").orderBy(F.col("earnings").desc())
window2  = Window.partitionBy("seller_state")

ans = task_4.groupBy('seller_state','seller_city','seller_id') \
      .agg(F.round(F.sum(F.col('price')),2).alias('earnings')) \
      .withColumn('rank',F.rank().over(window1)) \
      .withColumn('all_sales_in_region',F.sum('earnings').over(window2)) \
      .withColumn('sum_earnings',F.sum('earnings').over(window1)) \
      .where(F.col('rank') < 3)
      
ans = ans.withColumn('percent', F.concat(F.round(100*F.col('earnings') / F.col('all_sales_in_region'),2), F.lit('%')))
ans = ans.join(region_names,['seller_state'],'inner').orderBy('sum_earnings')

#print figure
ans_pd = ans.toPandas()
ans_pd['rank'] = ans_pd['rank'].astype('str')
fig = px.bar(ans_pd, x='name', y='earnings', color = 'rank', text='percent' \
             , labels=dict(name="State", earnings="Seller's Revenue ($)", rank="Seller's Rank") \
             , hover_name  = 'seller_id', hover_data=["earnings",'seller_city'])
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.update_layout(coloraxis={"colorbar":{"dtick":1}})
fig.update_xaxes(type='category')
#fig.show()


# html file
html_plot = fig.to_html()
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
            <h1 style='text-align: center;'>Case 4</h1> 
            <h2 style='text-align: center;'>Top 2 Highest earning sellers by each location. </h2>
            ''' + html_plot + ''' 
    </body>
</html>'''

with open('/usr/local/spark/resources/data/html_reports/4c_report.html', 'w', encoding = 'utf8') as f:
    f.write(html_string)
    