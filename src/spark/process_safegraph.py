from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, udf, collect_list, explode, to_json, desc, rank, asc, size, monotonically_increasing_id
from pyspark.sql.types import MapType, StringType, IntegerType, ArrayType
from collections import Counter
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("safegraph").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


#process safegraph poi data

#load poi data from S3
df_poi = spark.read.option("header",True)\
        .option("escape", '"')\
        .csv("s3a://sg-poi-data/poi-data/*.csv")

#find all restaurants
df = df_poi.where(col('naics_code').like('7225%')\
        & col('category_tags').isNotNull()\
        & col('postal_code').isNotNull())

#extract cuisine type
def split_tags(tags_str):
    result = []
    words = tags_str.lower().split(',')
    for word in words:
        word = word.strip()
        result.append(word)
    return result

udf_split_tags = udf(split_tags, ArrayType(StringType()))

df_1 = df.withColumn('category_tags', udf_split_tags(col('category_tags')))

tags = spark.read.option("header", True)\
        .csv("s3a://sg-poi-data/cuisine_tags.csv").collect()

tag_list = []
for t in tags:
    tag_list.append(t['cuisine_tag'].lower())

def match_cuisine_tag(tags):
    result = []
    for tag in tags:
        if tag in tag_list:
            result.append(tag)
    return result

udf_match_cuisine_tag = udf(match_cuisine_tag, ArrayType(StringType()))

df_2 = df_1.withColumn('cuisine_tags', udf_match_cuisine_tag(col('category_tags')))\
        .where(col('cuisine_tags').isNotNull())\
        .drop('category_tags')

df_tags = df_2.select('safegraph_place_id', explode(col('cuisine_tags')))
df_tag_output = df_tags.withColumn('id', monotonically_increasing_id())




#process foot traffic data

#load foot traffic data from S3
df = spark.read.option("header",True).\
        option("escape", '"').\
        csv("s3a://sg-foot-traffic-data/*/*.csv")


df = df.where(col('visitor_home_cbgs') != '{}')

#transform the dataset to get top 10 restaurants each census block residents visit most frequently. 
df_by_id = df.groupby('safegraph_place_id').agg(collect_list(col('visitor_home_cbgs')).alias('merged'))

def get_cbgs_count(list_str):
    result = {}
    for string in list_str:
        counts = string[1:-1].split(',')
        temp = {}
        for item in counts:
            temp[item.split(':')[0]] = int(item.split(':')[1])
        result = dict(Counter(result) + Counter(temp))
    return result

udf_cbgs_count = udf(get_cbgs_count, MapType(StringType(), IntegerType()))

df_temp_1 = df_by_id.withColumn('cbgs_counts', udf_cbgs_count(col('merged'))).drop('merged')

df_temp_2 = df_temp_1.select('safegraph_place_id', explode(col('cbgs_counts')).alias("cbgs", "count"))

df_temp_3 = df_temp_2.groupby('cbgs', 'safegraph_place_id').sum('count')

window = Window.partitionBy(df_temp_3['cbgs']).orderBy(df_temp_3['sum(count)'].desc())

df_top_restr = df_temp_3.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 10).drop('rank')

#find top 5 cbgs where most visitors reside for each restaurant
window_1 = Window.partitionBy(df_temp_2['safegraph_place_id']).orderBy(df_temp_2['count'].desc())

df_top_cbgs = df_temp_2.select('*', rank().over(window_1).alias('rank')).filter(col('rank') <= 5).drop('rank')

df_top_restr = df_top_restr.withColumnRenamed('safegraph_place_id','competitor_id')
df_top_cbgs = df_top_cbgs.withColumnRenamed('safegraph_place_id','restaurant_id')


df_result = df_top_cbgs.join(df_top_restr, 'cbgs', 'inner').select('restaurant_id', 'competitor_id')\
        .where(col('restaurant_id') != col('competitor_id'))
df_result = df_result.dropDuplicates()




#integrate two datasets to find the potential competitors for each restaurant
df_join_tag = df_result.join(df_tags, df_result.restaurant_id==df_tags.safegraph_place_id, 'inner').withColumnRenamed('col','restaurant_cuisine').withColumnRenamed('safegraph_place_id','temp_id')
df_join_tag_1 = df_join_tag.join(df_tags, df_join_tag.competitor_id == df_tags.safegraph_place_id, 'inner').withColumnRenamed('col','competitor_cuisine')


df_output = df_join_tag_1.where(col('restaurant_cuisine') == col('competitor_cuisine')).select('restaurant_id','competitor_id','restaurant_cuisine').sort(col('restaurant_id'))

df_comp_output = df_output.withColumn('id', monotonically_increasing_id())




#save tables to postgres
df_tag_output.write\
        .format('jdbc')\
        .mode('overwrite')\
        .option('url', 'jdbc:postgresql://10.0.0.10:5432/poi_db')\
        .option('dbtable', 'tag_tb')\
        .option('user','postgres')\
        .option('password', 'postgres')\
        .option('driver','org.postgresql.Driver')\
        .save()


df_comp_output.write\
        .format('jdbc')\
        .mode('overwrite')\
        .option('url', 'jdbc:postgresql://10.0.0.10:5432/poi_db')\
        .option('dbtable', 'competition_tb')\
        .option('user','postgres')\
        .option('password', 'postgres')\
        .option('driver','org.postgresql.Driver')\
        .save()
                                                                                                                              177,6         Bot

















