from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from math import radians, cos, sin, asin, sqrt

spark = SparkSession.builder.appName("Map_location").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

## read poi datasets
df_poi1 = spark.read.option("header",True).csv("s3a://restaurants-poi-data/core_poi-part1.csv")
df_poi2 = spark.read.option("header",True).csv("s3a://restaurants-poi-data/core_poi-part2.csv")
df_poi3 = spark.read.option("header",True).csv("s3a://restaurants-poi-data/core_poi-part3.csv")
df_poi4 = spark.read.option("header",True).csv("s3a://restaurants-poi-data/core_poi-part4.csv")
df_poi5 = spark.read.option("header",True).csv("s3a://restaurants-poi-data/core_poi-part5.csv")

df_poi = df_poi1.union(df_poi2).union(df_poi3).union(df_poi4).union(df_poi5)


## read yelp dataset
df_yelp = spark.read.json("s3a://yelp-data/yelp_restaurants.json")

## get all restaurant poi
df_poi_restr = df_poi.where(df_poi.naics_code.like('7225%'))

## select and process relevant columns
df_poi_location = df_poi_restr.select('safegraph_place_id', 'location_name','latitude', 'longitude','region','postal_code')

df_yelp_location = df_yelp.select('id', 'alias', 'coordinates','location')

df_yelp_location = df_yelp_location.\
withColumn('latitude', df_yelp_location.coordinates.latitude).\
withColumn('longitude', df_yelp_location.coordinates.longitude).\
withColumn('state', df_yelp_location.location.state).\
withColumn('zip_code', df_yelp_location.location.zip_code).\
drop('coordinates','location')

df_poi_location = df_poi_location.withColumnRenamed('region','state').\
withColumnRenamed('postal_code','zip_code').\
withColumnRenamed('latitude','sg_latitude').\
withColumnRenamed('longitude','sg_longitude')

df_poi_location = df_poi_location.withColumn('sg_latitude', F.col('sg_latitude').cast("float")).\
withColumn('sg_longitude', F.col('sg_longitude').cast("float"))

## join poi and yelp datasets on state and zip_code
df_join = df_poi_location.join(df_yelp_location, on = ['state','zip_code'], how='inner')

## filter out null values in yelp dataset
df_join = df_join.where(F.col("longitude").isNotNull()).where(F.col("latitude").isNotNull())

## define the function to calculate distance between two geo locations
def get_distance(longit_a, latit_a, longit_b, latit_b):
    longit_a, latit_a, longit_b, latit_b = map(radians, [longit_a,  latit_a, longit_b, latit_b])
    dist_longit = longit_b - longit_a
    dist_latit = latit_b - latit_a
    area = sin(dist_latit/2)**2 + cos(latit_a) * cos(latit_b) * sin(dist_longit/2)**2
    central_angle = 2 * asin(sqrt(area))
    radius = 6371
    distance = central_angle * radius
    return abs(round(distance, 2))

udf_get_distance = F.udf(get_distance)

## add distance column
df_distance = df_join.withColumn('distance', udf_get_distance(df_join.sg_longitude, df_join.sg_latitude, df_join.longitude, df_join.latitude))

df_distance = df_distance.withColumn('distance', F.col('distance').cast("float"))

## get first 50 match records for each poi
w = Window.partitionBy(df_distance.safegraph_place_id).orderBy(df_distance.distance)

df_distance = df_distance.withColumn("d_rank",F.rank().over(w))

df_sl = df_distance.where(df_distance.d_rank <=50)

## match location names
df_names = df_sl.where(F.col('location_name').isNotNull()).where(F.col('alias').isNotNull())

df_names = df_names.withColumn('sg_name', F.split(F.lower(F.col('location_name')), ' ')).withColumn('yelp_name', F.split(F.lower(F.col('alias')), '-'))

df_temp = df_names.withColumn("id_new", F.monotonically_increasing_id()).withColumn('word', F.explode('sg_name'))

word_match = F.udf(lambda a, b: 0 if a in b else 1)

df_temp = df_temp.withColumn('match', word_match('word','yelp_name'))

df_temp = df_temp.withColumn('match', F.col('match').cast("integer"))

df_match = df_temp.groupBy('id_new').agg(F.sum("match").alias('unique_count')\
        ,F.first("safegraph_place_id").alias('sg_id')\
        ,F.first('location_name').alias('sg_name')\
        ,F.first('id').alias('yelp_id')\
        ,F.first('alias').alias('yelp_name')\
        ,F.first('distance').alias('distance')\
        ,F.first("d_rank").alias('d_rank'))\
        .orderBy('id_new', ascending=True)

## find the best match
w1 = Window.partitionBy('sg_id')

result = df_match.withColumn('max_match', F.min('unique_count').over(w1))\
    .where(F.col('unique_count') == F.col('max_match'))\
    .withColumn('min_distance', F.min('distance').over(w1))\
    .where(F.col('distance') == F.col('min_distance'))

## test the result
#n = result.count()
#print('total: ', n)

#result.sample(True, 0.01).show(100, False)


## write to postgresql
#df_match.limit(100).write\
#        .format('jdbc')\
#        .mode('overwrite')\
#        .option('url', 'jdbc:postgresql://10.0.0.10:5432/poi_db')\
#        .option('dbtable', 'test1')\
#        .option('user','postgres')\
#        .option('password', 'postgres')\
#        .option('driver','org.postgresql.Driver')\
#        .save()

## read from postgresql
#df_test = spark.read.format("jdbc").option("url", "jdbc:postgresql://10.0.0.10:5432/poi_db").option("dbtable", "test1").option("user", "postgres").option('password','postgres').option("driver", "org.postgresql.Driver").load()

