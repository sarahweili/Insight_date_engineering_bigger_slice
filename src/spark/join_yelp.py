from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, sum, min, first, size, split, lower, udf, rank, regexp_replace, monotonically_increasing_id, explode
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

df_poi_location = df_poi_location.withColumn('sg_latitude', col('sg_latitude').cast("float")).\
withColumn('sg_longitude', col('sg_longitude').cast("float"))

df_yelp_location = df_yelp_location.withColumn('latitude', col('latitude').cast("float")).\
withColumn('longitude', col('longitude').cast("float"))

## join poi and yelp datasets on state and zip_code
df_join = df_poi_location.join(df_yelp_location, on = ['state','zip_code'], how='inner')

## filter out null values in yelp dataset
df_join = df_join.where(col("longitude").isNotNull()).where(col("latitude").isNotNull())

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

udf_get_distance = udf(get_distance)

## add distance column
df_distance = df_join.withColumn('distance', udf_get_distance(df_join.sg_longitude, df_join.sg_latitude, df_join.longitude, df_join.latitude))

df_distance = df_distance.withColumn('distance', col('distance').cast("float"))

## get first 50 match records for each poi
w = Window.partitionBy(df_distance.safegraph_place_id).orderBy(df_distance.distance)

df_distance = df_distance.withColumn("d_rank",rank().over(w))

df_sl = df_distance.where(df_distance.d_rank <=50)

## match location names
## standardize strings
## not null
df_names = df_sl.where(col('location_name').isNotNull()).where(col('alias').isNotNull())

## lowercase
df_names = df_names.withColumn('sg_name_temp', lower(col('location_name'))).withColumn('yelp_name_temp', lower(col('alias')))

## remove ' from names
df_names = df_names.withColumn('sg_name_temp2', regexp_replace(col('sg_name_temp'), "'",'')).withColumn('yelp_name_temp2', regexp_replace(col('yelp_name_temp'), "'",'')).drop('sg_name_temp', 'yelp_name_temp')

## split the string
df_names = df_names.withColumn('sg_name', split(col('sg_name_temp2'), ' ')).withColumn('yelp_name', split(col('yelp_name_temp2'), '-')).drop('sg_name_temp2', 'yelp_name_temp2')

df_names = df_names.withColumn('name_length', size(col('sg_name')))

## match the names
df_temp = df_names.withColumn("id_new", monotonically_increasing_id()).withColumn('word', explode('sg_name'))

word_match = udf(lambda a, b: 0 if a in b else 1)

df_temp = df_temp.withColumn('match', word_match('word','yelp_name'))

df_temp = df_temp.withColumn('match', col('match').cast("integer"))

df_match = df_temp.groupBy('id_new').agg(sum("match").alias('unique_count')\
        ,first("safegraph_place_id").alias('sg_id')\
        ,first('location_name').alias('sg_name')\
        ,first('id').alias('yelp_id')\
        ,first('alias').alias('yelp_name')\
        ,first('distance').alias('distance')\
        ,first("d_rank").alias('d_rank')\
        ,first('name_length').alias('name_length'))\
        .orderBy('id_new', ascending=True)

get_unique_pct = udf(lambda a, b: round(a/b, 2))

df_match = df_match.withColumn('match_p', get_unique_pct('unique_count', 'name_length'))

## find the best match
w1 = Window.partitionBy('sg_id')

result = df_match.withColumn('best_match', min('match_p').over(w1))\
    .where(col('match_p') == col('best_match'))\
    .withColumn('min_distance', min('distance').over(w1))\
    .where(col('distance') == col('min_distance'))

## test the result
n = result.count()
print('total: ', n)

test = result.sample(True, 0.001, 123)

## write to postgresql
test.write\
        .format('jdbc')\
        .mode('overwrite')\
        .option('url', 'jdbc:postgresql://10.0.0.10:5432/poi_db')\
        .option('dbtable', 'test')\
        .option('user','postgres')\
        .option('password', 'postgres')\
        .option('driver','org.postgresql.Driver')\
        .save()

## read from postgresql
#print_test = spark.read.format("jdbc")\
#        .option("url", "jdbc:postgresql://10.0.0.10:5432/poi_db")\
#        .option("dbtable", "test")\
#        .option("user", "postgres")\
#        .option('password','postgres')\
#        .option("driver", "org.postgresql.Driver")\
#        .load()
