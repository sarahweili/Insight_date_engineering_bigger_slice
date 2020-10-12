from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, max,  sum, min, first, desc, size, split, lower, udf, rank, regexp_replace, monotonically_increasing_id, explode
from pyspark.sql.window import Window
import re as re
from pyspark.sql.types import FloatType, IntegerType, ArrayType, StringType
from math import radians, cos, sin, asin, sqrt
import Levenshtein as lev



spark = SparkSession.builder.appName("mapping").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

## read poi datasets
df_sg_input = spark.read.option("header",True).option("escape", '"').csv("s3a://sg-poi-data/poi-data/*.csv")
df_sg = df_sg_input.where(df_sg_input.naics_code.like('7225%'))

## read yelp dataset
df_yelp_input = spark.read.json("s3a://yelp-reviews-data/yelp_academic_dataset_business.json")
df_yelp = df_yelp_input.where(col('categories').contains('Restaurants'))



## select and process relevant columns
df_sg = df_sg.select('safegraph_place_id', 'location_name','latitude', 'longitude', 'street_address', 'city', 'region','postal_code').na.drop()

df_yelp = df_yelp.select('business_id', 'name', 'latitude', 'longitude', 'address','city', 'state', 'postal_code').na.drop()

df_sg = df_sg.withColumnRenamed('postal_code','sg_postal_code')\
        .withColumnRenamed('latitude','sg_latitude')\
        .withColumnRenamed('longitude','sg_longitude')\
        .withColumnRenamed('city','sg_city')

df_sg = df_sg.withColumn('sg_latitude', col('sg_latitude').cast("float"))\
        .withColumn('sg_longitude', col('sg_longitude').cast("float"))

df_yelp = df_yelp.withColumn('latitude', col('latitude').cast("float"))\
        .withColumn('longitude', col('longitude').cast("float"))

## join poi and yelp datasets on state and zip_code
df_s_zip = df_yelp.join(df_sg, (df_sg.region == df_yelp.state) & (df_sg.sg_postal_code==df_yelp.postal_code), 'inner')



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

udf_get_distance = udf(get_distance, FloatType())

## add distance column
df_distance = df_s_zip.withColumn('distance', udf_get_distance(col('sg_longitude'), col('sg_latitude'), col('longitude'), col('latitude')))




## get first 50 match records for each poi
df_d50 = df_distance.where(col('distance') <=3)

## match location names
#common words in restaurant names
stopwords=['restaurant','restaurants','pizza','grill','bar','the','and','cafe','kitchen','house','food','mexican','la'\
        ,'cuisine','bistro','thai','taco','burger','express','of','bbq','pizzeria','deli','bakery','pub','chinese'\
        'chicken','el','lounge','italian','coffee','shop','pho','café','le','buffet','tavern','market','steakhouse'\
        ,'on','diner','a', 'de', 'n', 'at']

#preprocess the names

def process_name(name):
    result=[]
    name = name.replace('&','').replace("'",'').replace('-','').replace("+",'').replace("'s",'')
    for word in name.split(' '):
        if word != '':
            word_lowered = word.lower()
            if word_lowered not in stopwords:
                result.append(word_lowered)
    return result

udf_process_name = udf(process_name, ArrayType(StringType()))


df_name = df_d50.withColumn('sg_name', udf_process_name(col('location_name')))\
        .withColumn('yelp_name', udf_process_name(col('name'))).where((size(col('sg_name'))>=1) & (size(col('yelp_name'))>=1))

def match_first_word(list_1, list_2):
    score_list = []
    for word in list_2:
        score = lev.distance(list_1[0], word)
        score_list.append(score)
    score_list.sort()
    if score_list[0] <= 3:
        return 1
    else:
        return 0


udf_match_first_word = udf(match_first_word, IntegerType())


df_name = df_name.withColumn('first_word', udf_match_first_word(col('yelp_name'), col('sg_name'))).where(col('first_word') == 1)



df_name = df_name.withColumn('name_length', size(col('yelp_name')))

df_temp = df_name.withColumn("id_new", monotonically_increasing_id()).withColumn('word', explode('yelp_name'))

word_match = udf(lambda a, b: 0 if a in b else 1, IntegerType())

df_temp = df_temp.withColumn('match', word_match('word','sg_name'))



df_match = df_temp.groupBy('id_new').agg(sum("match").alias('unique_count')\
        ,first("safegraph_place_id").alias('sg_id')\
        ,first('location_name').alias('sg_org_name')\
        ,first('street_address').alias('sg_address')\
        ,first('business_id').alias('yelp_id')\
        ,first('name').alias('yelp_org_name')\
        ,first('address').alias('yelp_address')\
        ,first('distance').alias('distance')\
        ,first('name_length').alias('name_length'))\
        .orderBy('id_new', ascending=True)


get_unique_pct = udf(lambda a, b: round(a/b, 2), FloatType())

df_match = df_match.withColumn('match_p', get_unique_pct('unique_count', 'name_length'))


w1 = Window.partitionBy('yelp_id')

df_result = df_match.withColumn('best_match', min('match_p').over(w1))\
    .where(col('match_p') == col('best_match'))\
    .withColumn('min_distance', min('distance').over(w1))\
    .where(col('distance') == col('min_distance'))


df_result = df_result.groupby('yelp_id').agg(first("sg_id").alias('sg_id')\
        ,first('yelp_org_name'), first('sg_org_name')\
        ,first('sg_address'), first('yelp_address')\
        ,first('distance'), first('match_p'))


result = df_result.select('sg_id','yelp_id')

## write to postgresql
result.write.format('jdbc')\
        .mode('overwrite')\
        .option('url', 'jdbc:postgresql://10.0.0.10:5432/poi_db')\
        .option('dbtable', 'mapping_tb')\
        .option('user','postgres')\
        .option('password', 'postgres')\
        .option('driver','org.postgresql.Driver')\
        .save()


## test the result
#n = df_result.count()
#print('total: ', n)

#test = df_result.sample(True, 0.005, 1234)



## read from postgresql
#label_test = spark.read.format("jdbc")\
#        .option("url", "jdbc:postgresql://10.0.0.10:5432/poi_db")\
#        .option("dbtable", "test_label")\
#        .option("user", "postgres")\
#        .option('password','postgres')\
#        .option("driver", "org.postgresql.Driver")\
#        .load()

#test = label_test.select('yelp_id').join(df_result, 'yelp_id', 'left')


## write test to postgresql
#test.write.format('jdbc')\
#        .mode('overwrite')\
#        .option('url', 'jdbc:postgresql://10.0.0.10:5432/poi_db')\
#        .option('dbtable', 'test_label')\
#        .option('user','postgres')\
#        .option('password', 'postgres')\
#        .option('driver','org.postgresql.Driver')\
#        .save()

