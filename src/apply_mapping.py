from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("apply_mapping").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")



# define read and write functions
def write_psql(df, tb_name):
        df.write.format('jdbc')\
        .mode('overwrite')\
        .option('url', 'jdbc:postgresql://10.0.0.10:5432/poi_db')\
        .option('dbtable', tb_name)\
        .option('user','postgres')\
        .option('password', 'postgres')\
        .option('driver','org.postgresql.Driver')\
        .save()
        return print(tb_name, ' is saved!')

def read_psql(tb_name):
        df = spark.read.format('jdbc')\
        .mode('overwrite')\
        .option('url', 'jdbc:postgresql://10.0.0.10:5432/poi_db')\
        .option('dbtable', tb_name)\
        .option('user','postgres')\
        .option('password', 'postgres')\
        .option('driver','org.postgresql.Driver')\
        .save()
        return df


# read mapping table
df_mapping = read_psql('mapping_tb')

# mapping competition table
df_competition = read_psql('competition_tb')

df_competition = df_competition.withColumnRenamed('id', 'competition_id')\
        .withColumnRenamed('restaurant_id', 'sg_id_r')\
        .withColumnRenamed('competitor_id', 'sg_id_c')

df_competition_mapped = df_competition.join(result.select('id','sg_id'), df_competition.sg_id_r == result.sg_id, 'inner')\
        .withColumnRenamed('id','restaurant_id')\
        .drop('sg_id_r','sg_id')\
        .join(result.select('id','sg_id'), df_competition.sg_id_c == result.sg_id, 'inner')\
        .withColumnRenamed('id','competitor_id')\
        .drop('sg_id_c','sg_id')

write_psql(df_competition_mapped, 'competition_mapped_tb')



# mapping review table
df_review = read_psql('topics_tb')

df_review = df_review.withColumnRenamed('Avg(Topics[0])','Service')\
        .withColumnRenamed('Avg(Topics[1])','Atmosphere')\
        .withColumnRenamed('Avg(Topics[2])','Asian')\
        .withColumnRenamed('Avg(Topics[3])','Coffee')\
        .withColumnRenamed('Avg(Topics[4])','Dessert')\
        .withColumnRenamed('Avg(Topics[5])','Mexican')\
        .withColumnRenamed('Avg(Topics[6])','Accomodation')\
        .withColumnRenamed('Avg(Topics[7])','American')\
        .withColumnRenamed('Avg(Topics[8])','European')

df_bus = df_yelp_input.select('business_id','name','address','city','state','postal_code','latitude','longitude','stars')

df_review_mapped = df_review.join(df_bus, 'business_id', 'inner')\
        .join(result.select('id','yelp_id'), df_review.business_id == result.yelp_id, 'inner')\
        .drop('business_id','yelp_id')

write_psql(df_review_mapped,'review_mapped_tb')



## mapping tag table
df_tag = read('tag_tb')

df_tag = df_tag.withColumnRenamed('id','tag_id')\
        .withColumnRenamed('col','Cuisine')

df_tag_mapped = df_tag.join(result.select('id','sg_id'), df_tag.safegraph_place_id == result.sg_id, 'inner')\
        .drop('safegraph_place_id','sg_id')

write_psql(df_tag_mapped,'tag_mapped_tb')
