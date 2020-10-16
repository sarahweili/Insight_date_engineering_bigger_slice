from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, udf, concat
from pyspark.sql.types import ArrayType, StringType, IntegerType, FloatType
#from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import re as re
from pyspark.ml.feature import CountVectorizer, IDF
from pyspark.ml.linalg import Vectors, Vector
from pyspark.ml.clustering import LDA
import pandas as pd

spark = SparkSession.builder.appName("review").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")



df_bus = spark.read.json("s3a://yelp-reviews-data/yelp_academic_dataset_business.json")
df_review = spark.read.json("s3a://yelp-reviews-data/yelp_academic_dataset_review.json")
df_restr = df_bus.where(col('categories').contains('Restaurants'))
df_review_f = df_review.where(col('text').isNotNull() & (col('useful') >4))
df_reviews = df_review_f.join(df_restr, 'business_id', 'inner').select('business_id','text')



# Stop words associated with location, wanted recommender to be location agnostic
additional_stopwords = ['restaurant', 'vegas', 'waitress', 'dinner',
                        'wa', 'waiter', 'scottsdale', 'toronto',
                       'pittsburgh', 'madison', 'fremont', 'manager',
                       'husband', 'phoenix', 'dakota', 'caesar',
                       'bellagio', 'canal', 'venetian', 'mandalay',
                       'lotus', 'siam', 'buca','beppo',
                       'di', 'buca', 'ohio', 'tretmont',
                       'bathroom', 'montreal', 'italy', 'et', 'est',
                       'que', 'il', 'en', 'la', 'une', 'pa', 'hostess']

# food related stopwords that I needed to remove as this was a bar recommender not a restaurant recommender
food_stopwords = ['chicken', 'curry', 'steak', 'egg', 'pork', 'meat',
                 'sandwich', 'cheese', 'pasta', 'salad', 'taco', 'salsa',
                 'guacamole', 'bruschetta', 'fish', 'dessert', 'onion',
                 'bun', 'sushi', 'sashimi', 'shrimp', 'crab', 'seafood',
                 'lobster', 'meatball', 'potato', 'entree', 'burrito',
                 'tortilla', 'food', 'olive', 'ramen', 'rib', 'brisket',
                 'bbq', 'bean', 'chip', 'mac', 'rice', 'beef', 'avocado',
                 'pizza', 'garlic', 'crust', 'burger', 'bacon', 'meal',
                 'toast', 'bread', 'lunch', 'breakfast', 'appetizer',
                 'filet', 'cake', 'sauce', 'dish', 'dining', 'pie',
                 'nacho', 'enchilada', 'wing', 'roll', 'salmon', 'oyster',
                 'soup', 'sausage', 'truffle', 'noodle', 'ravioli', 'lasagna',
                 'veal', 'buffet', 'tiramisu', 'eggplant', 'chocolate', 'scallop',
                 'chef', 'duck', 'butter', 'steakhouse', 'kobe', 'caviar',
                 'stroganoff', 'corn', 'mushroom', 'thai', 'prawn', 'coconut',
                 'pretzel', 'pho', 'tuna', 'donut', 'chili', 'panini', 'fig',
                 'holstein', 'calamari', 'pancake', 'fruit', 'pierogi', 'pierogis',
                 'pierogies', 'mignon', 'rare', 'medium', 'lamb', 'milkshake',
                 'ribeye', 'mashed', 'bone', 'bass', 'sea', 'guac', 'queso',
                 'fajitas', 'carne', 'pasty', 'asada', 'mozzarella', 'marsala',
                 'spaghetti', 'gnocchi', 'parm', 'alfredo', 'linguine', 'buffalo',
                 'falafel', 'hummus', 'pita', 'scrambled', 'risotto', 'fat',
                 'strip', 'roast', 'miso', 'tempura', 'udon', 'edamame',
                 'cucumber', 'dipping', 'yellowtail', 'waffle', 'quesadilla', 'dog',
                 'primanti', 'tot', 'tater', 'phyllo', 'pomegranate', 'cinnamon',
                 'shepherd', 'banger', 'corned', 'foie', 'gras', 'latte', 'banana',
                 'poutine', 'seabass', 'du', 'je', 'au', 'mais', 'très', 'asparagus',
                 'slider', 'tikka', 'naan', 'popcorn', 'masala', 'bonefish', 'lime']

stopwords_rows = spark.read.option("header",True).csv("s3a://yelp-reviews-data/stopwords.csv").collect()
stopwords = []
for r in stopwords_rows:
    stopwords.append(r['stopword'])
all_stopwords = stopwords+additional_stopwords+food_stopwords

# lemmatizer
lemmatizer = WordNetLemmatizer()
# clean the text
def preprocess_text(text):
    result = []
    for word in text.split(' '):
        word_punc = re.sub('[,\.!?-]@&=', '', word)
        if word != '':
            word_lowered = word_punc.lower()
            word_lm=lemmatizer.lemmatize(word_lowered)
            if word_lm not in all_stopwords:
                result.append(word_lm)
    return result


udf_preprocess_text = udf(preprocess_text, ArrayType(StringType()))
df_clean = df_reviews.withColumn('review', udf_preprocess_text(col('text'))).select('business_id','review')


#topic modeling
cv = CountVectorizer(inputCol="review", outputCol="raw_features", vocabSize=5000, minDF=10.0)
cvmodel = cv.fit(df_clean)
result_cv = cvmodel.transform(df_clean)
idf = IDF(inputCol="raw_features", outputCol="features")
idfModel = idf.fit(result_cv)
result_tfidf = idfModel.transform(result_cv)
lda = LDA(k=9, maxIter=100).setTopicDistributionCol('topicDistributionCol')
lda_model = lda.fit(result_tfidf.select('business_id','features'))
transformed = lda_model.transform(result_tfidf.select('business_id','features'))
topics = lda_model.describeTopics(maxTermsPerTopic=15)
vocabArray = cvmodel.vocabulary
topics = topics.toPandas()
topics_matrix=topics['termIndices'].apply(lambda x: [vocabArray[index] for index in x])
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

#print topics
print(topics_matrix)


udf_to_array = udf(lambda v: v.toArray().tolist(), ArrayType(FloatType()))
df_output_tmp = transformed.withColumn('topics', udf_to_array('topicDistributionCol'))
df_output = df_output_tmp.select('business_id', col('topics')[0], col('topics')[1], col('topics')[2]\
        ,col('topics')[3], col('topics')[4], col('topics')[5], col('topics')[6], col('topics')[7]\
       ,col('topics')[8]).groupby('business_id').mean()




#save the table to postgres
df_output.write\
        .format('jdbc')\
        .mode('overwrite')\
        .option('url', 'jdbc:postgresql://10.0.0.10:5432/poi_db')\
        .option('dbtable', 'topics_tb')\
        .option('user','postgres')\
        .option('password', 'postgres')\
        .option('driver','org.postgresql.Driver')\
        .save()
