
# data location
# --------------------------------------------
INPUT_LOC = 's3://data-science-263198015083/data/wikipedia-110116/lda/'

# import packages
# --------------------------------------------
import re, sys, time, string, datetime
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.clustering import LDA
from pyspark.ml.feature import IDF, StopWordsRemover, CountVectorizer


# get the set of tags for each item
# --------------------------------------------
#def is_set(val):
#    return val[0] != '"'

#udfSet = udf(is_set, BooleanType())
#docs2 = docs.withColumn('goodtags', udfSet('tags'))
#docs2.filter(docs2.goodtags != True).show(50)
#docs2.groupBy('goodtags').count().show()


# import articles to dataframe
# ----------------------------
docs = spark.read.parquet(INPUT_LOC) \
                 .select(['id','words'])
#docs.cache()


# NLP feature transformers
# ----------------------------
stopwords = StopWordsRemover(inputCol="words", outputCol="no_stopwords")
vectorizer = CountVectorizer(minDF=10, vocabSize=15000, 
                         inputCol="no_stopwords", outputCol="pre_idf")
idf = IDF(inputCol="pre_idf", outputCol="features")
    
    
# LDA estimator
# ----------------------------
lda = LDA().setK(100) \
           .setMaxIter(100) \
           .setOptimizer('online') \
           .setFeaturesCol('features') \
           .setTopicDistributionCol('topicDist')


# Fit the pipeline to training documents
# ----------------------------
pipeline = Pipeline(stages=[stopwords, vectorizer, idf, lda])
model = pipeline.fit(docs)


# Predict the documents for the inputs
# ----------------------------
preds = model.transform(docs)


# translate topics to words
# ----------------------------
def translate_topics(df):
    for row in df:
        term_weights = [(vv_b.value[i[0]], i[1]) 
                        for i in zip(row.termIndices,row.termWeights)]
        for term in term_weights:
            yield (row.topic, term[0], term[1])


# save decoded topics
# ----------------------------
vv = model.stages[1].vocabulary
vv_b = sc.broadcast(vv)
topics = model.stages[3].describeTopics(maxTermsPerTopic = 25)
topics_decode = topics.rdd \
                      .coalesce(1) \
                      .mapPartitions(translate_topics)




