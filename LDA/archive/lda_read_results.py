
topics = spark.read.load("s3://data-science-263198015083/models/ifa/2016-10-13/topics/")
preds = spark.read.load("s3://data-science-263198015083/models/ifa/2016-10-13/predictions/")


# define within doc ranking of topics
# ---------------------------------------------------
def in_doc_rank(df):
    for line in df:
        for topic_tuple in enumerate(line):
            topic = topic_tuple[0]
            score = round(float(topic_tuple[1])/float(0.01),0)*float(0.01)
            if score > 0:
                yield ((topic, score), 1)


# calculate the distribution of topics across document
# ---------------------------------------------------
doc_count = preds.count()
x = preds.select('topicDist') \
         .rdd \
         .map(lambda x: x[0]) \
         .mapPartitions(in_doc_rank) \
         .reduceByKey(lambda a, b: a + b) \
         .map(lambda x: (x[0][0], x[0][1], x[1])) \
         .toDF(['topic','weight','doc_count']) \
         .coalesce(1)

x.rdd.saveAsTextFile("s3://data-science-263198015083/models/ifa/2016-10-13/topics_in_docs/")


# define within doc ranking of topics
# ---------------------------------------------------
def describe_topics(df):
    for line in df:
        topic, word_weights = line[0], line[1]
        for word in word_weights:
            yield (topic, word[0], word[1])

z = topics.rdd \
          .mapPartitions(describe_topics) \
          .toDF(['topic','word','weight']) \
          .coalesce(1) 

z.rdd.saveAsTextFile("s3://data-science-263198015083/models/ifa/2016-10-13/topic_defs/")
