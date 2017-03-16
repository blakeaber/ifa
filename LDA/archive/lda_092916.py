
# import packages
# --------------------------------------------
from pyspark import SparkContext, SQLContext
import sys


# article parsing function
# ----------------------------
def article_parser(df):
    for line in df:
        if line[0] == '{' and line[-1] == '}':
            d = eval(line)
            id, mediatype, source, published, content = d["id"], d["media-type"], d["source"], d["published"], d["content"]
            yield (id, mediatype, source, published, content)


# translate topics to words
# ----------------------------
def topic_translate(df):
    for row in df:
        term_weights = [(vv_rdd.value[i[0]], i[1]) for i in zip(row.termIndices,row.termWeights)]
        yield (row.topic, term_weights)


# MAIN function call
# ----------------------------
if __name__ == "__main__":


    # check that only I/O parameters exist
    # --------------------------------------------
    if len(sys.argv) != 3:
        print("Usage: LDA ", sys.stderr)
        exit(-1)


    # start spark context
    # --------------------------------------------
    sc = SparkContext(appName="LDA Tagging Model")  
    sqlContext = SQLContext(sc)


    # import packages
    # --------------------------------------------
    from pyspark.ml.feature import CountVectorizer
    from pyspark.ml.feature import StopWordsRemover
    from pyspark.ml.feature import RegexTokenizer
    from pyspark.ml.clustering import LDA, LocalLDAModel, DistributedLDAModel
    
    
    # SET VARIABLES
    # ----------------------------
    # INPUT_FOLDER = "s3://data-science-263198015083/data/signal-media/"
    INPUT_FOLDER = sys.argv[1]
    NUM_PARTITIONS = 100
    
    MIN_WORD_LENGTH = 3
    VOCAB_SIZE = 50000
    WORD_PCT_DOCS = 0.005
    
    NUM_TOPICS = 200
    MAX_EPOCHS = 100
    LDA_OPTIM = 'online' # or 'em'
    LDA_SAMPLE = 0.05
    LDA_DECAY = 0.1
    LDA_OFFSET = 10
    
    # OUTPUT_FOLDER = "s3://data-science-263198015083/models/signal-media/2016-09-27/"
    OUTPUT_FOLDER = sys.argv[2]
    
    
    # import articles to textfile
    # ----------------------------
    articles_raw = sc.textFile(INPUT_FOLDER)
    
    
    # articles of dataframe
    # ----------------------------
    articles = articles_raw.repartition(NUM_PARTITIONS) \
                           .mapPartitions(article_parser) \
                           .toDF(["id","mediatype","source","published","content"])
    
    
    # built-in regex tokenizer
    # ----------------------------
    tokenizer = RegexTokenizer(inputCol="content", outputCol="words", pattern="\\W")
    articles_parsed = tokenizer.transform(articles)
    
    
    # custom length parsing
    # ----------------------------
    articles_parsed = articles_parsed.select(['id','words']) \
                                     .rdd \
                                     .map(lambda x: (x.id, [i for i in x.words if len(i) >= MIN_WORD_LENGTH])) \
                                     .toDF(['id','long_words'])
    
    remover = StopWordsRemover(inputCol="long_words", outputCol="filtered_words")
    articles_parsed = remover.transform(articles_parsed) \
                             .select(['id','filtered_words'])
    
    
    # built-in count vectorizer
    # ----------------------------
    vectorizer = CountVectorizer(minDF=WORD_PCT_DOCS, vocabSize=VOCAB_SIZE, 
                                 inputCol="filtered_words", outputCol="features") \
                                 .fit(articles_parsed)
    
    articles_model = vectorizer.transform(articles_parsed).select(["id", "features"])
    
    
    # ML
    # set parameters of LDA algorithm
    # ----------------------------
    lda = LDA().setK(NUM_TOPICS) \
               .setMaxIter(MAX_EPOCHS) \
               .setOptimizer(LDA_OPTIM) \
               .setFeaturesCol('features') \
               .setTopicDistributionCol('topicDist')

#               .setSubsamplingRate(LDA_SAMPLE) \
#               .setLearningDecay(LDA_DECAY) \
#               .setLearningOffset(LDA_OFFSET) \

    # ML
    # fit model to data
    # ----------------------------
    model = lda.fit(articles_model)
    vocab_size = model.vocabSize()
    
    
    # model fit diagnostics
    # ----------------------------
    print("Size of Vocabulary: " + str(vocab_size))
    print("Log Likelihood: " + str(model.logLikelihood(articles_model)))
    print("Perplexity: " + str(model.logPerplexity(articles_model)))


    # save model to s3
    # ----------------------------
    model.save(OUTPUT_FOLDER)
    # model2 = DistributedLDAModel.load(OUTPUT_FOLDER)
    
    
    # save decoded topics
    # ----------------------------
    vv = vectorizer.vocabulary
    vv_rdd = sc.broadcast(vv)
    topics = model.describeTopics()
    topics_decode = topics.rdd \
                          .mapPartitions(topic_translate) \
                          .toDF(['topic','wordDist']) \
                          .coalesce(1) \
    
    topics_decode.write.save(OUTPUT_FOLDER + 'topics/')
    
    
    # save document classification
    # ----------------------------
    y_hat = model.transform(articles_model)
    y_hat.write.save(OUTPUT_FOLDER + 'predictions/')
    

    # stop spark context
    # --------------------------------------------
    sc.stop()
