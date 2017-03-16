
# import packages
# --------------------------------------------
from pyspark.sql import SparkSession
import re, sys, time, string, datetime


# translate topics to words
# ----------------------------
def translate_topics(df):
    for row in df:
        term_weights = [(vv_b.value[i[0]], i[1]) 
                        for i in zip(row.termIndices,row.termWeights)]
        for term in term_weights:
            yield (row.topic, term[0], term[1])


# measure topic concentration across docs
# ---------------------------------------------------
def in_doc_rank(df):
    for line in df:
        for topic_tuple in enumerate(line):
            topic = topic_tuple[0]
            score = round(float(topic_tuple[1])/float(0.05),0)*float(0.05)
            if score > 0:
                yield ((topic, score), 1)


# tag articles with topics
# ----------------------------        
def assign_tags(df):
    for row in df:
        article = ' '.join(row.words)
        topics = [i for (i,j) in enumerate(row.topicDist) if j >= TOPIC_THRESH]
        for topic in topics:
            yield (topic, row.published, row.id, article)


# MAIN function call
# ----------------------------
if __name__ == "__main__":

    """
    Need to input 4 parameters:
     1) whether to create a model ('build') or load a model ('load')
     2) whether the model is Var Bayes ('online') or Exp Max ('em')
     3) the location of the input data to fit/predict
     4) the S3 location where the model will save/load
    """


    # start spark context
    # --------------------------------------------
    spark = SparkSession.builder \
                        .appName('LDA Tagger - Final Model') \
                        .getOrCreate()


    # check that only I/O parameters exist
    # --------------------------------------------
    if len(sys.argv) != 5:
        print("Usage: LDA ", sys.stderr)
        exit(-1)


    # import packages
    # --------------------------------------------
    from pyspark.ml import Pipeline, PipelineModel
    from pyspark.ml.clustering import LDA
    from pyspark.ml.feature import IDF, StopWordsRemover, CountVectorizer
    
    
    # VOCAB SETTINGS
    # ----------------------------
    VOCAB_SIZE = 20000
    WORD_OCCUR = 50

    print("Vocab Size: ", VOCAB_SIZE)
    print("Min Words: ", WORD_OCCUR)

    # MODEL SETTINGS
    # ----------------------------
    MODEL_TYPE = sys.argv[1]
    MODEL_OPTIM = sys.argv[2]
    INPUT_LOC = sys.argv[3]
    OUTPUT_LOC = sys.argv[4]
    
    print("Model Type: ", MODEL_TYPE)
    print("Model Optim: ", MODEL_OPTIM)
    print("Input: ", INPUT_LOC)
    print("Output: ", OUTPUT_LOC)
    
    # LDA SETTINGS
    # ----------------------------
    NUM_TOPICS = 250
    MAX_EPOCHS = 150
    TOPIC_ALPHA = 0.25
    LDA_OFFSET = 1000

    print("# Topics: ", NUM_TOPICS)
    print("# Epochs: ", MAX_EPOCHS)
    print("Topic Alpha: ", TOPIC_ALPHA)
    print("LDA Offset: ", LDA_OFFSET)
    
    
    # TAG SETTINGS
    # ----------------------------
    global TOPIC_THRESH
    TOPIC_THRESH = 0.1

    print("Topic Threshold: ", TOPIC_THRESH)
    
    # import articles to dataframe
    # ----------------------------
    docs = spark.read.parquet(INPUT_LOC).sample(False, 0.25, 42)
    
    
    # ML
    # Configure a 3-stage pipeline: Stopwords, Vectorizer, and LDA
    # ----------------------------
    if MODEL_TYPE == 'build':
    
    
        # NLP feature transformers
        # ----------------------------
        stopwords = StopWordsRemover(inputCol="words", outputCol="no_stopwords")
        vectorizer = CountVectorizer(minDF=WORD_OCCUR, vocabSize=VOCAB_SIZE, 
                                 inputCol="no_stopwords", outputCol="pre_idf")
        idf = IDF(inputCol="pre_idf", outputCol="features")
    
    
        # LDA estimator
        # ----------------------------
        if MODEL_OPTIM == 'online':
            lda = LDA().setK(NUM_TOPICS) \
                       .setMaxIter(MAX_EPOCHS) \
                       .setOptimizer(MODEL_OPTIM) \
                       .setFeaturesCol('features') \
                       .setLearningOffset(LDA_OFFSET) \
                       .setTopicConcentration(TOPIC_ALPHA) \
                       .setTopicDistributionCol('topicDist')
    
        else:
            lda = LDA().setK(NUM_TOPICS) \
                       .setMaxIter(MAX_EPOCHS) \
                       .setOptimizer(MODEL_OPTIM) \
                       .setFeaturesCol('features') \
                       .setTopicDistributionCol('topicDist')

        print("Begin Model Training...")
        start = time.time()

        # Fit the pipeline to training documents.
        # docs = docs.sample(False, 0.01, 42)
        # ----------------------------
        pipeline = Pipeline(stages=[stopwords, vectorizer, idf, lda])
        model = pipeline.fit(docs)

        took = time.time() - start
        print("                    ... Model Training Complete in " + str(round(float(took)/float(3600),1)) + " hours!")
        print("Saving Model to S3  ...")
        start = time.time()

        # save model to s3
        # ----------------------------
        model.save(OUTPUT_LOC)

        took = time.time() - start
        print("                    ... Save to S3 Complete in " + str(round(float(took)/float(3600),1)) + " hours!")
        print("Begin Topic Decoding...")
        start = time.time()

        # save decoded topics
        # ----------------------------
        vv = model.stages[1].vocabulary
        vv_b = sc.broadcast(vv)
        topics = model.stages[3].describeTopics(maxTermsPerTopic = 25)
        topics_decode = topics.rdd \
                              .coalesce(1) \
                              .mapPartitions(translate_topics)

        took = time.time() - start
        print("                    ... Topic Decoding Complete in " + str(round(float(took)/float(3600),1)) + " hours!")
        print("Saving Topics to S3 ...")    
        start = time.time()

        topics_decode.saveAsTextFile(OUTPUT_LOC + 'topics/')

        took = time.time() - start
        print("                    ... Topic Save Complete! in " + str(round(float(took)/float(3600),1)) + " hours!")
        print("Predict All Topics  ...")    
        start = time.time()

        # predict topics for evaluation
        # ---------------------------------------------------
        preds = model.transform(docs)

        took = time.time() - start
        print("                    ... Prediction Complete in " + str(round(float(took)/float(3600),1)) + " hours!")
        print("Calculate % Docs    ...")    
        start = time.time()

        # calculate the distribution of topics across document
        # ---------------------------------------------------
        topic_dist = preds.select('topicDist') \
                          .rdd \
                          .map(lambda x: x[0]) \
                          .mapPartitions(in_doc_rank) \
                          .reduceByKey(lambda a, b: a + b) \
                          .coalesce(1) \
                          .map(lambda x: (x[0][0], x[0][1], x[1]))

        took = time.time() - start
        print("                    ... % Docs Complete in " + str(round(float(took)/float(3600),1)) + " hours!")
        print("Saving Analysis     ...")    
        start = time.time()

        topic_dist.saveAsTextFile(OUTPUT_LOC + 'topic-distribution/')

        took = time.time() - start
        print("                    ... Analysis Complete in " + str(round(float(took)/float(3600),1)) + " hours!")

    # ML
    # Load pre-existing model and predict topics
    # ----------------------------
    else:

        start = time.time()
        print("Loading Model       ...")    

        model = PipelineModel.load(OUTPUT_LOC)

        # read in topics
        # ----------------------------
        # topics = sqlContext.read.parquet(OUTPUT_LOC + 'topic-codes/')
        # topics = topics.select('tag') \
        #                .collect()
        # topics_b = sc.broadcast(topics)

        took = time.time() - start
        print("                    ... Model Load Complete in " + str(round(float(took)/float(3600),1)) + " hours!")
        print("Saving Predictions  ...")    
        start = time.time()

        # save IFA document classification
        # ----------------------------
        preds = model.transform(docs) \
                     .select(['id','dataset','published','words','topicDist'])

        ifa_preds = preds.filter(preds.dataset=='ifa') \
                         .sample(False, 0.2, 42) \
                         .rdd \
                         .mapPartitions(assign_tags) \
                         .toDF(['topic','published', 'id', 'article'])
        
        ifa_preds.cache()

        ifa_final = ifa_preds.repartition('topic')

        ifa_final.rdd.saveAsTextFile(OUTPUT_LOC + 'topic-examples/')

        took = time.time() - start
        print("                    ... Predictions Complete in " + str(round(float(took)/float(3600),1)) + " hours!")    
    
    # stop spark context
    # --------------------------------------------
    spark.stop()
