
# import packages
# --------------------------------------------
from pyspark import SparkContext, SQLContext
from nltk.tokenize import RegexpTokenizer
import re, sys, string, datetime


# article parsing function
# ----------------------------
def article_parser_signal(df):
    for line in df:
        if line[0] == '{' and line[-1] == '}':
            d = eval(line)
            id, cat, source = d["id"], d["media-type"], d["source"]
            pub_dt, content = d["published"], d["content"]
            dataset = 'signal'
            url = ''
            yield (id, dataset, cat, source, url, pub_dt, content)


def article_parser_ag(df):
    for line in df:
        d = re.split(r'<.*?><.*?>', line[4:-10])
        id, source, url, title, image = d[0], d[1], d[2], d[3], d[4]
        cat, desc, rank, pub_dt = d[5], d[6], d[7], d[8]
        content = title + ' ' + desc
        dataset = 'ag'
        yield (id, dataset, cat, source, url, pub_dt, content)


def article_parser_ifa(df):
    for line in df:
        d = line.split('\t')
        if len(d) >= 7:
            id, source, source_link, url_id = d[0], d[1], d[2], d[3]
            url, pub_unix, content = d[4], d[5], ' '.join(d[6:])
            dataset = 'ifa'
            cat = ''
            pub_dt = datetime.datetime.fromtimestamp(int(pub_unix)) \
                                      .strftime('%Y-%m-%d %H:%M:%S')
            yield (url, dataset, cat, source, url, pub_dt, content)


def custom_tokenizer(df):
    tokenizer = RegexpTokenizer('\w+|\S+')
    html_stripper = re.compile(r'<.*?>')
    punct = set(string.punctuation)
    for line in df:
        if len(line) == 7:
            no_tags = html_stripper.sub('', line[6])
            no_punct = re.sub(r'\W+ ', ' ', no_tags)
            lc_words = no_punct.lower()
            tokens = [''.join([ch for ch in x if ch not in punct]) for x in tokenizer.tokenize(lc_words)]
            alpha_only = [x for x in tokens if not any(c.isdigit() for c in x)]
            long_alpha = [i for i in alpha_only if len(i) >= 3 and len(i) <= 20]
            yield (line[0], line[1], line[2], line[3], line[4], line[5], long_alpha)


# translate topics to words
# ----------------------------
# def topic_translate(df):
#     for row in df:
#         term_weights = [(vv_rdd.value[i[0]], i[1]) 
#                         for i in zip(row.termIndices,row.termWeights)]
#         yield (row.topic, term_weights)


# MAIN function call
# ----------------------------
if __name__ == "__main__":


    # check that only I/O parameters exist
    # --------------------------------------------
    if len(sys.argv) != 2:
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
    from pyspark.ml.clustering import LDA, LocalLDAModel, DistributedLDAModel
    
    
    # SET VARIABLES
    # ----------------------------
    NUM_PARTITIONS = 500
    
    VOCAB_SIZE = 25000
    WORD_PCT_DOCS = 0.005
    
    NUM_TOPICS = 250
    MAX_EPOCHS = 125
    LDA_OPTIM = 'online' # or 'em'
    LDA_SAMPLE = 0.05
    LDA_DECAY = 0.05
    LDA_OFFSET = 15
    
    # OUTPUT_FOLDER = "s3://data-science-263198015083/models/signal-media/2016-09-27/"
    OUTPUT_FOLDER = sys.argv[1]
    
    
    # import articles to textfile
    # ----------------------------
    signal_raw = sc.textFile("s3://data-science-263198015083/data/signal-media/")
    ag_raw = sc.textFile("s3://data-science-263198015083/data/ag-research/")
    ifa_raw = sc.textFile("s3://data-science-263198015083/crawler/articles/")
    
    
    # articles of dataframe
    # ----------------------------
    docs_signal = signal_raw.repartition(NUM_PARTITIONS) \
                            .mapPartitions(article_parser_signal)


    docs_ag = ag_raw.repartition(NUM_PARTITIONS) \
                    .mapPartitions(article_parser_ag)


    docs_ifa = ifa_raw.repartition(NUM_PARTITIONS) \
                      .mapPartitions(article_parser_ifa)


    # built-in regex tokenizer
    #    tokenizer = RegexTokenizer(inputCol="content", outputCol="words", pattern="\\W")
    #    articles_parsed = tokenizer.transform(articles)
    # ----------------------------
    articles_parsed = sc.union([docs_signal, docs_ag, docs_ifa]) \
                        .mapPartitions(custom_tokenizer) \
                        .toDF(["id", "dataset", "cat", 
                               "source", "url", "published", "words"])

    
    # custom length parsing
    # ----------------------------    
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
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
               .setSubsamplingRate(LDA_SAMPLE) \
               .setLearningDecay(LDA_DECAY) \
               .setLearningOffset(LDA_OFFSET) \
               .setTopicDistributionCol('topicDist')


    # ML
    # fit model to data
    # ----------------------------
    model = lda.fit(articles_model)
    vocab_size = model.vocabSize()


    # save model to s3
    # ----------------------------
    model.save(OUTPUT_FOLDER)
    # model2 = DistributedLDAModel.load(OUTPUT_FOLDER)
    

    # model fit diagnostics
    # ----------------------------
#    print("Size of Vocabulary: " + str(vocab_size))
#    print("Log Likelihood: " + str(model.logLikelihood(articles_model)))
#    print("Perplexity: " + str(model.logPerplexity(articles_model)))


    # save decoded topics
    # ----------------------------
    vv = vectorizer.vocabulary
    vv_rdd = sc.broadcast(vv)
    topics = model.describeTopics()
    topics_decode = topics.rdd \
                          .mapPartitions(topic_translate) \
                          .toDF(['topic','wordDist']) \
                          .coalesce(1)

    topics_decode.write.save(OUTPUT_FOLDER + 'topics/')
    
    
    # save document classification
    # ----------------------------
#    y_hat = model.transform(articles_model)
#    y_hat.write.save(OUTPUT_FOLDER + 'predictions/')
    

    # stop spark context
    # --------------------------------------------
    sc.stop()
