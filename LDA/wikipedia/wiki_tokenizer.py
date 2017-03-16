
r = sc.textFile('s3://data-science-263198015083/data/wikipedia-110116/clean/').repartition(500)

r.getNumPartitions()

def parse_row(df):
    # import packages
    # ---------------------------------------------
    import re, string
    from nltk.tokenize import RegexpTokenizer
    for line in df:
        try:
            url, id, title, tags, text = line.split('\t')
            # tokenize articles into list of words
            # ---------------------------------------------
            tokenizer = RegexpTokenizer('\w+|\S+')
            punct = set(string.punctuation)
            words_only = re.sub(r'\W+ ', ' ', text)
            tokens = [''.join([ch for ch in x if ch not in punct]) for x in tokenizer.tokenize(words_only)]
            alpha_only = [x for x in tokens if not any(c.isdigit() for c in x)]
            clean_text = [i.lower() for i in alpha_only if len(i) >= 3 and len(i) <= 20]
            yield url, id, title, tags, clean_text
        except:
            pass

df = r.mapPartitions(parse_row).toDF(['url','id','title','tags','words'])

df.write.parquet('s3://data-science-263198015083/data/wikipedia-110116/lda/')


