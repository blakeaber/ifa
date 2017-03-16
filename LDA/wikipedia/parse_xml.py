
# NEED TO REFORMAT THESE FILES SO THEY ARE ONE LINE PER XML
# REMOVE ALL NEWLINE, REPLACE DOC/NEWSITEM TAGS WITH EXTRA NEWLINE
# REMOVE XML HEADERS FROM RCV1



# this is to grab the TREC Reuters dataset (1 yr, 2008, 1,613,707 total articles, 540,177 corrupt)
# ------------------------------------------------------------------------------------------------
--packages com.databricks:spark-xml_2.10:0.4.1

df = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='doc').load('s3://data-science-263198015083/data/wikipedia-110116/clean/')





    
    

df.filter(df._corrupt_record.isNull()).show(10)



# this is to grab the RCV1 Reuters dataset (1 yr, 2008, 1.5M total articles, 500k corrupt)
# ------------------------------------------------------------------------------------------------
df2 = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='newsitem').load('s3://data-science-263198015083/data/reuters-rc1/all.xml.gz')