
# AWS SSH
# ---------------------------------------------
# ssh -i /Users/blakeaber/Desktop/IFA/BlakeKey.pem ec2-user@52.36.93.32
# scp -i /Users/blakeaber/Desktop/IFA/BlakeKey.pem chromedriver ec2-user@52.36.93.32:
# scp -i /Users/blakeaber/Desktop/IFA/BlakeKey.pem pip_python_packages.sh ec2-user@52.36.93.32:
# scp -i /Users/blakeaber/Desktop/IFA/BlakeKey.pem RSS_AWS.py ec2-user@52.36.93.32:

# Virtual Browser for Selenium on EC2 Box
# ---------------------------------------------
# sudo yum install Xvfb
scp -i /Users/blakeaber/Desktop/IFA/BlakeKey.pem google-chrome-stable_current_amd64.deb ec2-user@52.36.93.32:

# Backup on S3
# 2.5 Mb every 15 minutes
# 10 Mb per hour, 1Gb in 100 hours
# Need to backup roughly every 4 days
# ---------------------------------------------
# aws mv test.txt s3://data-science-263198015083/test.txt

# import packages
# ---------------------------------------------
import re
import csv
import time
import feedparser
from bs4 import BeautifulSoup
from selenium import webdriver

# capture current time for naming convention
# cron runs every 15 mins -> 900 secs
# ---------------------------------------------
run_time = round(time.time()/900,0)*900

# start up selenuim browser for bs4
# ---------------------------------------------
chromeOptions = webdriver.ChromeOptions()
prefs = {'profile.managed_default_content_settings.images':2,
         'profile.managed_default_content_settings.javascript': 2,
         'profile.managed_default_content_settings.css': 2}
chromeOptions.add_experimental_option('prefs',prefs)
browser = webdriver.Chrome('./chromedriver', chrome_options=chromeOptions)

# save runtime logs for analysis
# ---------------------------------------------
filename = './logs/runs.txt'
l = open(filename, "a")
logger = csv.writer(l, delimiter='\t')

# define function to remove html non-text
# ---------------------------------------------
def visible(element):
    if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
        return False
    elif re.match('<!--.*-->', str(element.encode('utf-8'))):
        return False
    return True

# function: get_soup()
# --------------------------------------------------------
def get_soup(url, browser):
    # browser = webdriver.Chrome('./chromedriver')
    browser.get(url)
    # browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    soup = BeautifulSoup(browser.page_source,'lxml')
    # browser.quit()
    return soup

# list all RSS feeds to scrape
# ---------------------------------------------
with open('feed_list.csv', 'rb') as f:
    feeds = csv.reader(f)
    available_feeds = list(feeds)

# available_feeds = [
# ('BBC-Latest', 'http://feeds.bbci.co.uk/news/rss.xml?edition=us'),
# ('BBC-World', 'http://feeds.bbci.co.uk/news/world/rss.xml?edition=us'),
# ('Buzzfeed-Latest', 'https://www.buzzfeed.com/community/justlaunched.xml'),
# ('Buzzfeed-LGBT', 'https://www.buzzfeed.com/lgbt.xml'),
# ('CBS-Health', 'http://www.cbsnews.com/latest/rss/health'),
# ('CBS-Latest', 'http://www.cbsnews.com/latest/rss/main'),
# ('CBS-Politics', 'http://www.cbsnews.com/latest/rss/politics'),
# ('CBS-US', 'http://www.cbsnews.com/latest/rss/us'),
# ('CNN-Health', 'http://rss.cnn.com/rss/cnn_health.rss'),
# ('CNN-Latest', 'http://rss.cnn.com/rss/cnn_latest.rss'),
# ('CNN-Politics', 'http://rss.cnn.com/rss/cnn_allpolitics.rss'),
# ('CNN-US', 'http://rss.cnn.com/rss/cnn_us.rss'),
# ('CourierJournal-Communities', 'http://rssfeeds.courier-journal.com/courierjournal/communities'),
# ('CourierJournal-Education', 'http://rssfeeds.courier-journal.com/courierjournal/education'),
# ('CourierJournal-Environment', 'http://rssfeeds.courier-journal.com/courierjournal/watchdogearth'),
# ('CourierJournal-Latest', 'http://rssfeeds.courier-journal.com/courierjournal/home'),
# ('CourierJournal-Politics', 'http://rssfeeds.courier-journal.com/courierjournal/politicsblog'),
# ('DNAinfo-Courts', 'https://www.dnainfo.com/new-york/topics/courts.rss'),
# ('DNAinfo-Crime', 'https://www.dnainfo.com/new-york/topics/crime-mayhem.rss'),
# ('DNAinfo-Education', 'https://www.dnainfo.com/new-york/topics/education.rss'),
# ('DNAinfo-Health', 'https://www.dnainfo.com/new-york/topics/health-wellness.rss'),
# ('DNAinfo-Parks&Rec', 'https://www.dnainfo.com/new-york/topics/parks-recreation.rss'),
# ('DNAinfo-Politics', 'https://www.dnainfo.com/new-york/topics/politics.rss'),
# ('DNAinfo-RealEstate', 'https://www.dnainfo.com/new-york/topics/real-estate.rss'),
# ('DNAinfo-Religion', 'https://www.dnainfo.com/new-york/topics/religion.rss'),
# ('DNAinfo-Transportation', 'https://www.dnainfo.com/new-york/topics/transportation.rss'),
# ('DNAinfo-Transportation', 'https://www.dnainfo.com/new-york/topics/transportation.rss'),
# ('FoxNews-Latest', 'http://feeds.foxnews.com/foxnews/latest'),
# ('FoxNews-Politics', 'http://feeds.foxnews.com/foxnews/politics'),
# ('HuffingtonPost-Black', 'http://www.huffingtonpost.com/feeds/verticals/black-voices/index.xml'),
# ('HuffingtonPost-Green', 'http://www.huffingtonpost.com/feeds/verticals/green/index.xml'),
# ('HuffingtonPost-Impact', 'http://www.huffingtonpost.com/feeds/verticals/impact/index.xml'),
# ('HuffingtonPost-Latest', 'http://www.huffingtonpost.com/feeds/index.xml'),
# ('HuffingtonPost-Latino', 'http://www.huffingtonpost.com/feeds/verticals/latino-voices/index.xml'),
# ('HuffingtonPost-LGBT', 'http://www.huffingtonpost.com/feeds/verticals/queer-voices/index.xml'),
# ('HuffingtonPost-Parents', 'http://www.huffingtonpost.com/feeds/verticals/parents/index.xml'),
# ('HuffingtonPost-Politics', 'http://www.huffingtonpost.com/feeds/verticals/politics/index.xml'),
# ('HuffingtonPost-Religion', 'http://www.huffingtonpost.com/feeds/verticals/religion/index.xml'),
# ('HuffingtonPost-Women', 'http://www.huffingtonpost.com/feeds/verticals/women/index.xml'),
# ('NYT-Arts', 'http://rss.nytimes.com/services/xml/rss/nyt/Arts.xml')
# ('NYT-Education', 'http://rss.nytimes.com/services/xml/rss/nyt/Education.xml'),
# ('NYT-Health', 'http://rss.nytimes.com/services/xml/rss/nyt/Health.xml'),
# ('NYT-Local', 'http://rss.nytimes.com/services/xml/rss/nyt/NYRegion.xml'),
# ('NYT-Nutrition', 'http://rss.nytimes.com/services/xml/rss/nyt/Nutrition.xml'),
# ('NYT-Politics', 'http://rss.nytimes.com/services/xml/rss/nyt/Politics.xml'),
# ('NYT-Research', 'http://rss.nytimes.com/services/xml/rss/nyt/Research.xml'),
# ('NYT-Views', 'http://rss.nytimes.com/services/xml/rss/nyt/Views.xml'),
# ('SFGate-Latest', 'http://www.sfgate.com/bayarea/feed/Bay-Area-News-429.php'),
# ('SFGate-LGBT', 'http://www.sfgate.com/rss/feed/Gay-Lesbian-601.php'),
# ('SFGate-Opinion', 'http://www.sfgate.com/rss/feed/Opinion-RSS-Feed-437.php'),
# ('SFGate-Parents', 'http://www.sfgate.com/rss/feed/The-Mommy-Files-594.php'),
# ('SFGate-RealEstate', 'http://www.sfgate.com/rss/feed/Real-Estate-News-RSS-Feed-444.php'),
# ('Yahoo-Latest', 'http://news.yahoo.com/rss/'),
# ]

# for each link in RSS list
# ---------------------------------------------
for current_feed in available_feeds:

    # performance tracking variables
    # ---------------------------------------------
    article_execution_timing = []

    # performance reporting: start feed
    # ---------------------------------------------
    start_feed_timer = time.time()

    # save variables for later
    # ---------------------------------------------
    feed_name = current_feed[0]
    feed_link = current_feed[1]

    # open feed file for saving results
    # ---------------------------------------------
    filename = './data/' + feed_name + '.txt'
    f = open(filename, "a")
    writer = csv.writer(f, delimiter='\t')

    # parse the latest RSS feed
    # ---------------------------------------------
    d = feedparser.parse(feed_link)

    # available fields from feedparser
    # ---------------------------------------------
    # d.entries[0].title
    # d.entries[0].link
    # d.entries[0].description
    # d.entries[0].published
    # d.entries[0].published_parsed
    # d.entries[0].id

    # save all article links from last 15 mins
    # ---------------------------------------------
    new_article_links = [(x.id, x.link, time.mktime(x.published_parsed)) for x in d.entries \
                         if (('id' in x) and ('published_parsed' in x) and 
                             (run_time - time.mktime(x.published_parsed) <= 900))]

    # for each new article in the feed
    # ---------------------------------------------
    for article in new_article_links:

        # performance reporting: start article
        # ---------------------------------------------
        start_article_timer = time.time()

        # save variables for later
        # ---------------------------------------------
        article_id = article[0]
        article_link = article[1]
        publish_time = int(article[2])

        # pull and parse the text using BeautifulSoup
        # ---------------------------------------------
        soup = get_soup(article_link, browser)
        data = soup.findAll(text=True)
 
        # filter out non-text html (using UDF above)
        # ---------------------------------------------
        clean_article_text = [x.strip().replace('\n', ' ') for x in filter(visible, data)]
        clean_article_text = ' '.join(clean_article_text).encode("utf-8")

        # output final results
        # ---------------------------------------------
        writer.writerow([run_time, feed_name, feed_link, article_id, article_link, 
                         publish_time, clean_article_text])

        # performance reporting: end article
        # ---------------------------------------------
        end_article_timer = time.time()
        article_execution_timing.append(end_article_timer - start_article_timer)

        # slow down to "human" speed
        # ---------------------------------------------
        time.sleep(2)

    # close feed file
    # ---------------------------------------------
    f.close()

    # performance reporting: article stats, current feed
    # ---------------------------------------------
    article_count = len(article_execution_timing)
    if article_count != 0:
        article_avg = float(sum(article_execution_timing)) / \
                      float(len(article_execution_timing))
    else:
        article_avg = 0

    # performance reporting: logging
    # ---------------------------------------------
    end_feed_timer = time.time()
    feed_time = end_feed_timer - start_feed_timer
    logger.writerow([int(run_time), feed_name, feed_time, 
                     article_count, article_avg])

# shutdown selenuim browser for bs4
# ---------------------------------------------
browser.close()

# close runtime logs
# ---------------------------------------------
f.close()