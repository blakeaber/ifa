
# AWS SSH
# ---------------------------------------------
# ssh -i /Users/blakeaber/Desktop/IFA/BlakeKey.pem ec2-user@ec2-52-26-66-63.us-west-2.compute.amazonaws.com

# scp -i /Users/blakeaber/Desktop/IFA/BlakeKey.pem /Users/blakeaber/Desktop/IFA/article-logger/chromedriver ec2-user@ec2-52-26-66-63.us-west-2.compute.amazonaws.com/home/ec2-user/article-logger/chromedriver

# import packages
# ---------------------------------------------
import re
import csv
import time
import feedparser
from bs4 import BeautifulSoup
from selenium import webdriver

# capture current time for naming convention
# ---------------------------------------------
run_time = round(time.time()/3600,0)*3600

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
filename = './logs/' + str(int(run_time)) + '.txt'
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
available_feeds = [
('CNN-Politics', 'http://rss.cnn.com/rss/cnn_allpolitics.rss'),
('CNN-US', 'http://rss.cnn.com/rss/cnn_us.rss'),
('CNN-Health', 'http://rss.cnn.com/rss/cnn_health.rss'),
('CNN-Latest', 'http://rss.cnn.com/rss/cnn_latest.rss'),
('NYT-Local', 'http://rss.nytimes.com/services/xml/rss/nyt/NYRegion.xml'),
('NYT-Education', 'http://rss.nytimes.com/services/xml/rss/nyt/Education.xml'),
('NYT-Politics', 'http://rss.nytimes.com/services/xml/rss/nyt/Politics.xml'),
('NYT-Research', 'http://rss.nytimes.com/services/xml/rss/nyt/Research.xml'),
('NYT-Health', 'http://rss.nytimes.com/services/xml/rss/nyt/Health.xml'),
('NYT-Nutrition', 'http://rss.nytimes.com/services/xml/rss/nyt/Nutrition.xml'),
('NYT-Views', 'http://rss.nytimes.com/services/xml/rss/nyt/Views.xml'),
('NYT-Arts', 'http://rss.nytimes.com/services/xml/rss/nyt/Arts.xml')
]

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
    filename = './data/' + feed_name + '_' + str(int(run_time)) + '.txt'
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

    # save all article links from last 3 hours
    # 1 hour = 60*60 = 3600 seconds
    # PUBLISHED_PARSED DOES NOT EXIST - FAILS SCRIPT
    # ---------------------------------------------
    new_article_links = [(x.link, time.mktime(x.published_parsed)) for x in d.entries \
                         if (('published_parsed' in x) and 
                             (run_time - time.mktime(x.published_parsed) <= 3600))]

    # for each new article in the feed
    # ---------------------------------------------
    for article in new_article_links:

        # performance reporting: start article
        # ---------------------------------------------
        start_article_timer = time.time()

        # save variables for later
        # ---------------------------------------------
        article_link = article[0]
        publish_time = int(article[1])

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
        writer.writerow([run_time, feed_name, feed_link, article_link, 
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