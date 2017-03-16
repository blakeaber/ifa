
# AWS SSH
# ---------------------------------------------
# scp -i /Users/blakeaber/Desktop/IFA/BlakeKey.pem RSS_AWS.py ec2-user@52.36.93.32:


# import packages
# ---------------------------------------------
import re
import csv
import sys
import time
import feedparser


# DEFINE CRON RUN FREQUENCY
# capture current time for naming convention
# ---------------------------------------------
global CRON_FREQ = 900
global EXCL_HTML = ['script', 'style', '[document]', 'head', 'header', 'title', 'footer', 'img', 'a']
global USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'


# save execution logs for analysis
# ---------------------------------------------
filename = './logs/runs.txt'
l = open(filename, "a")
run_logger = csv.writer(l, delimiter='\t')


# save error logs for analysis
# ---------------------------------------------
filename = './logs/errors.txt'
l = open(filename, "a")
error_logger = csv.writer(l, delimiter='\t')


# function: visible()
# ---------------------------------------------
def visible(element):

    """ 
    This function reads all elements in the bs4 soup object and strips unwanted 
    sections defined within the EXCL_HTML global variable.
    """

    if element.parent.name in EXCL_HTML:
        return False
    elif re.match('<!--.*-->', str(element.encode('utf-8'))):
        return False
    return True


# function: get_soup()
# --------------------------------------------------------
def get_soup(url, USER_AGENT):

    """ 
    This function creates a Selenium webdriver that calls PhantomJS as a headless 
    browser on EC2. The webdriver shuts off javascript, images, etc for faster 
    processing of pages. The user agent can be passed programmatically.
    """

    # import packages
    # ---------------------------------------------
    from bs4 import BeautifulSoup
    from selenium import webdriver
    from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

    # set phantomJS settings
    # ---------------------------------------------
    dcap = dict(DesiredCapabilities.PHANTOMJS)
    dcap['phantomjs.page.settings.userAgent'] = USER_AGENT
    dcap['phantomjs.page.settings.loadImages'] = False
    dcap['phantomjs.page.settings.javascriptEnabled'] = False

    # instantiate driver and get URL
    # ---------------------------------------------
    driver = webdriver.PhantomJS(desired_capabilities=dcap)
    driver.set_window_size(1120, 550)
    driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
    driver.get(url)

    # pull and parse the text using BeautifulSoup
    # ---------------------------------------------
    soup = BeautifulSoup(driver.page_source,'lxml')

    # kill all script and style elements
    # ---------------------------------------------
    for script in soup(EXCL_HTML):
        script.extract()    # rip it out

    return soup

# list all RSS feeds to scrape
# ---------------------------------------------
with open('feed_list.csv', 'rb') as f:
    feeds = csv.reader(f)
    available_feeds = list(feeds)

# start tracking run time
# ---------------------------------------------
run_time = round(time.time() / CRON_FREQ, 0) * CRON_FREQ

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

    # save all article links from last CRON_FREQ mins
    # ---------------------------------------------
    new_article_links = [(x.id, x.link, time.mktime(x.published_parsed)) for x in d.entries \
                         if (('id' in x) and ('published_parsed' in x) and 
                             (run_time - time.mktime(x.published_parsed) <= CRON_FREQ))]

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
        try:
            soup = get_soup(article_link)
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
            time.sleep(3)

        # print (but ignore) errors
        # ---------------------------------------------        
        except:
            print("Feed: ", feed_name, "Article: ", article_link, "Unexpected error:", sys.exc_info()[0])
    
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
    run_logger.writerow([int(run_time), feed_name, feed_time, 
                         article_count, article_avg])

# close runtime logs
# ---------------------------------------------
f.close()