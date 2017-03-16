

# function: visible()
# ---------------------------------------------
def visible(element):

    """ 
    This function reads all elements in the bs4 soup object and strips unwanted 
    sections defined within the EXCL_HTML global variable.
    """

    # strip tags
    # ---------------------------------------------
    if element.parent.name in EXCL_HTML:
        return False
    elif re.match('<!--.*-->', str(element.encode('utf-8'))):
        return False
    return True


# function: get_soup()
# --------------------------------------------------------
def get_soup(url):

    """ 
    This function creates a Selenium webdriver that calls PhantomJS as a headless 
    browser on EC2. The webdriver shuts off javascript, images, etc for faster 
    processing of pages. The user agent can be passed programmatically.
    """

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
        script.extract()

    return soup


# function: parse_article()
# --------------------------------------------------------
def parse_article(article, feed_name, feed_link, feed_writer, error_logger):

    """ 
    This function takes an article link from an RSS feed, pulls the HTML page,
    filters the page down to text, and saves the article along with the
    metadata to file. All articles are logged as complete or an error. Execution
    time is logged and returned if available after a successful run.
    """

    # performance reporting: start article
    # ---------------------------------------------
    start_article_timer = time.time()

    # save variables for later
    # ---------------------------------------------
    article_id = article[0]
    article_link = article[1]
    publish_time = int(article[2])

    try:

        # pull and parse the text using BeautifulSoup
        # ---------------------------------------------
        soup = get_soup(article_link)
        data = soup.findAll(text=True)
 
        # filter out non-text html (using UDF above)
        # ---------------------------------------------
        raw_text = [x.strip().lower() for x in filter(visible, data)]
        raw_text = ' '.join(raw_text)

        # tokenize articles into list of words
        # ---------------------------------------------
        tokenizer = RegexpTokenizer('\w+|\S+')
        html_stripper = re.compile(r'<.*?>')
        punct = set(string.punctuation)
        no_tags = html_stripper.sub('', raw_text)
        words_only = re.sub(r'\W+ ', ' ', no_tags)
        tokens = [''.join([ch for ch in x if ch not in punct]) for x in tokenizer.tokenize(words_only)]
        alpha_only = [x for x in tokens if not any(c.isdigit() for c in x)]
        long_alpha = [i for i in alpha_only if len(i) >= 3 and len(i) <= 20]
        clean_article_text = ' '.join(long_alpha)        

        # output final results
        # ---------------------------------------------
        feed_writer.writerow([RUN_ID, feed_name, feed_link, article_id, article_link, 
                              publish_time, clean_article_text])

        # performance reporting: end article
        # ---------------------------------------------
        end_article_timer = time.time()
        time.sleep(3)
        return (end_article_timer - start_article_timer)

    except:

        # save run errors to file
        # ---------------------------------------------
        error_message = sys.exc_info()[0]
        error_logger.writerow([feed_name, article_id, article_link, error_message])

        # performance reporting: end article
        # ---------------------------------------------
        end_article_timer = time.time()
        time.sleep(WAIT_TIME)
        return (end_article_timer - start_article_timer)


# function: parse_feed()
# --------------------------------------------------------
def parse_feed(current_feed, feed_writer, feed_logger, error_logger):

    """ 
    Parses each field pulled down by the feedparser package
    """

    # performance reporting: start feed
    # ---------------------------------------------
    start_feed_timer = time.time()

    # save variables for later
    # ---------------------------------------------
    feed_name = current_feed[0]
    feed_link = current_feed[1]

    # parse the latest RSS feed
    # ---------------------------------------------
    d = feedparser.parse(feed_link)

    # save all article links from last CRON_FREQ mins
    # ---------------------------------------------
    new_article_links = [(x.id, x.link, time.mktime(x.published_parsed)) 
                          for x in d.entries if (
                          ('id' in x) and ('published_parsed' in x) and 
                          (RUN_ID - time.mktime(x.published_parsed) <= CRON_FREQ))]

    # parse and save each article
    # performance reporting: execution times
    # ---------------------------------------------
    article_execution_timing = [parse_article(article, feed_name, feed_link, feed_writer, 
                                              error_logger) 
                                for article in new_article_links]

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
    feed_logger.writerow([int(RUN_ID), feed_name, feed_time, 
                          article_count, article_avg])


if __name__ == '__main__':

    # import packages
    # ---------------------------------------------
    import re, csv, sys, time, string, datetime, feedparser
    from nltk.tokenize import RegexpTokenizer
    from bs4 import BeautifulSoup
    from selenium import webdriver
    from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
    
    # define global variables for script
    # ---------------------------------------------
    global WAIT_TIME
    global CRON_FREQ
    global EXCL_HTML
    global USER_AGENT
    global RUN_ID

    WAIT_TIME = 3
    CRON_FREQ = 900
    EXCL_HTML = ['script', 'style', '[document]', 'head', 'header', 'title', 'footer', 'img', 'a']
    USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'
    RUN_ID = round(time.time() / CRON_FREQ, 0) * CRON_FREQ


    # open write and log files
    # ---------------------------------------------
    filename = './articles/articles.txt'
    f = open(filename, "a")
    feed_writer = csv.writer(f, delimiter='\t')
    
    filename = './logs/logs.txt'
    l = open(filename, "a")
    feed_logger = csv.writer(l, delimiter='\t')
    
    filename = './errors/errors.txt'
    e = open(filename, "a")
    error_logger = csv.writer(e, delimiter='\t')
    
    
    # scrape all RSS feeds
    # ---------------------------------------------
    with open('feed_list.csv', 'rb') as f:
        feeds = csv.reader(f)
        for current_feed in list(feeds):
            parse_feed(current_feed, feed_writer, feed_logger, error_logger) 
    
    
    # close feed, log and error files
    # ---------------------------------------------
    f.close()
    l.close()
    e.close()
    