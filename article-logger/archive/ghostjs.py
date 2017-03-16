
# define url for testing
# ---------------------------------------------
url='http://www.nytimes.com/2016/09/18/upshot/at-last-a-little-breathing-room-in-the-financial-aid-gantlet.html?partner=rss&emc=rss&_r=0'

# import packages
# ---------------------------------------------
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

# define function to remove html non-text
# ---------------------------------------------
def visible(element):
    if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
        return False
    elif re.match('<!--.*-->', str(element.encode('utf-8'))):
        return False
    return True

# set phantomJS settings
# ---------------------------------------------
dcap = dict(DesiredCapabilities.PHANTOMJS)
dcap["phantomjs.page.settings.userAgent"] = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/53 (KHTML, like Gecko) Chrome/15.0.87"
dcap["phantomjs.page.settings.loadImages"] = False
dcap["phantomjs.page.settings.javascriptEnabled"] = False

# instantiate driver and get URL
# ---------------------------------------------
driver = webdriver.PhantomJS(desired_capabilities=dcap)
driver.set_window_size(1120, 550)
driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
driver.get(url)

# pull and parse the text using BeautifulSoup
# ---------------------------------------------
soup = BeautifulSoup(driver.page_source,'lxml')
data = soup.findAll(text=True)
 
# filter out non-text html
# ---------------------------------------------
clean_article_text = [x.strip().replace('\n', ' ') for x in filter(visible, data)]
clean_article_text = ' '.join(clean_article_text).encode("utf-8")

print(clean_article_text)

# driver.quit()

