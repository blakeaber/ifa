
# import packages
# ------------------------------------------------
import re, csv, os
from bs4 import BeautifulSoup

# keep track of text
# ------------------------------------------------
running_text = []
text_attributes = []

os.chdir('parsed')

# run code for each file in directory
# ------------------------------------------------
for filename in os.listdir(os.getcwd()):

    # unzip the file for analysis
    # ------------------------------------------------
    os.system('bunzip2 ' + filename)

    # read lines in file, do something
    # ------------------------------------------------
    with open(filename[:-4]) as f:
    
        # write to another file
        # ------------------------------------------------
        with open(filename[:-4] + '_clean', 'w') as fout:
            writer = csv.writer(fout, delimiter='\t')
    
            # read through all lines
            # ------------------------------------------------
            for line in f:
                running_text.append(line.strip())
        
                # parse xml once element is complete
                # ------------------------------------------------
                if line.startswith('</doc>'):
        
                    joined = ' '.join(running_text)
                    stripped = re.sub(r'[^\x00-\x7F\t]+',' ', joined)
    
                    # create the bs4 object for parsing
                    # ------------------------------------------------                
                    soup = BeautifulSoup(stripped, 'lxml')
    
                    # strip non-html tags and save attributes
                    # ------------------------------------------------
                    for link in soup.findAll('a'):
                        text_attributes.append((link.get('href'), link.text.encode('utf-8')))
        
                    # get raw text
                    # ------------------------------------------------
                    raw_text = soup.get_text()
    
                    # save details and reset for next article
                    # ------------------------------------------------
                    article_url, article_id, article_title = soup.doc['url'], soup.doc['id'], soup.doc['title']
        
                    # save results to file
                    # ------------------------------------------------
                    writer.writerow([article_url, article_id, article_title.encode('utf-8'), \
                                     set(text_attributes), raw_text.encode('utf-8')])
    
                    # clear results for next article
                    # ------------------------------------------------
                    running_text = []
                    text_attributes = []
                
        # zip the file once complete
        # ------------------------------------------------
        os.system('gzip ' + filename[:-4] + '_clean')
        os.remove(filename[:-4])
