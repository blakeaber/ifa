
# import packages
# ------------------------------------------------
import re, csv
import xml.etree.ElementTree as ET

# keep track of text
# ------------------------------------------------
running_text = []
text_attributes = {}

# read lines in file, do something
# ------------------------------------------------
with open('/Users/blakeaber/Desktop/IFA/LDA/wikipedia/parsed/wiki_09') as f:
#with open('wiki_test.html') as f:

    # write to another file
    # ------------------------------------------------
    with open("wiki_clean.txt", "w") as fout:
        writer = csv.writer(fout, delimiter='\t')

        # read through all lines
        # ------------------------------------------------
        for line in f:
            running_text.append(line.strip())
    
            # parse xml once element is complete
            # ------------------------------------------------
            if line.startswith('</doc>'):
    
                joined = ' '.join(running_text).replace('<br>','').replace('</br>','') \
                                               .replace(' < ',' lt ').replace(' > ',' gt ')
                stripped = re.sub(r'[^\x00-\x7F\t]+',' ', joined)
                stripped = re.sub(r'&', 'and', stripped)
                print(stripped)
                root = ET.fromstring(stripped)

                # strip non-html tags and save attributes
                # ------------------------------------------------
                for child in root:
                    if child.tag != "a":
                        root.remove(child)
                    else:
                        text_attributes[child.attrib['href']] = child.text

                # save details and reset for next article
                # ------------------------------------------------
                article_details = root.attrib
                running_text = []
                text_attributes = {}
    
                # get raw text
                # ------------------------------------------------
                raw_text = ' '.join(list(root.itertext()))
    
                # save results to file
                # ------------------------------------------------
                writer.writerow([article_details, text_attributes, raw_text])
