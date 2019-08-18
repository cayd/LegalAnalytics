import urllib.request
from bs4 import BeautifulSoup
import PyPDF2
import json
import re
from sanitize_text import sanitize

#find the opinions on this particular page  
def scrape_year(url):
    top_level = "https://law.justia.com"
    request = urllib.request.Request(url)
    try:
        html = urllib.request.urlopen(request).read()
    except:
        print("fail2", url)
    soup = BeautifulSoup(html,'html.parser')
    #look for a next link now
    extracted_records = []
    next_page_records = None
    try:
        next_page = soup.find("span", class_="next pagination page")
        print("next page is ", next_page)
        next_link = next_page.find("a")
        print("next link", next_link)
        next_page_records = scrape_year(top_level + next_link['href'])
    except:
        pass
        
    links = soup.find_all("a",class_="case-name")

    #from each link extract the text of link and the link itself
    #List to store a dict of the data we extracted
    for link in links:
        title = link.text
        url = link['href']
        if not url.startswith('http'):
            url = top_level+url 
        record = {
            'title':title[1:],
            'url':url
        }
        if "In re" in record['title']:
            #print("skipping ", title)
            continue
        extracted_records.append(record)

    match_count = 0
    for record in extracted_records:
        request = urllib.request.Request(record['url'])
        try:
            html = urllib.request.urlopen(request).read()
        except:
            print("fail1", record['url'])
        soup = BeautifulSoup(html,'html.parser')
        case_decision = soup.find("noframes")
        case_summary = soup.find("div", attrs={'id':'diminished-text'})
        try:
            record['opinion'] = case_decision.text
        except:
            record['opinion'] = ""
            print("no opinion", record['title'])
        try:
            record['summary'] = case_summary.text
        except:
            record['summary'] = ""

        #parsed, uni, bi, tri = sanitize(record['opinion'])
        #record['opinion'] = parsed
        #record['opinion_unigrams'] = uni
        #record['opinion_bigrams'] = bi
        #record['opinion_trigrams'] = tri
        #parsed, uni, bi, tri = sanitize(record['opinion'])
        #record['summary'] = parsed
        #record['summary_unigrams'] = uni
        #record['summary_bigrams'] = bi
        #record['summary_trigrams'] = tri

    if next_page_records is None:
        print("returning from base case")
        if extracted_records is None:
            print("none here")
        else:
            print("records found")
        return extracted_records
    
    print("returning from recursion")
    if extracted_records is None:
        print("extracted_records is None")
    else:
        print("extracted_records is not None")
    if next_page_records is None:
        print("next_page_records is None")
    else:
        print("next_page_records is not None")
        
    extracted_records.extend(next_page_records)
    #if ret_ is None:
    #    print("ret_ is None")
    #else:
    #    print("ret_ is not None")

    print(len(extracted_records))
    print(len(next_page_records))
    #print(len(ret_))
    return extracted_records #.append(next_page_records)


def get_years(url):
    request = urllib.request.Request(url)
    html = urllib.request.urlopen(request).read()

    soup = BeautifulSoup(html,'html.parser')
    main_table = soup.find("ul",attrs={'class':'list-columns list-columns-three list-no-styles'})

    links = main_table.find_all("a")
     
    top_level = "https://law.justia.com"
    #from each link extract the text of link and the link itself
    #List to store a dict of the data we extracted
    year_urls = []
    for link in links:
        title = link.text
        url = link['href']
        if not url.startswith('http'):
            url = top_level+url        
        year_urls.append(url)
    
    return year_urls

def find_opinions(url):
    ret = []
    year_urls = get_years(url)
    c = 2002
    for year_url in year_urls:
        print("year url", year_url)
        year_results = scrape_year(year_url)
        if year_results is None:
            print("results from the year:", year_results)
        ret.append(year_results)
        #with open('appeals_court_opinions_' + str(c) + '.json', 'w') as outfile:
        #    json.dump(year_results, outfile, indent=4)
        c += 1
    return ret


def main():
    print("Imports completed successfully")
    
    ca_supreme_court_url = "https://law.justia.com/cases/california/supreme-court/"
    supreme_court_opinions = find_opinions(ca_supreme_court_url)
    with open('supreme_court_opinions.json', 'w') as outfile:
        json.dump(supreme_court_opinions, outfile, indent=4)
            
    #ca_appeals_court_url = "https://law.justia.com/cases/california/court-of-appeal/"
    #appeals_court_opinions = find_opinions(ca_appeals_court_url)
    #with open('appeals_court_opinions.json', 'w') as outfile:
    #    json.dump(appeals_court_opinions, outfile, indent=4)

    #print("Scraping completed successfully")
    
    
if __name__ == "__main__":
    main()
