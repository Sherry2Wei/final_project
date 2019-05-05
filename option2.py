import pandas as pd
from selenium import  webdriver
import requests
import time

##browser = webdriver.Chrome(r'D:\01software\chromedriver\chromedriver')
browser = webdriver.Firefox(executable_path=r'D:\01software\chromedriver\geckodriver')
firm_list = ['BOFA SECURITIES, INC.','BARCLAY INVESTMENTS LLC','BARCLAYS CAPITAL INC.'
,'JPMORGAN DISTRIBUTION SERVICES, INC.','GOLDMAN SACHS & CO. LLC'
,'MORGAN STANLEY & CO. LLC','CITIGROUP GLOBAL MARKETS INC.'
,'CREDIT SUISSE SECURITIES (USA) LLC','CREDIT SUISSE PRIME SECURITIES SERVICES (USA) LLC'
,'CREDIT SUISSE SECURITIES (USA) LLC','CREDITEX SECURITIES CORPORATION']


## for firm in firm_list:
firm = 'BOFA SECURITIES, INC.'
def download_misconduct(firm):
    browser.get('https://brokercheck.finra.org/')
    browser.find_element_by_xpath('//md-tabs-wrapper/md-tabs-canvas/md-pagination-wrapper/md-tab-item[3]/div[1]').click()
    firm_name = browser.find_element_by_id('firmtab-input')
    firm_name.send_keys(firm)
    browser.find_element_by_xpath('//md-tab-content[3]/div/form/div/div[1]/button').click()
    time.sleep(2)
    CRD = browser.find_element_by_xpath('//bc-bio-geo-section/div/div[1]/div[3]/span').text
    file_No = CRD.split('/')[0]
    firm_url = 'https://files.brokercheck.finra.org/firm/firm_{}.pdf'.format(file_No)
    browser.get(firm_url)
    browser.find_element_by_id('download').click()

download_misconduct(firm)

for firm in firm_list:
    time.sleep(1)
    try:
        download_misconduct(firm)
    except:
        continue

