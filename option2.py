import pandas as pd
from selenium import  webdriver
import requests
import time

##browser = webdriver.Chrome(r'D:\01software\chromedriver\chromedriver')
browser = webdriver.Firefox(executable_path=r'D:\01software\chromedriver\geckodriver')

browser.get('https://www.finra.org/about/firms-we-regulate')
browser.find_element_by_xpath('//div/article//div').text
link = [term.text for term in browser.find_elements_by_xpath('//div/article//div/span/a')][2:]

firm_list_combine = [term.text.split(',')[0] for term in browser.find_elements_by_xpath('//div/article//div/p')]

for letter in link:
    time.sleep(1)
    browser.find_element_by_link_text(letter).click()
    firm_list = [term.text.split(',')[0] for term in browser.find_elements_by_xpath('//div/article//div/p')]
    firm_list_combine.extend(firm_list)


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



for firm in firm_list_combine:
    time.sleep(1)
    try:
        download_misconduct(firm)
    except:
        continue

