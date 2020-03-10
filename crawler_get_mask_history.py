#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 10 10:56:38 2020

@author: RichC
"""

import schedule
import time

import sys
import os
import datetime
import requests
import json
from urllib import request
from bs4 import BeautifulSoup

datetimeFormat = '%Y-%m-%d %H:%M:%S'
start_date = '2020_02_06'

log_foldername = 'crawler_mask_history_log'
log_filename = 'crawler_mask_history_log.txt'

if 'usb' in sys.argv:
    log_foldername = '/Volumes/32G/mask_record/crawler_mask_history_log'
else:
    log_foldername = "crawler_mask_history_log"

def ifLineInFile(target_line):
    
    result = False
    
    try:
        with open(os.path.join(log_foldername, log_filename), 'r') as f:
            lines = f.readlines()
            
            for line in lines:
                
                if target_line in line:
                    result = True
                    break
    except:
        result = False
    
    return result


def saveFileFromURl(foldername, filename):

    time_proc_1 = datetime.datetime.now()
    
    #download_url = https://raw.githubusercontent.com/apan1121/taiwan_mask/gh-pages/log/history/2020_03_10/01_03_41.log
    download_url = 'https://raw.githubusercontent.com/apan1121/taiwan_mask/gh-pages/log/history/{0}/{1}'.format(foldername, filename)
    #print(">>>", download_url)
            
    operUrl = request.urlopen(download_url)

    if(operUrl.getcode()==200):

        data = operUrl.read()
        jsonData = json.loads(data)
        
        try:
            path = '{0}/{1}'.format(log_foldername, foldername)
            
            if not os.path.exists(path):
                os.makedirs(path)
                
            file_name = foldername + '_' + filename
            
            ### Save URL data into file
            with open(os.path.join(path, file_name), 'w', encoding='utf-8') as f:
                json.dump(jsonData, f, ensure_ascii=False, indent=4)
                
            ### Update file-processd-history log
            with open(os.path.join(log_foldername, log_filename), 'a', encoding='utf-8') as f:
                f.writelines(file_name + '\n')

            time_proc_2 = datetime.datetime.now()            

            if 'debug' in sys.argv:
                print(">>> Processed file <{0}> Esc time {1}".format(file_name, time_proc_2 - time_proc_1))
            
        except Exception as e:
            print(">>> Exception", e)

def job():

    time_1 = datetime.datetime.now()
    print ("\r\nSTART:", time_1.strftime(datetimeFormat))

    day = datetime.datetime.strptime(start_date, '%Y_%m_%d')

    count = 0
    while day < datetime.datetime.now():
        
        if count >= 0: 
        
            print('[{0}]'.format(day.strftime('%Y_%m_%d')))

            time_proc_day_start = datetime.datetime.now()
            
            #req_path = 'https://github.com/apan1121/taiwan_mask/tree/gh-pages/log/history/2020_03_10'
            req_path = 'https://github.com/apan1121/taiwan_mask/tree/gh-pages/log/history/{0}'.format(day.strftime('%Y_%m_%d'))
            
            req = requests.get(req_path)
            
            if req.status_code == requests.codes.ok:
                
                soup = BeautifulSoup(req.text, 'html.parser')
                
                results = soup.find_all('a', class_='js-navigation-open')
                
                for file_count, res in enumerate(results):

                    if '.log' in res.text:
                        
                        targetd_line = day.strftime('%Y_%m_%d') + '_' + res.text
                        if ifLineInFile(targetd_line):
                            continue
    
                        #print(res.text)
                        saveFileFromURl(day.strftime('%Y_%m_%d'), res.text)

                        if 'debug' not in sys.argv:
                            if file_count%10 == 0:
                                print(">>> Processed", file_count)
            
            time_proc_day_end = datetime.datetime.now()
            print (">>> Processed day Esc", time_proc_day_end - time_proc_day_start)

        count = count + 1
        day = day + datetime.timedelta(days=1)
        
    time_2 = datetime.datetime.now()
    print ("END:", time_2.strftime(datetimeFormat))

    time_diff = time_2 - time_1
    print("Total Job time Esc:", time_diff)
    print("\r\n")
        
job()

schedule.every().day.at("04:30").do(job)

while True:

    schedule.run_pending()
    time.sleep(1)
            