#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  9 18:01:23 2020

@author: RichC
"""

import schedule
import time

def syncCVStoDB():
    
    import datetime
    import os
    import json
    import psycopg2
    import sys
    
    datetimeFormat = '%Y-%m-%d %H:%M:%S'
    time_1 = datetime.datetime.now()
    print ("\r\nSTART:", time_1.strftime(datetimeFormat))
    
    # Construct connection string
    
    host = ""
    user = ""
    password = ""
    dbname = "MaskRecord"
    port = "5432"
    db_table = "mask_record_dev"
    history_filename = 'process_history_dev.txt'
    
    print(">>> Connecting ...... <database>", dbname)
    conn_string = "host={0} user={1} dbname={2} password={3} port={4}".format(host, user, dbname, password, port)
    conn = psycopg2.connect(conn_string) 
    print(">>> Connection established on [table]." + db_table)

    cursor = conn.cursor()
    
    ### Get files in directory
    
    directory = '/Volumes/32G/mask_record/getMasks'
    file_count=0
    
    for filename in os.listdir(directory):
        
        if ifLineInFile(history_filename, filename):
            continue
        
        ### Start process selected file
        print("\r\nFile-{0}".format(file_count) + ": " + filename)
        if file_count >= 0:
            if filename.endswith(".json"):
                
                f = open(os.path.join(directory, filename))
                data = f.read()
                jsonData = json.loads(data)

                ### Insert to SQL
                row_count=0
                for i in jsonData["payload"]:
                    
                    if row_count >= 0:
                        
                        datetime_object = datetime.datetime.strptime(filename[5:20], '%Y%m%d_%H%M%S')
                        
                        sql = "INSERT INTO {0} (pharmacy_code, adult_mask, child_mask, updated) VALUES('{1}', {2}, {3}, TIMESTAMP '{4}')".format(db_table, i["code"], i["adult_count"], i["child_count"], datetime_object)
                        #print(sql)
                        cursor.execute(sql)
                        
                        if (row_count % 1000 == 0):
                            print(">>> Processed ", row_count)
                    
                        row_count = row_count + 1
                    
                print (">>> Total update rows", row_count)
        
                conn.commit()   #Commit SQL per file (about 6000 rows) in one commit

                ### Update file-processd-history log
                with open(history_filename, 'a', encoding='utf-8') as f:
                    f.writelines(filename + '\n')
                    
                file_count = file_count + 1
        else:
            break
        
    print (">>> Total process files", file_count)
    
    # Clean up
    cursor.close()
    conn.close()


def ifLineInFile(source_filename, target_line):
    result = False
    
    try:
        with open(source_filename, 'r') as f:
            lines = f.readlines()
            for line in lines:
                if target_line in line:
                    result = True
                    break
    except:
        result = False
    
    return result

syncCVStoDB()

schedule.every().hour.do(syncCVStoDB)

while True:

    if datetime.datetime.now().hour > 8 and datetime.datetime.now().hour < 22:
        schedule.run_pending()
        time.sleep(1)

