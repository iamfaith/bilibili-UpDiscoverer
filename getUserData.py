#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
填好user\passwd\db即可使用
爬取所有level6的UP存入指定数据库中
'''
__author__ = 'kasora'

import requests
import json
import time,queue
import threading
import pymysql
import sys
#taskqueue为任务队列
taskqueue = queue.Queue()
#flag为备用
flag = True
#jump为跳跃值
jump = 1000;

url = "http://space.bilibili.com/ajax/member/GetInfo"
head = {'Referer':'http://space.bilibili.com'}

user = ""
passwd = ""
db =  ""

def getdata():
    #连接数据库
    conn = pymysql.connect(host="localhost",user=user,passwd=passwd,db=db,use_unicode=True, charset="utf8")
    cur = conn.cursor()
    #任务队列取空则终止
    while(flag and not taskqueue.empty()):             
        #从队列获取起始值
        x = taskqueue.get()
        #设定爬虫爬取的范围
        lid = x*jump
        #获取lid-lid+jump范围的数据
        for i in range(lid,lid+jump):
            #设定参数
            params = {"mid": i}
            try:
                r = requests.post(url,data=params,headers=head)
                info = json.loads(r.text)
                #通过分析数据包获取需要的信息
                level = info['data']['level_info']['current_level']
                name = info['data']['name']
                uid = info['data']['mid']                
                if(level==6):
                    #入库
                    cur.execute('insert into user (uid, uname ,ulevel) values (%s, %s, %s)', [uid,name,level])
                    conn.commit()
            except:
                pass
    cur.close()#关闭指针对象 
    conn.close()#关闭数据库连接对象

if __name__=='__main__':
    #动态设定爬取区间
    args = sys.argv
    datastart = 0
    dataend = 45000000//jump
    if(len(args)==2):
        dataend = args[1]//jump
    if(len(args)==3):
        datastart = args[1]//jump
        dataend = args[2]//jump
    for i in range(datastart,dataend):
        taskqueue.put(i)
    #开启线程进行爬取
    for i in range(30):
        sthread = threading.Thread(target=getdata)
        sthread.start()
    #每30秒打印当前爬取进度
    while(True):
        conn = pymysql.connect(host="localhost",user=user,passwd=passwd,db=db,use_unicode=True, charset="utf8")
        cur = conn.cursor()
        cur.execute("select count(*) from user;")
        res = cur.fetchall()
        print(res[0][0])
        print(taskqueue.qsize())
        time.sleep(30)
    
