#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
填好user\passwd\db即可使用
分析数据，存入表中
'''
__author__ = 'kasora'

import requests
import json
import pymysql
from bs4 import BeautifulSoup
import re
import logging
logging.basicConfig(level=logging.WARNING)

getSubmitVedioUrl = "http://space.bilibili.com/ajax/member/getSubmitVideos"
vedioBaseUrl = "http://www.bilibili.com/video/av"

user = ""
passwd = ""
db =  ""
    

def gettendency():
    #连接数据库
    conn = pymysql.connect(host="localhost",user=user,passwd=passwd,db=db,use_unicode=True, charset="utf8")
    cur = conn.cursor()
    #获取所有已收录的用户
    cur.execute("select * from user order by uid;")
    allUserRes = cur.fetchall()
    #分析
    for x in allUserRes:
        #获取用户投稿信息
        uid = x[0]
        params = {"mid":uid,
                  "pagesize":1000000}
        r = requests.get(getSubmitVedioUrl,params = params)
        jsonSubmitInfo = json.loads(r.text)
        #防止用户无投稿导致的报错
        try:
            if('vlist' not in jsonSubmitInfo['data']):
                continue
        except:
            #投稿数超过8K就会获取失败。。共有4位菊苣达成。
            logging.warning(str(uid)+"号录入失败")
            continue
        
        #获取投稿列表和分区计数
        vedioList = jsonSubmitInfo['data']['vlist']
        vedioNumber = jsonSubmitInfo['data']['count']

        devideUpSubmit = dict()
        devideUpSubmitNumber = dict()
        #处理投稿稿件
        for vedio in vedioList:
            vedioTypeId = vedio['typeid']
            vedioId = str(vedio['aid'])
            vedioPlayNumber = vedio['play']
            #查询该类型id是否已经对应上了一个类型名称
            cur.execute("select * from vediotype where typeid = %s;",[vedioTypeId])
            vedioRes = cur.fetchall()
            #未匹配则将其入库                
            if(len(vedioRes)==0):                    
                vedioUrl = vedioBaseUrl + vedioId
                r = requests.get(vedioUrl)
                logging.info(vedioUrl)
                soup = BeautifulSoup(r.text,"html.parser")
                vedioTitle = soup.title.string
                logging.info(vedioTitle)
                pattern = re.compile("_.*?_.*?_bilibili_")
                titleSplit = pattern.findall(vedioTitle)[0].split('_')
                cur.execute('insert into vediotype (typeid, typename ,fathertypename) values (%s,%s,%s)', [vedioTypeId,titleSplit[1],titleSplit[2]])
                conn.commit()
                logging.info(titleSplit[1]+'入库成功')                                
            #统计
            if(vedioTypeId in devideUpSubmit):
                devideUpSubmit[vedioTypeId] += vedioPlayNumber
                devideUpSubmitNumber[vedioTypeId] += 1
            else:
                devideUpSubmit[vedioTypeId] = vedioPlayNumber
                devideUpSubmitNumber[vedioTypeId] = 1
            logging.info("稿件"+vedioId+"处理完毕")
            
        #for vedio in vcount:
            #vtype = vedio['']
            
        #取各类稿件的平均点击数
        proTendency = dict()
        for k in devideUpSubmit:
            devideUpSubmit[k] = devideUpSubmit[k]//devideUpSubmitNumber[k]
        for k in devideUpSubmitNumber:
            if(devideUpSubmitNumber[k]>vedioNumber//4):
                proTendency[k]=devideUpSubmit[k]
        jsonstrDevideUpSubmit = json.dumps(devideUpSubmit)
        jsonstrDevideUpSubmitNumber = json.dumps(devideUpSubmitNumber)
        jsonstrProTendency = json.dumps(proTendency)
        #更新数据
        cur.execute('select uid,tendency,procount,protendency from usertendency where uid=%s;', [uid])
        oldres = cur.fetchall()            
        if(len(oldres)==0):                
            cur.execute('insert into usertendency (uid, tendency, procount, protendency) values (%s,%s,%s,%s);', [uid,jsonstrDevideUpSubmit,jsonstrDevideUpSubmitNumber,jsonstrProTendency])
            conn.commit()
            logging.info("用户"+str(uid)+"处理完毕")
        else:
            cur.execute('update usertendency set tendency=%s,procount=%s,protendency=%s where uid=%s;', [jsonstrDevideUpSubmit,jsonstrDevideUpSubmitNumber,jsonstrProTendency,uid])
            conn.commit()
            logging.info("用户"+str(uid)+"更新完毕")



    cur.close()#关闭指针对象 
    conn.close()#关闭数据库连接对象

if __name__=='__main__':
    gettendency()
