#!/usr/bin/env python
# coding=utf-8


import sqlite3

from dytt8.dytt8Moive import dytt_Lastest
from model.TaskQueue import TaskQueue
from thread.FloorWorkThread import FloorWorkThread
from thread.TopWorkThread import TopWorkThread
from utils.Utils import Utils
from pymongo import MongoClient, uri_parser
from secret import mongo
from datetime import datetime

'''
    程序主入口
@Author monkey
@Date 2017-08-08
'''

# 截止到2018-02-01, 最新电影一共才有 169 个页面
LASTEST_MOIVE_TOTAL_SUM = 169  # 164

# 请求网络线程总数, 线程不要调太高, 不然会返回很多 400
THREAD_SUM = 2


def getmongodb():
    client = MongoClient(mongo['url'].split('?')[0])
    parsed = uri_parser.parse_uri(mongo['url'])
    return client[parsed['database']]


def startSpider():
    # 实例化对象

    # 获取【最新电影】有多少个页面
    LASTEST_MOIVE_TOTAL_SUM = dytt_Lastest.getMaxsize()
    print('【最新电影】一共  ' + str(LASTEST_MOIVE_TOTAL_SUM) + '  有个页面')
    dyttlastest = dytt_Lastest(LASTEST_MOIVE_TOTAL_SUM)
    floorlist = dyttlastest.getPageUrlList()

    floorQueue = TaskQueue.getFloorQueue()
    for item in floorlist:
        floorQueue.put(item, 3)

    # print(floorQueue.qsize())

    for i in range(THREAD_SUM):
        workthread = FloorWorkThread(floorQueue, i)
        workthread.start()

    while True:
        if TaskQueue.isFloorQueueEmpty():
            break
        else:
            pass

    for i in range(THREAD_SUM):
        workthread = TopWorkThread(TaskQueue.getMiddleQueue(), i)
        workthread.start()

    while True:
        if TaskQueue.isMiddleQueueEmpty():
            break
        else:
            pass

    insertData()


def insertData():
    db = getmongodb()

    count = 1

    while not TaskQueue.isContentQueueEmpty():
        item = TaskQueue.getContentQueue().get()
        item['_created'] = item['_updated'] = datetime.utcnow().replace(microsecond=0)
        db.lastest_moive.insert_one(item)
        print('插入第 ' + str(count) + ' 条数据成功')
        count = count + 1


if __name__ == '__main__':
    startSpider()
