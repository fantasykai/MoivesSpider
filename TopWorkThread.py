#!/usr/bin/env python
#coding=utf-8

import threading
import requests
import time


from RequestModel import RequestModel
from TaskQueue import TaskQueue
from dytt8.dytt8Moive import dytt_Lastest

'''
    1)从电影详细信息页面【http://www.dytt8.net/html/gndy/dyzz/20170806/54695.html】中抓取目标内容
    2)将数据存储到数据库中
@Author monkey
@Date 2017-08-14
'''
class TopWorkThread(threading.Thread):

    NOT_EXIST = 0

    def __init__(self, queue, id):
        threading.Thread.__init__(self)
        self.queue = queue
        self.id = id



    def run(self):
        while not self.NOT_EXIST:
            # 队列为空, 结束
            if self.queue.empty():
                NOT_EXIST = 1
                self.queue.task_done()
                break

            try:
                url = self.queue.get()
                response = requests.get(url, headers=RequestModel.getHeaders(), proxies=RequestModel.getProxies(), timeout=3)
                print('Top 子线程 ' + str(self.id) + ' 请求【 ' + url + ' 】的结果： ' + str(response.status_code))

                # 需将电影天堂的页面的编码改为 GBK, 不然会出现乱码的情况
                response.encoding = 'GBK'

                if response.status_code != 200:
                    self.queue.put(url)
                    time.sleep(20)
                else :
                    tmpDir = dytt_Lastest.getMoiveInforms(response.text)
                    # TaskQueue.getContentQueue().put()
                time.sleep(5)

            except Exception as e:
                print(e)