import json
import queue
import logging
import sqlite3
import requests
import threading
import multiprocessing
import os.path as path
from multiprocessing.process import current_process


logging.basicConfig(level=logging.INFO,format='[%(processName)s] [%(threadName)s]:  %(message)s')

class FLICKR_USER(multiprocessing.Process):
    def __init__(self,username:str ,thread:int):
        multiprocessing.Process.__init__(self)
        current_process().name = username
        self.Init(username,thread)

    def run(self):
        self.MediaThread()
        self.ActiveStat()
        self.MediaDb()

    def Init(self, username, thread):
        self.media = []
        self.done = False
        self.userhead = None
        self.username = username
        self.configuration = None
        
        self.jobs = queue.Queue()
        self.dblock = threading.Lock()
        self.dbin = threading.Lock()
        self.thread_lock = threading.Semaphore(thread)
        
        self.Conf()
        self.csrf = self.configuration['csrf']
        self.api_key = self.configuration['key']
        self.mode = self.configuration['mode']
        self.cookie = self.configuration['cookie']
        
        self.db = path.join(path.dirname(__file__),"flickr.db")
        self.db_connection = sqlite3.connect(self.db)
        self.dbc = self.db_connection.cursor()

        self.ScrapUserDb(username)
        self.MakeQueue()
        
    def Conf(self):
        conf = open("conf.txt","r",encoding="utf-8")
        self.configuration = eval(conf.read())
        conf.close()
        logging.info("Configuration loaded successfuly")

    def MakeQueue(self):
        _, _, page_n, _ = self.userhead[self.mode]
        pages = [x for x in range(1, page_n+1)]
        for item in pages:
            self.jobs.put(item)
        logging.info("Number of pages in Queue: %i" % self.jobs.qsize())
        logging.info("Queue done successfuly")
    
    def ScrapUserDb(self,user_id):
        url = "https://api.flickr.com/services/rest?per_page=0&page=0&get_user_info=1&user_id=%s&method=flickr.people.getPhotos&csrf=%s&api_key=%s&format=json&nojsoncallback=1"%(str(user_id),self.csrf,self.api_key)
        query = requests.get(url, headers=self.cookie)
        query_response = query.json()
        if query_response['stat'] == "ok":
            nsid = query_response['user']['nsid']
            path_alias = query_response['user']['path_alias']
            username = query_response['user']['username']
            is_pro = query_response['user']['ispro']
            is_add_free = query_response['user']['is_ad_free']
            is_deleted = query_response['user']['is_deleted']
            date_created = query_response['user']['datecreate']
            user_photos_number = int(query_response['photos']['total'])
            total = self.ReqNum(user_photos_number)
            logging.info("UserData finished successfuly")
        else:
            error = query_response['message']
            logging.info("UserData error: %s" % error)

        fav_url = "https://api.flickr.com/services/rest?per_page=0&page=0&get_user_info=1&user_id=%s&method=flickr.favorites.getList&csrf=%s&api_key=%s&format=json&nojsoncallback=1"%(str(nsid),self.csrf,self.api_key)
        fav_query = requests.get(fav_url, headers=self.cookie)
        fav_query_response = fav_query.json()
        if fav_query_response['stat']  == "ok":
            fav_user_photos_number = int(fav_query_response['photos']['total'])
            fav_count, fav_page = fav_user_photos_number, self.ReqNum(fav_user_photos_number)
            logging.info("UserFav finished successfuly")
        else:
            error = fav_query_response['message']
            logging.info("UserFav error: %s" % error)

        user_data = (username,path_alias,nsid,user_photos_number,total,fav_count,fav_page,is_pro,is_add_free,is_deleted,date_created)
        exist_user = self.dbc.execute("select * from USER where nsid = ?",(nsid,)).fetchall()
        logging.info("Start checking user existance")
        if len(exist_user) == 0:
            self.dblock.acquire()
            self.dbc.execute("insert into USER values (?,?,?,?,?,?,?,?,?,?,?)",user_data)
            self.db_connection.commit()
            self.dblock.release()
            logging.info("User <%s> added to database" % user_id)
        
        self.userhead = {
            "pic": [nsid,user_photos_number,total,"flickr.people.getPhotos"],
            "fav":[nsid,fav_count,fav_page,"flickr.favorites.getList"]
            }
        logging.info("User is ready")

    def MediaLinks(self, page):
        try:
            self.thread_lock.acquire()
            user_id, _, _, mode = self.userhead[self.mode]
            url = "https://api.flickr.com/services/rest?per_page=500&page=%s&user_id=%s&method=%s&csrf=%s&api_key=%s&format=json&nojsoncallback=1"%(str(page),str(user_id),mode,self.csrf,self.api_key)
            logging.info("Start download from page <%i>" % page)
            query = requests.get(url, headers= self.cookie, timeout=20)
            resonse = query.json()
            if resonse['stat'] == "ok":
                media = resonse['photos']['photo']
                base_url = 'http://c1.staticflickr.com/'
                for item in media:
                    media_url = base_url + str(item['farm']) + "/" + item['server'] + "/" + item['id'] + "_" + item['secret'] + "_b_d.jpg"
                    owner = item['owner']
                    public = item['ispublic']
                    title = item['title']
                    safe = item['safe']
                    meia_data = {"owner": owner, "title": title, "public": public, "safe": safe, "download_link": media_url}
                    self.media.append(meia_data)
            else:
                logging.info("MediaPage error: %s, %s" % (resonse['message'], resonse['message']['exception']))
                self.jobs.put(page)
        except Exception as e:
            self.jobs.put(page)
            logging.info("MediaPage exception: %s" % e)
        finally:
            self.thread_lock.release()

    def MediaThread(self):
        while not self.jobs.empty():
            page = self.jobs.get()
            threading.Thread(target=self.MediaLinks, args=(page,)).start()

    def MediaDb(self):
        self.dblock.acquire()
        self.dbin.acquire()
        for item in self.media:
            self.dbc.execute("insert into MEDIA values (?,?,?,?,?,?)",(item['owner'],item['title'],item['public'],item['safe'],item['download_link'],0))
        self.db_connection.commit()
        self.dblock.release()
        self.dbin.release()
        logging.info("Media: %i inserted successfuly" % len(self.media))
    
    def ActiveStat(self):
        self.dbin.acquire()
        while not self.done:
            if threading.active_count() == 1:
                self.done = True
                self.dbin.release()

    @staticmethod
    def ReqNum(number:int):
        full = number // 500
        spare = number % 500
        if spare > 1:
            spare = 1
        else:
            spare = 0
        total = full + spare
        return total

def extract():
    db = path.join(path.dirname(__file__),"flickr.db")
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute("select distinct(link) from MEDIA where extract = 0")
    links = cur.fetchall()
    f = open("gen_link.txt","a",encoding="utf-8")
    for item in links:
        f.write(item[0]+"\n")
    f.close()
    cur.execute("update MEDIA set extract = 1 where extract = 0 ")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    for user in ['user_name']:
        FLICKR_USER(user,20).run()