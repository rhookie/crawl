import urllib2
from pymongo import MongoClient,errors
import random
import urlparse
import logging
from datetime import datetime , timedelta
import zlib 
import pickle 
from bson.binary import Binary
from PySide.QtWebKit import QWebView
from PySide.QtGui import QApplication
import sys 
from PySide.QtCore import QEventLoop,QTimer,QUrl
import time

logging.basicConfig(level=logging.DEBUG,format="[%(asctime)s] (%(threadName)s) %(message)s",)


class Download:
    def __init__(self,throttle=None,proxies=None,num_retries=2,headers=None,user_agent='wspa',cache=None):
        self.throttle=throttle 
        self.proxies=proxies 
        self.user_agent=user_agent
        self.cache=cache
        self.headers=headers
        self.num_retries=num_retries
    
    def __call__(self,url):
        results=None
        if self.cache:
            try:
                results=self.cache['url']
            except KeyError:
                pass 
            else:
                if self.num_retries>0 and 500<=results['code']<600:
                    results=None
        if results is None:
            results=self.download(url,self.num_retries)
            if self.cache:
                self.cache[url]=results
        return results['html']
    
    
    def download(self,url,num_retries=2):
        logging.debug("Downloading...%s",url)
        headers=self.headers or {}
        if self.user_agent:
            headers['User-agent']=self.user_agent
        request=urllib2.Request(url,headers=headers)
        opener=urllib2.ProxyHandler()
        proxy=random.choice(self.proxies) if self.proxies else None
        if proxy:
            param={urlparse.urlparse(url).scheme:proxy}
            opener.build_opener(param)
        try:
            response=opener.open(request)
            html=response.read()
            code=response.code 
        except Exception as e:
            html=""
            if hasattr(e,'code'):
                code=e.code 
                if num_retries>0 and 500<=code<600:
                    self.download(url, num_retries-1)
        return {'html':html,'code':code}
    
    
class MongoQueue:
    OUTSTANDING,PROCESSING,COMPLETED=range(3)
    def __init__(self,client=None,timeout=300):
        self.client=MongoClient("mongodb://localhost:27017/") if client is None else client
        self.db=self.client.datas 
        self.timeout=timeout
    
    def __nonzero__(self):
        record=self.db.coll.find_one({'status':{'$ne':self.COMPLETED}})
        return True if record else False
    
    def push(self,url):
        try:
            self.db.coll.insert({'_id':url,'status':self.OUTSTANDING})
        except errors.DuplicateKeyError:
            pass  
    
    def pop(self):
        record=self.db.coll.find_and_modify(query={'status':self.OUTSTANDING},update={'$set':{'status':self.PROCESSING,'timestamp':datetime.now()}})
        if record:
            return record['_id']
        else:
            self.repair()
            raise KeyError()    #This is details that show the last uncompleted url in mongodb
    
    def complete(self,url):
        self.db.coll.update({'_id':url},{'$set':{'status':self.COMPLETED}})
        
    def repair(self):
        
        record=self.db.coll.find_and_modify(query={'timstamp':{'$lt':datetime.now()-timedelta(seconds=self.timeout)},'status':{'$ne':self.COMPLETED}},
                                            update={'$set':{'status':self.OUTSTANDING}})
        if record:
            logging.debug('Released:%s',record['_id'])
            
class MongoCache:
    def __init__(self,client,expires=timedelta(days=10)):
        self.client=MongoClient("mongodb://localhost:27017/") if client is None else client 
        self.expires=expires 
        self.db=self.client.cachesa
    
    def __getitem__(self,url):
        rp=self.db.coll.find_one({'_id':url})
        if rp:
            return pickle.loads(zlib.decompress(rp['results']))
        else:
            raise KeyError(url,'does not exits')
    
    def __setitem__(self,url,results):
        result={'results':Binary(zlib.compress(pickle.dumps(results))),'timestamp':datetime.utcnow()}
        self.db.coll.update({'_id':url,'$set':result},upsert=True)
        
    def clear(self):
        self.db.coll.drop()


class BrowserRender(QWebView):
    def __init__(self,show=True):
        self.app=QApplication(sys.argv)
        QWebView.__init__(self)
        if show:
            self.show()
    
    def download(self,url,timeout=60):
        """wait for download to complete and return result"""
        loop=QEventLoop()
        timer=QTimer()
        timer.setSingleShot(True)
        timer.timeout.connect(loop.quit)
        self.loadFinished.connect(loop.quit)
        self.load(QUrl(url))
        timer.start(timeout*1000)
        loop.exec_()
        if timer.isActive():
            timer.stop()
            return self.html()
        else:
            print 'Request timed out:'+url 
    
    def html(self):
        """Shortcut to return the current HTML"""
        return self.page().mainFrame().toHtml()
    
    def find(self,pattern):
        """Find all elemnts that match the pattern"""
        return self.page().mainFrame().findAllElements(pattern)
    
    def attr(self,pattern,name,value):
        """Set attribute for matching elements"""
        for e in self.find(pattern):
            e.setAttribute(name,value)
    
    def text(self,pattern,value):
        frame=self.page().mainFrame()
        frame.evaluateJavaScript("document.getElementById('%s').options[1].text='%s'"%(pattern,value))
#         for e in self.find(pattern):
#             e.setPlainText(value)
    
    def click(self,pattern):
        """Click matching elements"""
        for e in self.find(pattern):
            e.evaluateJavaScript("this.click()")
    
    def wait_load(self,pattern,timeout=60):
        """wait until pattern is found and return matches"""
        deadline=time.time()+timeout
        while time.time()<deadline:
            self.app.processEvents()
            matches=self.find(pattern)
            if matches:
                return matches 
        print 'Wait load timed out'
        
            
        
        