# coding=utf-8
runningLocally = True

from os import getenv
from os import path
from os import makedirs

import re
import scrapy.http.request
import scrapy.spiders
import scrapy.crawler as crawler
from collections import OrderedDict
from bs4 import BeautifulSoup
from urllib.parse import urlencode, urljoin
if runningLocally == False:
    from google.cloud import storage
import json
from datetime import datetime
from twisted.internet import reactor

class ClutterTrimmer:
    def trimAwayClutter(self, body):
        text = body.replace('\/', '/')
        text = re.sub('<div' + r'.*?>', '<del>', text)
        text = re.sub('<span' + r'.*?>', '<del>', text)
        text = re.sub('aria-label' + r'.*?>', '<del>', text)
        text = text.split('"replaceifexists"', 1)[0]

        text = text.replace('</div>', '')
        text = text.replace('</span>', '')
        text = re.sub('aria-label="View event details for' + r'[.]', '', text)
        text = text.replace('</h1>', '<del>')
        
        text = text.replace('<del><del><del><del>', '<del>')
        text = text.replace('<del><del><del>', '<del>')
        text = text.replace('<del><del>', '<del>')
        text = text.replace('<del><del>', '<del>')
        text = re.sub('for \(\;' + r'.*?' + 'html":"', '', text)
        text = re.sub('<h1 class=' + r'.*?>', '<h1>', text)
        text = re.sub('<a class="_' + r'[0-9]+' + '"', '<a', text)
        text = re.sub('\?acontext=' + r'.*?' + 'aref=0', '', text)
        return text
    
    def trimSingleEvent(self, text):
        trimmed = self.trimAwayClutter(text)
        trimmed = trimmed.replace('<dt>', '')
        trimmed = trimmed.replace('<dd>', '')
        trimmed = re.sub('</' + '.*?>', '', trimmed)
        trimmed = re.sub('<del>' + r'$', '', trimmed)
        trimmed = trimmed.replace('<del><del>', '')
        return trimmed

class EventPersister:
    def __init__(self, target_username):
        self.target_username = target_username

    def __upload_blob(self, bucket_name, blob_text, destination_blob_name):
        """Uploads a file to the bucket."""
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_string(blob_text)

        print('File uploaded to {}.'.format(destination_blob_name))

    def __saveToLocalFile(self, name, event):
        folder = self.__getFolder()
        if not (path.exists(folder)):
            makedirs(folder)
        with open(folder + name, 'w', encoding='utf-8') as outfile:
            json.dump(event, outfile, ensure_ascii=False)

    def writeEventToFile(self, event):
        name = self.target_username + "_" + event['eventID'] + '.json'
        if (runningLocally):
            self.__saveToLocalFile(name, event)
        else:
            self.__upload_blob('fb-events2', json.dumps(event, ensure_ascii=False), self.__getFolder() + name)

    def __getFolder(self):
        return 'events/' + datetime.today().strftime('%Y%m%d') +'/'


class EventFactory:
    def __init__(self, displayName, target_username):
        print('toprint')
        self.displayName = displayName
        try:
            self.eventPersister = EventPersister(target_username)
        except Exception as e:
            print(e)

    def parseSingleEvent(self, response):
        try:
            self.__parseSingleEventInner(response)
        except Exception as e:
            print(e)

    def __parseSingleEventInner(self, response):
        html_str = response.body.decode('unicode-escape')
        soup = BeautifulSoup(html_str, 'html.parser')

        summaries = soup.find_all('div', class_='fbEventInfoText')

        original = response.meta.get('original')
        parsedEvent = {}
        parsedEvent['title'] = original['title']
        parsedEvent['month'] = original['month']
        parsedEvent['dayOfMonth'] = original['dayOfMonth']
        timeOfDay = original['time'].split(' UTC')[0]
        timeOfDay = datetime.strptime(timeOfDay, '%I:%M %p')
        timeOfDay = datetime.strftime(timeOfDay, '%H.%M')
        parsedEvent['timeOfDay'] = timeOfDay
        parsedEvent['url'] = response.url
        parsedEvent['eventID'] = re.sub('http' + r'.*?' + 'events/', '', parsedEvent['url'])

        fullLocation = ClutterTrimmer().trimSingleEvent(str(summaries[1])).split('<del>')
        parsedEvent['location'] = fullLocation[0]
        if (len(fullLocation) == 2):
            parsedEvent['address'] = fullLocation[1]
        else:
            parsedEvent['address'] = ''

        positionFromMap = self.getPositionFromMap(html_str)
        if positionFromMap is not None:
            parsedEvent['lat'] = positionFromMap['lat']
            parsedEvent['lon'] = positionFromMap['lon']

        parsedEvent['host'] = self.displayName
        try:
            self.eventPersister.writeEventToFile(parsedEvent)
        except Exception as e:
            print(e)

    def formatAsEvent(self, eventIn):
        event = {}
        splitted = eventIn.split('<del>')
        event['host'] = self.displayName
        event['title'] = splitted[0]
        event['month'] = splitted[1]
        event['dayOfMonth'] = splitted[2]
        event['time'] = splitted[3]
        event['location'] = splitted[4]
        if not splitted[5].startswith("<a href"):
            event['city'] = splitted[5]
            event['url'] = splitted[6]
        else:
            event['url'] = splitted[5]
            event['city'] = ''
        
        event['url'] = event['url'].replace('<a href="/events/', '').replace('"', '').strip()
        return event
        
    def getPositionFromMap(self, html):
        try:
            lmindex = html.index("26daddr%3D")
            pos = html[lmindex+10:lmindex+45].split('%252C')
            lat = pos[0]
            lon = pos[1].split('%')[0]
            return {"lat": lat, "lon": lon}
        except ValueError as e:
            return None



class FacebookEventSpider(scrapy.Spider):
    fburl = 'https://m.facebook.com/'
    name = 'facebook_event'
    start_urls = (
        fburl,
    )
    allowed_domains = ['m.facebook.com']
    top_url = fburl

    def __init__(self, page, *args, **kwargs):
        self.displayName = page[0].strip()
        self.target_username = page[2].strip()

    def parse(self, response):
        try: 
            url = '{top_url}/{username}/events/'.format(
                top_url=self.top_url,
                    username=self.target_username)
            return scrapy.Request(url,
            callback=self._get_facebook_events_ajax)
        except Exception as e:
            print(e)

    def _get_facebook_events_ajax(self, response):
        def get_fb_page_id():
            p = re.compile(r'page_id=(\d*)')
            search = re.search(p, str(response.body))
            return search.group(1)

        self.fb_page_id = get_fb_page_id()

        return scrapy.Request(self.create_fb_event_ajax_url(self.fb_page_id,
                                                            '0',
                                                            'u_0_d'),
                              callback=self._get_fb_event_links)

    

    def _get_fb_event_links(self, response):
        html_resp_unicode_decoded = ClutterTrimmer().trimAwayClutter(response.body.decode('unicode_escape'))
        eventsForThisPage = html_resp_unicode_decoded.split('<h1>')    
        eventsForThisPage.pop(0)
        if (not eventsForThisPage):
            return 
        eventFactory = EventFactory(self.displayName, self.target_username)
        
        for event in eventsForThisPage:
            formattedEvent = eventFactory.formatAsEvent(event)
            url = urljoin(self.top_url, 'events/' + formattedEvent['url'])
            yield scrapy.Request(url, callback=eventFactory.parseSingleEvent, meta={'original': formattedEvent})

    @staticmethod
    def create_fb_event_ajax_url(page_id, serialized_cursor, see_more_id):
        event_url = 'https://m.facebook.com/pages/events/more'
        query_str = urlencode(OrderedDict(page_id=page_id,
                                          query_type='upcoming',
                                          see_more_id=see_more_id,
                                          serialized_cursor=serialized_cursor))

        return '{event_url}/?{query}'.format(event_url=event_url,
                                             query=query_str)
    


def getPages():
    if runningLocally:
        return [
            'Oslo Søndre Nordstrand;Rødt Oslo; RoedtSondreNordstrand',
            'Oslo Skole og Barnehage;Rødt Oslo;',
            'Rødt;;Roedt'
        ]
    now = int(datetime.now().strftime('%H'))
    if now % 2 == 0:
        pagelist = 'pages3'
    else:
        pagelist = 'pages4'
    client = storage.Client()
    bucket = client.bucket('fb-events2')

    blob = bucket.get_blob(pagelist + '.txt')
    pages = str(blob.download_as_string(), 'utf-8')
    pages = pages.split('\r\n')
    return pages

def fetch():
    runner = crawler.CrawlerRunner({
        'USER_AGENT': 'Mozilla/5.0 (Linux; U; Android 4.0.3; ko-kr; LG-L160L Build/IML74K) AppleWebkit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30'
    })
    for page in getPages():
        singlePage = page.split(';')
        singlePage[0] = 'Rødt ' + singlePage[0]
        if len(singlePage) == 3 and singlePage[2].strip():
            runner.crawl(FacebookEventSpider, page=singlePage)
    d = runner.join()
    d.addBoth(lambda _: reactor.stop())
    reactor.run()

def run(d, f):
    fetch()

def runSingleParam(d):
    run(d, None)

if runningLocally:
    try:
        run(None, None)
    except Exception as e:
        print(e)