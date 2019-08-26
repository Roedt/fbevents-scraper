# coding=utf-8
runningLocally = True

from os import getenv
from os import path
from os import makedirs

import pytz
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
        name = event['preciseTime'] + "_" + self.target_username + "_" + event['eventID'] + '.json'
        if (runningLocally):
            self.__saveToLocalFile(name, event)
        else:
            self.__upload_blob('fb-events2', json.dumps(event, ensure_ascii=False), self.__getFolder() + name)

    def __getFolder(self):
        return 'events/v5/' + self.__getToday().strftime('%Y%m%d') +'/'

    def __getToday(self):
        return datetime.now(pytz.timezone('Europe/Oslo'))

class Event:
    def __init__(self, original, url, soup, summaries, positionFromMap, displayName):
        if original:
            self.title = original['title']
            self.month = original['month']
            self.dayOfMonth = int(original['dayOfMonth'])
            timeOfDay = original['time'].split(' UTC')[0]
            timeOfDay = datetime.strptime(timeOfDay, '%I:%M %p')
            hour = datetime.strftime(timeOfDay, '%H')
            minutes = datetime.strftime(timeOfDay, '%M')
        else:
            self.dayOfMonth = int(self.__getDayOfMonth(soup))
            self.month = self.__getMonth(soup)
            
            eventSearch = re.search(re.compile(r'startDate":".*"'), str(soup))
            if eventSearch is not None:
                eventInfo = eventSearch.group()
                eventInfo = eventInfo.split('","')
                self.title = eventInfo[2].split(':"')[1]
                [hour, minutes] = eventInfo[0].split('T')[1].split(':00+')[0].split(':')
            else:
                self.title = ''
                time = soup.find_all('div', class_='_52je _52jb _52jg')
                time = str(time[0]).split(' at ')
                time = time[1].split(' UTC')[0]
                timeOfDay = datetime.strptime(timeOfDay, '%I:%M %p')
                hour = datetime.strftime(timeOfDay, '%H')
                minutes = datetime.strftime(timeOfDay, '%M')
                # = time[0]
                minutes = time[1]

        self.timeOfDay = hour + '.' + minutes
        
        self.dayOfMonth, self.month = self.__getFirstRecurringUpcoming(soup)
        self.eventID, self.url = self.__getEventID(url, original)

        self.location, self.address = self.__getLocationAndAddress(summaries)

        self.lat, self.lon = self.__getPositionFromMap(positionFromMap)

        self.host = displayName
        self.preciseTime = self.__getTimeOfEvent(int(hour), int(minutes))

    MONTHS = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']

    def __getEventID(self, url, original): 
        eventID = re.sub('http' + r'.*?' + 'events/', '', url)
        if ('event_time_id' in eventID):
            eventID = eventID.split('=')[1].replace('&_rdr', '')
        if (url == 'https://m.facebook.com/events/' and eventID == '' and original):
            location = original['location']
            eventID = location.replace('<a href="/events/', '').replace('" ', '')
            url = url + eventID
        return eventID, url

    def __getLocationAndAddress(self, summaries):
        if len(summaries) < 2:
            return ['', '']
        fullLocation = ClutterTrimmer().trimSingleEvent(str(summaries[0])).split('<del>')
        location = fullLocation[0]
        if (len(fullLocation) == 2):
            address = fullLocation[1]
        else:
            address = ''
        return [location, address]

    def __getPositionFromMap(self, positionFromMap):
        if positionFromMap is not None:
            return [positionFromMap['lat'], positionFromMap['lon']]
        elif self.eventID == '703005723505583':
            return ['59.739340', '10.203205']
        else:
            return [None, None]

    def __getDayOfMonth(self, soup):
        day = soup.find_all('span', class_='_38nj')
        day = re.sub('<span' + r'.*?>', '', str(day))
        day = re.sub('</span>', '', day).replace('[','').replace(']','')
        return day

    def __getMonth(self, soup):
        monthsFound = soup.find_all('span', class_='_5a4-')
        for month in monthsFound:
            month = re.sub('<span' + r'.*?>', '', str(month))
            month = re.sub('</span>', '', month)

        if len(monthsFound) == 0:
            month = soup.find_all('span', class_='_38nk')
            month = re.sub('<span' + r'.*?>', '', str(month))
            month = re.sub('</span>', '', month).replace('[','').replace(']','')
        return month

    def __getTimeOfEvent(self, hour, minutes):
        year = datetime.today().year
        month = self.MONTHS.index(self.month) + 1
        asDatetime = datetime(year, month, self.dayOfMonth, hour, minutes).strftime('%Y%m%d%H%M')
        return asDatetime

    def __getFirstRecurringUpcoming(self, soup):
        try:
            now = datetime.now()
            originalTime = datetime(now.year,  self.MONTHS.index(self.month) + 1, self.dayOfMonth)
            if now <= originalTime:
                return self.dayOfMonth, self.month
            startingPoint = soup.text.split('setIsDetailedProfiler')
            if len(startingPoint) < 3:
                return self.dayOfMonth, self.month
            startingPoint = startingPoint[2].split('AgainCancelLoading')[0]
            startingPoint = startingPoint.split('InterestedInviteMoreSummary')
            if len(startingPoint) < 2:
                return self.dayOfMonth, self.month
            startingPoint = startingPoint[1]
            startingPoint = re.sub(r'.*?' + 'UTC\+[0-9]+', '', startingPoint)
            try:
                dayOfMonth = int(startingPoint[3:5])
            except ValueError:
                dayOfMonth = int(startingPoint[3:4])
            return dayOfMonth, startingPoint[:3]
        except Exception as e:
            return self.dayOfMonth, self.month

    def toItem(self):
        parsedEvent = {}
        parsedEvent['title'] = self.title
        parsedEvent['month'] = self.month
        parsedEvent['dayOfMonth'] = self.dayOfMonth
        parsedEvent['timeOfDay'] = self.timeOfDay
        parsedEvent['url'] = self.url
        parsedEvent['eventID'] = self.eventID
        parsedEvent['address'] = self.address
        if self.lat:
            parsedEvent['lat'] = self.lat
        if self.lon:
            parsedEvent['lon'] = self.lon
        parsedEvent['host'] = self.host
        parsedEvent['preciseTime'] = self.preciseTime
        return parsedEvent

class EventFactory:
    def __init__(self, displayName, target_username):
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
        positionFromMap = self.getPositionFromMap(html_str)
        parsedEvent = Event(original, response.url, soup, summaries, positionFromMap, self.displayName)
        event = parsedEvent.toItem()

        try:
            self.eventPersister.writeEventToFile(event)
        except Exception as e:
            print(e)

    def formatAsEvent(self, eventIn):
        event = {}
        splitted = eventIn.split('<del>')
        if len(splitted) < 5:
            return
        event['host'] = self.displayName
        event['title'] = splitted[0]
        event['month'] = splitted[1]
        event['dayOfMonth'] = splitted[2]
        event['time'] = splitted[3]
        event['location'] = splitted[4]
        if len(splitted) == 6:
            event['url'] = splitted[4].split("=\"/events/")[1].replace('" ', '')
        elif (not splitted[5].startswith("<a href")):
            event['city'] = splitted[5]
            event['url'] = splitted[6]
        else:
            event['url'] = splitted[5]
            event['city'] = ''
        
        event['url'] = event['url'].replace('<a href="/events/', '').replace('"', '').strip()
        return event
        
    def getPositionFromMap(self, html):
        try:
            if not '26daddr%3D' in html:
                return
            lmindex = html.index('26daddr%3D')
            pos = html[lmindex+10:lmindex+45].split('%252C')
            lat = pos[0]
            lon = pos[1].split('%')[0]
            return {"lat": lat, "lon": lon}
        except ValueError as e:
            return None



class FacebookEventSpider(scrapy.Spider):
    name = 'facebook_event'
    allowed_domains = ['m.facebook.com']
    top_url = 'https://m.facebook.com/'
    start_urls = ( top_url, )

    def __init__(self, displayName, target_username, eventID):
        self.displayName = displayName
        self.target_username = target_username
        self.eventID = eventID

    def parse(self, response):
        if not self.eventID:
            try: 
                url = '{top_url}/{username}/events/'.format(top_url=self.top_url, username=self.target_username)
                return scrapy.Request(url, callback=self._get_facebook_events_ajax)
            except Exception as e:
                print(e)
        else:
            eventFactory = EventFactory(self.displayName, self.target_username)
            formattedEvent = eventFactory.formatAsEvent('')
            url = urljoin(self.top_url, 'events/' + self.eventID)
            meta = {'original': formattedEvent}
            return scrapy.Request(url, callback=eventFactory.parseSingleEvent, meta=meta)

    def _get_facebook_events_ajax(self, response):
        page_id = re.search(re.compile(r'page_id=(\d*)'), str(response.body)).group(1)
        url = self.create_fb_event_ajax_url(page_id)
        return scrapy.Request(url, callback=self._get_fb_event_links)

    def _get_fb_event_links(self, response):
        body = response.body.decode('unicode_escape')
        html_resp_unicode_decoded = ClutterTrimmer().trimAwayClutter(body)
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
    def create_fb_event_ajax_url(page_id):
        query_str = urlencode(OrderedDict(page_id=page_id,
                                          query_type='upcoming',
                                          see_more_id='u_0_d',
                                          serialized_cursor='0'))
        return '{event_url}/?{query}'.format(event_url='https://m.facebook.com/pages/events/more', query=query_str)

def getPages():
    if runningLocally:
        return [
            'Oslo Søndre Nordstrand;Oslo; RoedtSondreNordstrand',
            'Oslo Skole og Barnehage;Oslo;',
            'nasjonalt;;Roedt'
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
            runner.crawl(FacebookEventSpider, displayName=singlePage[0].strip(), target_username=singlePage[2].strip(), eventID=None)

    specificEventIds = [
        ['rodttromso', 'Rødt Tromsø', '340794319941557'], # Treff Rødt Tromsø, 31 aug
        ['rodttromso', 'Rødt Tromsø', '340794316608224'], # Treff Rødt Tromsø, 7. sept
        ['rodttromso', 'Rødt Tromsø', '390521238219707'], # Vkaktivistmøte, 29.aug
        ['rodttromso', 'Rødt Tromsø', '390521241553040']  # Vkaktivistmøte, 5. sept
    ]

    if int(datetime.now().strftime('%H')) % 10 == 0:
        for eventID in specificEventIds:
            runner.crawl(FacebookEventSpider, displayName=eventID[1], target_username=eventID[0], eventID=eventID[2])
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