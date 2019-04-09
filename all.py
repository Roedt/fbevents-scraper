# coding=utf-8
runningLocally = False

from os import getenv

import pymysql
from pymysql.err import OperationalError
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
from twisted.internet import reactor

class FacebookEventSpider(scrapy.Spider):
    name = 'facebook_event'
    start_urls = (
        'https://m.facebook.com/',
    )
    allowed_domains = ['m.facebook.com']
    top_url = 'https://m.facebook.com'

    def __init__(self, page, *args, **kwargs):
        self.target_username = page

        if not self.target_username:
            raise Exception('`target_username` argument must be filled')

    def parse(self, response):
        return scrapy.Request(
            '{top_url}/{username}/events'.format(
                top_url=self.top_url,
                username=self.target_username),
            callback=self._get_facebook_events_ajax)

    def _get_facebook_events_ajax(self, response):
        # Get Facebook events ajax
        def get_fb_page_id():
            p = re.compile(r'page_id=(\d*)')
            search = re.search(p, str(response.body))
            return search.group(1)

        self.fb_page_id = get_fb_page_id()

        return scrapy.Request(self.create_fb_event_ajax_url(self.fb_page_id,
                                                            '0',
                                                            'u_0_d'),
                              callback=self._get_fb_event_links)

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

    def parseAddressToPostnumber(self, address):
        parsed = re.search('\d{4}', address)
        if (parsed):
            return parsed.group(0)
        return ''

    def parseSingleEvent(self, response):
        try:
            self.parseSingleEventInner(response)
        except Exception as e:
            print(e)

    def parseSingleEventInner(self, response):
        html_str = response.body.decode('unicode-escape')
        soup = BeautifulSoup(html_str, 'html.parser')

        summaries = soup.find_all('div', class_='fbEventInfoText')

        original = response.meta.get('original')
        parsedEvent = {}
        parsedEvent['title'] = original['title']
        parsedEvent['month'] = original['month']
        parsedEvent['dayOfMonth'] = original['dayOfMonth']
        parsedEvent['timeOfDay'] = original['time']
        parsedEvent['url'] = response.url
        parsedEvent['eventID'] = re.sub('http' + r'.*?' + 'events/', '', parsedEvent['url'])

        time = self.trimSingleEvent(str(summaries[0])).split('<del>')
        parsedEvent['time'] = time[0]

        fullLocation = self.trimSingleEvent(str(summaries[1])).split('<del>')
        parsedEvent['location'] = fullLocation[0]
        if (len(fullLocation) == 2):
            parsedEvent['address'] = fullLocation[1]
        else:
            parsedEvent['address'] = ''
        parsedEvent['postnumber'] = self.parseAddressToPostnumber(parsedEvent['address'])
        if runningLocally == False:
            if len(parsedEvent['postnumber']) > 0:
                position = getPosition(parsedEvent['postnumber'])
                parsedEvent['lat'] = position['lat']
                parsedEvent['lon'] = position['lon']

        parsedEvent['host'] = self.target_username
        
        self.writeEventToFile(parsedEvent)

    def formatAsEvent(self, eventIn):
        event = {}
        splitted = eventIn.split('<del>')
        event['host'] = self.target_username
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

    def _get_fb_event_links(self, response):
        html_resp_unicode_decoded = self.trimAwayClutter(response.body.decode('unicode_escape'))
        splitted = html_resp_unicode_decoded.split('<h1>')    
        splitted.pop(0)
        events = []

        for event in splitted:
            formattedEvent = self.formatAsEvent(event)
            events.append(formattedEvent);
            url = urljoin(self.top_url, 'events/' + formattedEvent['url'])
            yield scrapy.Request(url, callback=self.parseSingleEvent, meta={'original': formattedEvent})

    def upload_blob(self, bucket_name, blob_text, destination_blob_name):
        """Uploads a file to the bucket."""
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_string(blob_text)

        print('File uploaded to {}.'.format(destination_blob_name))

    def saveToLocalFile(self, name, event):
        with open('events/' + name, 'w', encoding='utf-8') as outfile:
            json.dump(event, outfile, ensure_ascii=False)

    def writeEventToFile(self, event):
        name = event['host'] + "_" + event['eventID'] + '.json'
        if (runningLocally):
            self.saveToLocalFile(name, event)
        else:
            self.upload_blob('fb-events2', json.dumps(event, ensure_ascii=False), 'events/' + name)

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
        return ['AttacNorge', 'UngdommotEU']
    client = storage.Client()
    bucket = client.bucket('fb-events2')

    blob = bucket.get_blob('pages.txt')
    pages = str(blob.download_as_string())
    pages = pages.replace('b\'', '').replace('\'', '').split(",")
    return pages

def fetch():
    runner = crawler.CrawlerRunner({
        'USER_AGENT': 'Mozilla/5.0 (Linux; U; Android 4.0.3; ko-kr; LG-L160L Build/IML74K) AppleWebkit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30'
    })
    for page in getPages():
        runner.crawl(FacebookEventSpider, page=page)
    d = runner.join()
    d.addBoth(lambda _: reactor.stop())
    reactor.run()


CONNECTION_NAME = getenv(
  'INSTANCE_CONNECTION_NAME',
  'facebookevents:europe-west1:postnumber')
DB_USER = getenv('MYSQL_USER', 'read')
DB_PASSWORD = getenv('MYSQL_PASSWORD', 'AllowedToRead')
DB_NAME = getenv('MYSQL_DATABASE', 'postnummer')

mysql_config = {
  'user': DB_USER,
  'password': DB_PASSWORD,
  'db': DB_NAME,
  'charset': 'utf8mb4',
  'cursorclass': pymysql.cursors.DictCursor,
  'autocommit': True
}

# Create SQL connection globally to enable reuse
# PyMySQL does not include support for connection pooling
mysql_conn = None


def __get_cursor():
    """
    Helper function to get a cursor
      PyMySQL does NOT automatically reconnect,
      so we must reconnect explicitly using ping()
    """
    try:
        return mysql_conn.cursor()
    except OperationalError:
        mysql_conn.ping(reconnect=True)
        return mysql_conn.cursor()

def getPosition(postnumber):
    global mysql_conn

    # Initialize connections lazily, in case SQL access isn't needed for this
    # GCF instance. Doing so minimizes the number of active SQL connections,
    # which helps keep your GCF instances under SQL connection limits.
    if not mysql_conn:
        try:
            mysql_conn = pymysql.connect(**mysql_config)
        except OperationalError:
            # If production settings fail, use local development ones
            mysql_config['unix_socket'] = f'/cloudsql/{CONNECTION_NAME}'
            mysql_conn = pymysql.connect(**mysql_config)

    # Remember to close SQL resources declared while running this function.
    # Keep any declared in global scope (e.g. mysql_conn) for later reuse.
    with __get_cursor() as cursor:
        cursor.execute('SELECT lat, lon from postnummer where postnr = ' + postnumber)
        results = cursor.fetchone()
        toReturn = {}
        toReturn['lat'] = str(results['lat'])
        toReturn['lon'] = str(results['lon'])
        return toReturn


def run(d, f):
    fetch()

if runningLocally:
    try:
        run(None, None)
    except Exception as e:
        print(e)