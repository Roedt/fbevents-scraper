import re
import scrapy
import time
import json
import scrapy.crawler as crawler
from bs4 import BeautifulSoup
from collections import OrderedDict
from urllib.parse import urlencode, urljoin
from multiprocessing import Process
from twisted.internet import reactor
from twisted.internet import error
#from google.cloud import storage
from scrapy.crawler import CrawlerRunner

runningLocally = True

class FacebookEvent(scrapy.Item):
    date = scrapy.Field()
    summary_date = scrapy.Field()
    summary_place = scrapy.Field()
    title = scrapy.Field()
    username = scrapy.Field()
    url = scrapy.Field()

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

    def _get_fb_event_links(self, response):
        html_resp_unicode_decoded = str(
            response.body.decode('unicode_escape')).replace('\\', '')

        def get_see_more_id():
            # Get the next see more id
            see_more_id_regex = re.compile(r'see_more_id=(\w+)&')
            see_more_id_search = re.search(see_more_id_regex,
                                           html_resp_unicode_decoded)
            if see_more_id_search:
                return see_more_id_search.group(1)
            return None

        def get_serialized_cursor():
            # Get the next serialized_cursor
            serialized_cursor_regex = re.compile(r'serialized_cursor=([\w-]+)')
            serialized_cursor_search = re.search(serialized_cursor_regex,
                                                 html_resp_unicode_decoded)
            if serialized_cursor_search:
                return serialized_cursor_search.group(1)
            return None

        # Extract event urls from fb event ajax response.
        event_url_regex = re.compile(r'href=\"(/events/\d+)')
        event_urls = set(
            re.findall(event_url_regex, html_resp_unicode_decoded)
        )
        for event_url in event_urls:
            yield scrapy.Request(urljoin(self.top_url, event_url),
                                 callback=self._parse_event)

        # Check if there are `serialized_cursor` and `see_more_id` attached in
        # the ajax response.
        next_serialized_cursor = get_serialized_cursor()
        next_see_more_id = get_see_more_id()
        if next_serialized_cursor and next_see_more_id:
            yield scrapy.Request(self.create_fb_event_ajax_url(
                self.fb_page_id,
                next_serialized_cursor,
                next_see_more_id),
                callback=self._get_fb_event_links)

    def _parse_event(self, response):
        html_str = str(response.body)
        soup = BeautifulSoup(html_str, 'html.parser')

        def get_event_summary():
            # Return an array containing two elements,
            # the first element is the date of the event,
            # the second element is the place of the event.
            summaries = soup.find_all('div', class_='fbEventInfoText')

            date_and_place_list = [element.get_text(' ') for element in
                                   summaries]
            # All events should have a date, but it's not necessary
            # to have a place, sometimes there's an event that doesn't
            # have a place.
            if len(date_and_place_list) != 2:
                date_and_place_list.append(None)
            return date_and_place_list

        def get_event_title():
            return soup.select('title')[0].get_text()

        fevent = FacebookEvent()
        fevent['username'] = self.target_username
        fevent['url'] = response.url
        fevent['summary_date'], fevent['summary_place'] = get_event_summary()
        fevent['title'] = get_event_title()
        self.writeEventToFile(response, fevent)
        return fevent


    def upload_blob(self, bucket_name, blob_text, destination_blob_name):
        """Uploads a file to the bucket."""
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_string(blob_text)

        print('File uploaded to {}.'.format(destination_blob_name))

    def saveToLocalFile(self, name, fevent):
        with open('events/' + name, 'w') as outfile:
            json.dump(fevent.__dict__, outfile)

    def writeEventToFile(self, response, fevent):
        url = response.url.replace('https://m.facebook.com/events/', '')
        name = self.target_username +"_" + url + '.json'
        name = name.lower()
        print('Saving ' + name)
        if (runningLocally):
            self.saveToLocalFile(name, fevent)
        else:
            self.upload_blob('fb-events2', json.dumps(fevent.__dict__), name)

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

def run(d, f):
    fetch()

if runningLocally:
    run(None, None)