# coding=utf-8
runningLocally = True

import re
import scrapy
import scrapy.crawler as crawler
if runningLocally == False:
    from google.cloud import storage
import time
import json
from collections import OrderedDict
from urllib.parse import urlencode, urljoin
from multiprocessing import Process
from twisted.internet import reactor
from twisted.internet import error
from scrapy.crawler import CrawlerRunner

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
        html_resp_unicode_decoded = body.replace('\/', '/')
        html_resp_unicode_decoded = re.sub('<div' + r'.*?>', '<del>', html_resp_unicode_decoded)
        html_resp_unicode_decoded = re.sub('<span' + r'.*?>', '<del>', html_resp_unicode_decoded)
        html_resp_unicode_decoded = re.sub('aria-label' + r'.*?>', '<del>', html_resp_unicode_decoded)
        html_resp_unicode_decoded = html_resp_unicode_decoded.split('"replaceifexists"', 1)[0]

        html_resp_unicode_decoded = html_resp_unicode_decoded.replace('</div>', '')
        html_resp_unicode_decoded = html_resp_unicode_decoded.replace('</span>', '')
        html_resp_unicode_decoded = re.sub('aria-label="View event details for' + r'[.]', '', html_resp_unicode_decoded)
        html_resp_unicode_decoded = html_resp_unicode_decoded.replace('</h1>', '<del>')
        
        html_resp_unicode_decoded = html_resp_unicode_decoded.replace('<del><del><del><del>', '<del>')
        html_resp_unicode_decoded = html_resp_unicode_decoded.replace('<del><del><del>', '<del>')
        html_resp_unicode_decoded = html_resp_unicode_decoded.replace('<del><del>', '<del>')
        html_resp_unicode_decoded = re.sub('for \(\;' + r'.*?' + 'html":"', '', html_resp_unicode_decoded)
        html_resp_unicode_decoded = re.sub('<h1 class=' + r'.*?>', '<h1>', html_resp_unicode_decoded)
        html_resp_unicode_decoded = re.sub('<a class="_' + r'[0-9]+' + '"', '<a', html_resp_unicode_decoded)
        html_resp_unicode_decoded = re.sub('\?acontext=' + r'.*?' + 'aref=0', '', html_resp_unicode_decoded)
        return html_resp_unicode_decoded

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
            event['city'] = ''
            event['url'] = splitted[5]
        
        event['url'] = event['url'].replace('<a href="/events/', '').replace('"', '').strip()
        return event

    def _get_fb_event_links(self, response):
        html_resp_unicode_decoded = self.trimAwayClutter(response.body.decode('unicode_escape'))
        splitted = html_resp_unicode_decoded.split('<h1>')    
        splitted.pop(0)
        events = []

        for event in splitted:
            events.append(self.formatAsEvent(event));

        self.writeEventToFile(events)

    def upload_blob(self, bucket_name, blob_text, destination_blob_name):
        """Uploads a file to the bucket."""
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_string(blob_text)

        print('File uploaded to {}.'.format(destination_blob_name))

    def saveToLocalFile(self, name, events):
        with open('events/' + name, 'w', encoding='utf-8') as outfile:
            json.dump(events, outfile, ensure_ascii=False)

    def writeEventToFile(self, events):
        name = self.target_username + '.json'
        if (runningLocally):
            self.saveToLocalFile(name, events)
        else:
            self.upload_blob('fb-events2', json.dumps(events, ensure_ascii=False), 'events/' + name)

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

def run(d):
    fetch()

if runningLocally:
    run(None)