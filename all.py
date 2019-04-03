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
#

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
        html_resp_unicode_decoded = response.body.decode('unicode_escape').replace('\\/', '/')
        html_resp_unicode_decoded = html_resp_unicode_decoded.replace('</div>', '')
        html_resp_unicode_decoded = html_resp_unicode_decoded.replace('</span>', '')
        html_resp_unicode_decoded = re.sub('class' + r'[=S+">]', '', html_resp_unicode_decoded)
        html_resp_unicode_decoded = re.sub('"_' + r'[S+"]', '', html_resp_unicode_decoded)
        html_resp_unicode_decoded = html_resp_unicode_decoded.replace('<div "_55ws _5cqg _5cqi" data-sigil="touchable">', '')
        html_resp_unicode_decoded = html_resp_unicode_decoded.replace('<div "_2x2s">', '')
        html_resp_unicode_decoded = html_resp_unicode_decoded.replace(' "_592p _r-i">', '')
        html_resp_unicode_decoded = html_resp_unicode_decoded.replace('<span "_5cqh"><span "_1e38 _2-xr _1mxf"><span "_1e39">', '')
        html_resp_unicode_decoded = html_resp_unicode_decoded.replace('<span "_5cqh"><span "_1e38 _2-xr"><span "_1e39">', '')
        html_resp_unicode_decoded = html_resp_unicode_decoded.replace('"replaceifexists":false,"allownull":false},{"cmd":"append","target":"static_templates","html":"","replaceifexists":true}],"contentless_response":false,"displayResources":["2AFaL","UgwR5","JMYLN"],"bootloadable":{},"ixData":{},"bxData":{},"gkxData":{},"qexData":{},"resource_map":{"2AFaL":{"type":"css","src":"https://static.xx.fbcdn.net/rsrc.php/v3/yQ/l/0,cross/ufL1xS4QjEK.css"},"UgwR5":{"type":"css","src":"https://static.xx.fbcdn.net/rsrc.php/v3/y5/l/0,cross/x7Uw6MfjJNG.css"},"JMYLN":{"type":"css","src":"https://static.xx.fbcdn.net/rsrc.php/v3/yb/l/0,cross/uWfXfDT81zH.css"}}}}', '')
        html_resp_unicode_decoded = html_resp_unicode_decoded.replace('<div "_52jc _5d19"><span "_592p"><span title="', '<del>')
        html_resp_unicode_decoded = html_resp_unicode_decoded.replace('<span "_1e3a">', '<del>')
        html_resp_unicode_decoded = html_resp_unicode_decoded.replace('<div "_52jc _5d19"><span "_592p">','<del>')
        html_resp_unicode_decoded = html_resp_unicode_decoded.replace('div "_2k4b"><div>', '')
        html_resp_unicode_decoded = html_resp_unicode_decoded.replace('"_5379" ', '')
        html_resp_unicode_decoded = re.sub('aria-label="View event details for' + r'[.]', '', html_resp_unicode_decoded)
        html_resp_unicode_decoded = html_resp_unicode_decoded.replace('</h1>', '<del>')
        html_resp_unicode_decoded = html_resp_unicode_decoded.replace('",', '')
        html_resp_unicode_decoded = html_resp_unicode_decoded.replace('<<', '<')
        splitted = html_resp_unicode_decoded.split("<h1")
        splitted.pop(0)
    
        print(splitted)

        for event in splitted:
            self.writeEventToFile(self, '123', event)

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

    def writeEventToFile(self, urlIn, fevent):
        url = urlIn.replace('https://m.facebook.com/events/', '')
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

def run(d, f):
    fetch()

if runningLocally:
    run(None, None)