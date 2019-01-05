# fbevents-scraper
scrapy_facebooker tailored for fetching information about events

This code fetches events from a defined list of Facebook pages, using the scraping (via the scrapy library). This is a specified and updated version of [scrapy_facebooker](https://github.com/refeed/scrapy_facebooker)

It fetches the information available from the general events list for a page, which is currently the title, date, place and URL, in addition to the page ID.

Combine this with [fbevents-api](https://github.com/madsop/fbevents-api) and [functions-cron](https://github.com/FirebaseExtended/functions-cron) to get the full pipeline:

- fetch the events from facebook and save them to file with this app
- schedule runs of this app via the functions-cron
- use fbevents-api to retrieve the events afterwards