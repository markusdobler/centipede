# encoding: utf8
from concurrent import futures
import requests
from bs4 import BeautifulSoup
import re
from hashlib import sha1
import logging
from datetime import datetime
import itertools
from cachetools import LRUCache

thread_pool = futures.ThreadPoolExecutor(max_workers=10)

def load_url(url, timeout=None):
    result = requests.get(url, timeout=timeout)
    return result.content

def load_soup(url, timeout=None):
    return BeautifulSoup(load_url(url, timeout))


class Feed(object):
    feeds = {}
    def __init__(self, id, title, subtitle, url, cache_size=100):
        self.id = id
        self.title = title
        self.subtitle = subtitle
        self.url = url
        self.entries = []
        Feed.feeds[id] = self
        self._cache = LRUCache(cache_size)

    def get_parsed_item(self, item):
        link = item.link.string
        print "%s:" % link
        try:
            parsed = self._cache[link]
            print " from cache"
        except:
            parsed = self._cache[link] = self.parse_item(item)
            print " remote"
        print len(self._cache)
        return parsed

class TitanicRss(Feed):
    def __init__(self):
        Feed.__init__(self, 'titanic', 'Titanic RSS', 'Titanic fulltext',
                      'http://www.titanic-magazin.de')

    def extract_bodytext(self, item_soup):
        news_bodytext = item_soup.find('div', {'class': 'tt_news-bodytext'})
        bodytexts = news_bodytext.find_all('p', {'class': 'bodytext'})
        if bodytexts:
            return u"<div>\n%s\n</div>" % "\n".join(unicode(b) for b in bodytexts)
        lists = news_bodytext.find_all('ul')
        if lists:
            return u"<div>\n%s\n</div>" % "\n".join(unicode(l) for l in lists)

    def fix_image_links(self, soup):
        for img in soup('img'):
            if img['src'].startswith('http://'): continue
            img['src'] = "http://www.titanic-magazin.de/" + img['src']

    def parse_item(self, item):
        link = item.link.string
        soup = load_soup(link)

        self.fix_image_links(soup)

        return dict(
            link = link,
            title = item.title.string,
            id = item.guid.string,
            content = self.extract_bodytext(soup),
        )

    def crawl(self):
        rss_url = 'http://www.titanic-magazin.de/ich.war.bei.der.waffen.rss'
        soup = load_soup(rss_url)

        fs = [thread_pool.submit(self.get_parsed_item, item) for item in
                   soup('item')]

        done, not_done = futures.wait(fs, timeout=5)
        self.entries = [f.result() for f in done if not f.exception()]

        for f in not_done:
            logging.warning("Future not done: %s" % f)
        for f in [f for f in done if f.exception()]:
            logging.warning("Future failed: %s" % f.exception())

class TitanicBriefe(Feed):
    def __init__(self):
        Feed.__init__(self, 'titanic_briefe', 'Titanic Briefe an die Leser',
                      'Titanic Briefe fulltext',
                      'http://www.titanic-magazin.de/briefe')
        self.url = 'http://www.titanic-magazin.de/briefe/'

    def extract_bodytext(self, item_soup):
        bodytexts = item_soup.find_all('p', {'class': 'bodytext'})
        if bodytexts:
            return u"<div>\n%s\n</div>" % "\n".join(unicode(b) for b in bodytexts)

    def parse_item(self, item_soup):
        try:
            content = self.extract_bodytext(item_soup)
            title = item_soup.h1.string
        except:
            return None
        if content:
            return dict(
                    link = self.url,
                    title = title,
                    id = sha1(repr(content)).hexdigest(),
                    content = content,
                )

    def crawl(self):
        soup = load_soup(self.url)
        heft_texts = soup.find_all('div', {'class': 'heft_text'})
        items = [i for h in heft_texts for i in h.find_all('div', {'class': 'csc-default'})]
        potential_entries = [self.parse_item(item) for item in items]
        self.entries = [e for e in potential_entries if e]


class TitanicFachmann(TitanicBriefe):
    def __init__(self):
        Feed.__init__(self, 'titanic_fachmann', 'Titanic Vom Fachmann fuer Kenner',
                      'Titanic Fachmann fulltext',
                      'http://www.titanic-magazin.de/fachmann')
        self.url = 'http://www.titanic-magazin.de/fachmann/'


class RivvaRss(Feed):
    def __init__(self):
        Feed.__init__(self, 'rivva', 'Rivva grouped',
                      '6 hour blocks for Rivva',
                      'http://rivva.de/',
                     cache_size=500)

    def timeblock(self, timestamp):
        return timestamp.replace(hour=timestamp.hour/6*6, minute=0, second=0)

    
    def parse_item(self, item):
        rivva_link = item.link.string
        soup = load_soup(rivva_link)
        timestamp = item.pubdate.string
        timestamp = timestamp.rsplit(' ', 1)[0] # remove timezone info
        timestamp = datetime.strptime(timestamp, '%a, %d %b %Y %H:%M:%S')
        if timestamp > self._current_timeblock:
            raise "Timeblock still open. Keep aggregating"
        link = soup.h1.a['href']
        return dict(
            link = link,
            rivva_link = rivva_link,
            title = item.title.string,
            id = item.guid.string,
            timestamp = timestamp,
            timeblock = self.timeblock(timestamp),
        )

    def format_group(self, timeblock, items):
        content = "<ul>%s</ul>" % "\n".join(
            '<li><a href="%s">%s</a> (<a href="%s">via</a>)</li>' % (
                i['link'], i['title'], i['rivva_link']
            ) for i in items
        )
        return dict(
            link = '',
            title = 'Rivva %s..%02i:00' % (
                timeblock.strftime('%Y-%m-%d, %H:%M'),
                timeblock.hour+6),
            id = items[0]['id'],
            content = content
        )

    def crawl(self):
        self._current_timeblock = self.timeblock(datetime.now())
        rss_url = 'http://feeds.feedburner.com/rivva'
        soup = load_soup(rss_url)

        fs = [thread_pool.submit(self.get_parsed_item, item) for item in
                   soup('item')]

        done, not_done = futures.wait(fs, timeout=5)
        items = [f.result() for f in done if not f.exception()]

        items.sort(key=lambda d: d['timestamp'])
        groups = itertools.groupby(items, lambda d: d['timeblock'])
        self.entries = [self.format_group(timeblock, list(items)) for timeblock, items
                        in groups]

        for f in not_done:
            logging.info("Future not done: %s" % f)
        for f in [f for f in done if f.exception()]:
            logging.info("Future failed: %s" % f.exception())



titanic = TitanicRss()
titanic_briefe = TitanicBriefe()
titanic_fachmann = TitanicFachmann()
rivva = RivvaRss()

if __name__ == '__main__':
    for feed in Feed.feeds.values():
        feed.crawl()
        print feed.entries
