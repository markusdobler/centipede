# encoding: utf8
from concurrent import futures
import requests
from bs4 import BeautifulSoup
import re
from hashlib import sha1

thread_pool = futures.ThreadPoolExecutor(max_workers=10)

def load_url(url, timeout=None):
    result = requests.get(url, timeout=timeout)
    return result.content

def load_soup(url, timeout=None):
    return BeautifulSoup(load_url(url, timeout))


class Feed(object):
    feeds = {}
    def __init__(self, id, title, subtitle, url):
        self.id = id
        self.title = title
        self.subtitle = subtitle
        self.url = url
        self.entries = []
        Feed.feeds[id] = self


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

        fs = [thread_pool.submit(self.parse_item, item) for item in
                   soup('item')]

        done, not_done = futures.wait(fs, timeout=5)
        self.entries = [f.result() for f in done if not f.exception()]

        for f in not_done:
            print "not done", f
        for f in [f for f in done if f.exception()]:
            print f.exception()

class TitanicBriefeRss(Feed):
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


class TitanicFachmannRss(TitanicBriefeRss):
    def __init__(self):
        Feed.__init__(self, 'titanic_fachmann', 'Titanic Vom Fachmann fuer Kenner',
                      'Titanic Fachmann fulltext',
                      'http://www.titanic-magazin.de/fachmann')
        self.url = 'http://www.titanic-magazin.de/fachmann/'


titanic = TitanicRss()
titanic_briefe = TitanicBriefeRss()
titanic_fachmann = TitanicFachmannRss()

if __name__ == '__main__':
    for feed in Feed.feeds.values():
        feed.crawl()
        print feed.entries
