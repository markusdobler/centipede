# encoding: utf8
from concurrent import futures
import requests
from bs4 import BeautifulSoup
import re
from hashlib import sha1
import logging
from datetime import datetime, timedelta
import itertools
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Index

db = SQLAlchemy()

def none2now(now=None):
    return datetime.now() if now is None else now

class DoNotCache(Exception):
    pass

class Cache(db.Model):
    __tablename__ = 'centipede_cache'
    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(1000), index=True)
    obj = db.Column(db.PickleType())
        
    @classmethod
    def store(cls, key, obj):
        self = cls()
        self.key = key
        self.obj = obj
        db.session.add(self)
        db.session.commit()
        return self

    @classmethod
    def get(cls, key):
        entry = cls.query.filter_by(key=key).first()
        if entry:
            return entry.obj

    @classmethod
    def get_or_calc(cls, keys, fun, *extra_args_list):
        # wrap fun to also store key
        wrapped_fun = lambda key, *extra_args: (key, fun(key, *extra_args))
        # generate futures of fun(key, x_args) for all keys that are cache misses
        args_list = zip(keys, *extra_args_list) if extra_args_list else zip(keys)
        fs = [thread_pool.submit(wrapped_fun, *args)
              for (key,args) in zip(keys, args_list) if not cls.get(key)]

        done, not_done = futures.wait(fs, timeout=5)
        results = [f.result() for f in done if not f.exception()]

        # store objects
        for (key, obj) in results:
            if obj:
                cls.store(key, obj)

        # process errors
        for f in not_done:
            logging.warning("Future not done: %s" % f)
        for f in [f for f in done if f.exception()]:
            if isinstance(f.exception(), DoNotCache):
                continue
            logging.warning("Future failed: %s" % f.exception())

        return [cls.get(key) for key in keys]


thread_pool = futures.ThreadPoolExecutor(max_workers=10)

def load_url(url, timeout=None):
    result = requests.get(url, timeout=timeout)
    return result.content

def load_soup(url, timeout=None, parser='lxml'):
    html = load_url(url, timeout)
    html = html.replace(b"</scr' + 'ipt>",b"")  # workaround for problem with bs4 on dilbert
    return BeautifulSoup(html, features=parser)

def load_and_parse_rss_feed(url, timeout=None):
    soup = load_soup(url, timeout, 'xml')
    items = list(soup('item'))
    urls = [str(i.link.string) for i in items]
    return urls, items

class Feed(object):
    feeds = {}
    def __init__(self, id, title, subtitle, url, cache_size=100):
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
        bodytext = item_soup.find('article', {'class': 'tt_news-entry'})
        if not bodytext:
            content = item_soup.find('section', {'id': 'content'})
            bodytext = content.find('div', {'class': 'csc-default'})
        mainmatter_end = bodytext.find('div', {'class': 'tt_news-category'})
        if mainmatter_end:
            backmatter = list(mainmatter_end.next_siblings)
            for e in backmatter:
               e.extract()
        return str(bodytext)

    def fix_image_links(self, soup):
        for img in soup('img'):
            if img['src'].startswith('http://'): continue
            img['src'] = "http://www.titanic-magazin.de/" + img['src']

    def crawl(self):
        rss_url = 'http://www.titanic-magazin.de/ich.war.bei.der.waffen.rss'
        urls, items = load_and_parse_rss_feed(rss_url)
        def load_and_parse(url, item):
            link = url
            soup = load_soup(link)

            self.fix_image_links(soup)

            return dict(
                link = link,
                title = str(item.title.string),
                id = str(item.guid.string),
                content = self.extract_bodytext(soup),
            )
        self.entries = Cache.get_or_calc(urls, load_and_parse, items)

class TitanicBriefe(Feed):
    def __init__(self):
        Feed.__init__(self, 'titanic_briefe', 'Titanic Briefe an die Leser',
                      'Titanic Briefe fulltext',
                      'http://www.titanic-magazin.de/briefe')
        self.url = 'http://www.titanic-magazin.de/briefe/'

    def parse_item(self, item_soup):
        try:
            title = str(item_soup.h1.extract().string)
            content = str(item_soup)
        except:
            return None
        if content:
            return dict(
                    link = self.url,
                    title = title,
                    id = sha1(bytes(content, 'utf8')).hexdigest(),
                    content = content,
                )

    def crawl(self):
        soup = load_soup(self.url)
        briefe = soup.find('div', id='briefe')
        divs = briefe.find_all('div')
        potential_entries = [self.parse_item(item) for item in divs]
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
        return timestamp.replace(hour=timestamp.hour//6*6, minute=0, second=0)

    
    def parse_item(self, soup, item):
        timestamp = item.pubDate.string
        timestamp = timestamp.rsplit(' ', 1)[0] # remove timezone info
        timestamp = datetime.strptime(timestamp, '%a, %d %b %Y %H:%M:%S')
        if timestamp > self._current_timeblock:
            raise DoNotCache("Timeblock still open. Keep aggregating")
        try:
            link = soup.h2.a['href']
        except:
            link = "<no link>"
        return dict(
            link = str(link),
            rivva_link = str(item.link.string),
            title = str(item.title.string),
            id = str(item.guid.string),
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
            title = 'Rivva %s..%02i:00' % (
                timeblock.strftime('%Y-%m-%d, %H:%M'),
                timeblock.hour+6),
            id = items[0]['id'],
            content = content
        )

    def crawl(self):
        self._current_timeblock = self.timeblock(datetime.now())
        rss_url = 'http://feeds.feedburner.com/rivva'
        urls, items = load_and_parse_rss_feed(rss_url)

        def load_and_parse(url, item):
            soup = load_soup(url)
            return self.parse_item(soup, item)

        parsed_items = Cache.get_or_calc(urls, load_and_parse, items)

        parsed_items = [i for i in parsed_items if i]

        parsed_items.sort(key=lambda d: d['timestamp'])
        groups = itertools.groupby(parsed_items, lambda d: d['timeblock'])
        self.entries = [self.format_group(timeblock, list(parsed_items)) for timeblock, parsed_items
                        in groups]

        self.entries = self.entries[1:]  # remove oldest group (which 


class DauJonesRss(Feed):
    def __init__(self):
        Feed.__init__(self, 'daujones', 'Dau Jones', 'Dau Jones fulltext',
                      'http://www.daujones.com')

    def extract_bodytext(self, soup):
        maincontent = soup.find('div', {'class': 'maincontent'})

        # remove unwanted elements
        for unwanted in (
            ('div', {'class': 'rightnav'}),
            ('form',),
            ('center',),
        ):
            while maincontent.find(*unwanted) != None:
                maincontent.find(*unwanted).extract()

        # delete first two spans ('zurück'/'weiter' in header)
        maincontent.span.extract()
        maincontent.span.extract()

        # find next span ('weiter' in footer) -> remove this and elements below
        end = maincontent.span.previousSibling
        while end.nextSibling != None:
            end.nextSibling.extract()

        return str(maincontent)

    def crawl(self):
        rss_url = 'http://www.daujones.com/daubeitraege.rss'
        urls, items = load_and_parse_rss_feed(rss_url)
        def load_and_parse(url, item):
            soup = load_soup(url)

            return dict(
                link = url,
                title = str(item.title.string),
                id = url,
                content = self.extract_bodytext(soup),
            )
        self.entries = Cache.get_or_calc(urls, load_and_parse, items)

class DilbertRss(Feed):
    def __init__(self):
        Feed.__init__(self, 'dilbert', 'Dilbert', 'Dilbert images',
                      'http://www.dilbert.com')

    def crawl(self):
        today = datetime.today()
        last_21_days = (today+timedelta(days=n) for n in range(-20,1))
        urls = [d.strftime('http://dilbert.com/strips/comic/%Y-%m-%d')
                        for d in last_21_days]

        def load_and_parse(url):
            soup = load_soup(url)
            imgs = soup.find_all('img')
            for img in imgs:
                try:
                    src = img.attrs['src']
                    if 'strip.zoom' in src:
                        return dict(
                            link = url,
                            title = "Dilbert for %s" % url.split('/')[-1],
                            id = url,
                            content = '<img src="http://dilbert.com%s">' % src,
                        )
                except:
                    pass
            raise ('no image found')

        load_and_parse(urls[0])

        self.entries = Cache.get_or_calc(urls, load_and_parse)

class PostillonRss(Feed):
    def __init__(self):
        Feed.__init__(self, 'postillon', 'Postillon', 'Ehrliche Nachrichten',
                      'http://www.der-postillon.com/')

    def extract_bodytext(self, item_soup):
        return str(item_soup.find('div', {'class': 'post-body'}))

    def crawl(self):
        rss_url = 'http://feeds.feedburner.com/blogspot/rkEL'
        urls, items = load_and_parse_rss_feed(rss_url)
        def load_and_parse(url, item):
            link = url
            soup = load_soup(link)

            return dict(
                link = link,
                title = str(item.title.string),
                id = str(item.guid.string),
                content = self.extract_bodytext(soup),
            )
        self.entries = Cache.get_or_calc(urls, load_and_parse, items)

class TagespresseRss(Feed):
    def __init__(self):
        Feed.__init__(self, 'tagespresse', 'Tagespresse RSS', 'Tagespresse fulltext',
                      'http://www.dietagespresse.com')

    def extract_bodytext(self, item_soup):
        headline = item_soup.find('h1')
        main_div = headline.findParent('div')
        return str(main_div)

    def crawl(self):
        rss_url = 'https://dietagespresse.com/feed/'
        urls, items = load_and_parse_rss_feed(rss_url)
        def load_and_parse(url, item):
            link = url
            soup = load_soup(link)

            return dict(
                link = link,
                title = str(item.title.string),
                id = str(item.guid.string),
                content = self.extract_bodytext(soup),
            )
        #self.entries = [load_and_parse(u,i) for (u,i) in zip(urls, items)]
        self.entries = Cache.get_or_calc(urls, load_and_parse, items)


titanic = TitanicRss()
titanic_briefe = TitanicBriefe()
titanic_fachmann = TitanicFachmann()
rivva = RivvaRss()
daujones = DauJonesRss()
dilbert = DilbertRss()
postillon = PostillonRss()
tagespresse = TagespresseRss()


if __name__ == '__main__':
    for feed in Feed.feeds.values():
        feed.crawl()
        print(feed.entries)
