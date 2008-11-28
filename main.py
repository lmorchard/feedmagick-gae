#!/usr/bin/env python
"""
"""
# Find library locations relative to the working dir.
import sys, os
base_dir = os.path.dirname(__file__)
sys.path.extend([ os.path.join(base_dir, d) for d in 
    ( 'lib', 'extlib' ) 
])

import pickle, time, md5

import wsgiref.handlers
import feedparser, simplejson

import xml.sax
from xml.sax.saxutils import escape, unescape

from datetime import datetime

from google.appengine.ext import webapp
from google.appengine.ext import db
from google.appengine.api import urlfetch
from google.appengine.api import memcache

class MainHandler(webapp.RequestHandler):

    def get(self):
        out = []

        json = MyJSONEncoder()

        out.append('Hello world!')

        #f = Feed.get('agtmZWVkbWFnaWNrM3IvCxIERmVlZCIlRmVlZC0xNGY5NWMzNjAyMTcxNjUwOTU1YWZlYzViZWM5NzdmNgw')
        #out.append(f.url)

        """
        q = db.GqlQuery('SELECT * FROM QueuedMessage')
        results = q.fetch(1000)

        out.append("Count: %s" % len(results))
        out.append('<pre>')
        for result in results:
            out.append(escape('%s' % result))
        out.append('</pre>')
        db.delete(results)
        """

        q = db.GqlQuery('SELECT * FROM Feed')
        results = q.fetch(1000)

        out.append("Count: %s" % len(results))
        out.append('<pre>')
        for result in results:
            out.append(escape('%s' % result.key()))
        out.append('</pre>')

        db.delete(results)
        
        opml_url = self.request.get('opml')

        opml_data = self.urlfetch_cached(opml_url)

        if opml_data:
            opml_handler = OPMLHandler()
            parser = xml.sax.make_parser()
            parser.setFeature(xml.sax.handler.feature_namespaces, 1)
            parser.setContentHandler(opml_handler)
            parser.feed(opml_data['content'])

            feed_urls = [ x[u'xmlUrl'] for x in opml_handler.feeds if u'xmlUrl' in x ]
            for url in feed_urls:
                #feed = Feed(url=url)
                #feed.put()

                feed = Feed.get_or_insert(url=url)

                """
                msg = QueuedMessage(
                    topic='fetchfeed', 
                    body=json.encode({
                        'url': url
                    })
                )
                msg.put()
                """

                """
                feed_resp = self.urlfetch_cached(url)
                out.append('%s : %s = %s' % ( len(feed_resp['content']), 'cache_time' in feed_resp and feed_resp['cache_time'] or 'n/a', url ) )
                """
        
        self.response.out.write("<br />\n".join(out))

    def urlfetch_cached(self, url, payload=None, method='GET', headers=None, 
            allow_truncated=False, follow_redirects=True, cache_max_age=600):
        """Wrap urlfetch.fetch() calls in some caching magic."""

        # TODO: account for all parameters in the above
        cache_key = 'feedmagick:feed:%(url)s' % ({
            'url': url
        })

        # Try grabbing and unpickling cached results
        cache_data    = memcache.get(cache_key)
        cached_result = cache_data and pickle.loads(cache_data) or {}

        # Tolerate possibly stale cache for up to cache_max_age seconds
        if 'cache_time' in cached_result: 
            if time.time() - cached_result['cache_time'] < cache_max_age:
                cached_result['cache_hit'] = True
                return cached_result

        # If possible, prepare caching headers from previous request.
        if headers is None: headers = {}
        if method == 'GET' and 'headers' in cached_result:
            if 'Last-Modified' in cached_result['headers']:
                headers['If-Modified-Since'] = \
                    cached_result['headers']['Last-Modified']
            if 'ETag' in cached_result['headers']:
                headers['If-None-Match'] = \
                    cached_result['headers']['ETag']

        # Perform the actual URL fetch call.
        rv = urlfetch.fetch(
            url=url, payload=payload, method=method, headers=headers, 
            allow_truncated=allow_truncated, follow_redirects=follow_redirects
        )

        # Convert urlfetch response object into a plain old dict.
        result = dict([ 
            (x, getattr(rv, x)) for x in 
            ('content', 'status_code', 'headers', 'content_was_truncated')
        ])

        if result['status_code'] == 304:
            # On a not modified response, return the cached results.
            cached_result['cache_hit'] = True
            return cached_result
        else:
            # Otherwise, cache the fresh results and return them.
            result['cache_time'] = time.time()
            memcache.set(cache_key, pickle.dumps(result))
            result['cache_hit'] = False
            return result

class MyJSONEncoder(simplejson.JSONEncoder):

    def default(self, o):
        """
        Support arbitrary iterators for encoding.
        """
        try:
            iterable = iter(o)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self, o)

class OPMLHandler(xml.sax.ContentHandler):
    """ """
    def startDocument(self):
        self.feeds = []

    def startElementNS(self, name, qname, ns_attrs):
        if name[1] == 'outline':
            attrs = dict([ 
                (x, ns_attrs.getValueByQName(x)) 
                for x in ns_attrs.getQNames() 
            ])
            if u'xmlUrl' in attrs:
                self.feeds.append(attrs)

class UniqueModel(db.Model):
    pass

class Feed(db.Model):
    url           = db.LinkProperty(required=True)
    xml           = db.BlobProperty()
    title         = db.StringProperty()
    subtitle      = db.StringProperty()
    author        = db.StringProperty()
    updated       = db.DateTimeProperty(default=None)
    last_fetched  = db.DateTimeProperty(default=None)
    last_modified = db.StringProperty(default=None)
    etag          = db.StringProperty(default=None)

    @classmethod
    def buildKeyName(cls, kw):
        return 'Feed-%s' % md5.new(','.join(
            '%s=%s' % (key, kw.get(key, None)) 
            for key in 
            [ 'url' ]
        )).hexdigest()

    @classmethod
    def get_or_insert(cls, **kw):
        """ """
        if not 'key_name' in kw:
            kw['key_name'] = Feed.buildKeyName(kw)
        return db.Model.get_or_insert(cls, **kw)

    def __init__(self, parent=None, key_name=None, **kw):
        """ """
        if not key_name:
            key_name = Feed.buildKeyName(kw)
        return db.Model.__init__(self, parent, key_name, **kw)

    def __str__(self):
        return self.to_xml()

class Entry(db.Model):
    source    = db.ReferenceProperty(Feed)
    xml       = db.BlobProperty()
    id        = db.StringProperty()
    title     = db.StringProperty()
    link      = db.LinkProperty()
    published = db.DateTimeProperty()
    updated   = db.DateTimeProperty()
    content   = db.TextProperty()

def main():
    app = webapp.WSGIApplication([
        ('/', MainHandler)
    ], debug=True)

    import firepython.middleware.FirePythonWSGI
    app = firepython.middleware.FirePythonWSGI(app) 

    wsgiref.handlers.CGIHandler().run(app)

if __name__ == '__main__': main()
