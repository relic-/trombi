# Copyright (c) 2011 Jyrki Pulliainen <jyrki@dywypi.org>
# Copyright (c) 2010 Inoi Oy
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use, copy,
# modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
# BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""Asynchronous CouchDB client"""

import functools
from hashlib import sha1
import uuid
import logging
import re
import collections
import tornado.ioloop
import urllib

try:
    # Python 3
    from urllib.parse import quote as urlquote
    from urllib.parse import urlencode
except ImportError:
    # Python 2
    from urllib import quote as urlquote
    from urllib import urlencode

from base64 import b64encode, b64decode
from tornado.httpclient import AsyncHTTPClient
from tornado.httputil import HTTPHeaders
from tornado import gen

log = logging.getLogger('trombi')

try:
    import json
except ImportError:
    import simplejson as json

import trombi.errors
from trombi import exceptions as trombi_ex

def from_uri(uri, fetch_args=None, io_loop=None, **kwargs):
    try:
        # Python 3
        from urllib.parse import urlparse, urlunsplit
    except ImportError:
        # Python 2
        from urlparse import urlparse, urlunsplit

    p = urlparse(uri)
    if p.params or p.query or p.fragment:
        raise ValueError(
            'Invalid database address: %s (extra query params)' % uri)
    if not p.scheme in ('http', 'https'):
        raise ValueError(
            'Invalid database address: %s (only http:// and https:// are supported)' % uri)

    baseurl = urlunsplit((p.scheme, p.netloc, '', '', ''))
    server = Server(baseurl, fetch_args, io_loop=io_loop, **kwargs)

    db_name = p.path.lstrip('/').rstrip('/')
    return Database(server, db_name)


def _jsonize_params(params):
    result = dict()
    for key, value in params.items():
        result[key] = json.dumps(value)
    return urlencode(result)


def _exception(response):
    if response.code == 599:
        return trombi_ex.get_exception_by_code(599, 'Unable to connect to CouchDB')
        #return TrombiErrorResponse(599, 'Unable to connect to CouchDB')
    else:
        try:
            content = json.loads(response.body.decode('utf-8'))
        except ValueError:
            return trombi_ex.get_exception_by_code(response.code, response.body)
            #return TrombiErrorResponse(response.code, response.body)
        else:
            try:
                return trombi_ex.get_exception_by_code(response.code, content['reason'])
                #return TrombiErrorResponse(response.code, content['reason'])
            except (KeyError, TypeError):
                # TypeError is risen if the result is a list
                return trombi_ex.get_exception_by_code(response.code, content)
                #return TrombiErrorResponse(response.code, content)


class Server(object):
    def __init__(self, baseurl, fetch_args=None, io_loop=None,
                 json_encoder=None, **client_args):
        self.session_cookie = None
        self.baseurl = baseurl
        if self.baseurl[-1] == '/':
            self.baseurl = self.baseurl[:-1]
        if fetch_args is None:
            self._fetch_args = dict()
        else:
            self._fetch_args = fetch_args

        if io_loop is None:
            self.io_loop = tornado.ioloop.IOLoop.instance()
        else:
            self.io_loop = io_loop
        # We can assign None to _json_encoder as the json (or
        # simplejson) then defaults to json.JSONEncoder
        self._json_encoder = json_encoder
        self._client = AsyncHTTPClient(self.io_loop, **client_args)


    def _fetch(self, *args, **kwargs):
        # This is just a convenince wrapper for _client.fetch

        # Set default arguments for a fetch
        fetch_args = {
            'headers': HTTPHeaders({'Content-Type': 'application/json'})
        }
        fetch_args.update(self._fetch_args)
        fetch_args.update(kwargs)

        if self.session_cookie:
            fetch_args['X-CouchDB-WWW-Authenticate': 'Cookie']
            if 'Cookie' in fetch_args:
                fetch_args['Cookie'] += '; %s' % self.session_cookie
            else:
                fetch_args['Cookie'] = self.sesison_cookie

        self._client.fetch(*args, **fetch_args)


    @gen.engine
    def create(self, name, callback):
        if not VALID_DB_NAME.match(name):
            # Avoid additional HTTP Query by doing the check here
            raise trombi_ex.TrombiInvalidDatabaseName('Invalid database name: %r' % name)

        response = yield gen.Task(self._fetch, '%s/%s' % (self.baseurl, name), method='PUT', body='')
        
        if response.code == 201:
            callback(Database(self, name))
        elif response.code == 412:
            raise trombi_ex.TrombiPreconditionFailed('Database already exists: %r' % name)
        else:
            raise _exception(response)


    @gen.engine
    def get(self, name, callback, create=False):
        if not VALID_DB_NAME.match(name):
            raise trombi_ex.TrombiInvalidDatabaseName('Invalid database name: %r' % name)

        response = yield gen.Task(self._fetch, '%s/%s' % (self.baseurl, name))
        
        if response.code == 200:
            callback(Database(self, name))
        elif response.code == 404:
            # Database doesn't exist
            if create:
                self.create(name, callback)
            else:
                raise trombi_ex.TrombiNotFound('Database not found: %s' % name)
        else:
            raise _exception(response)

    @gen.engine
    def delete(self, name, callback):
        response = yield gen.Task(self._fetch, '%s/%s' % (self.baseurl, name), method='DELETE')
  
        if response.code == 200:
            callback(json.loads(response.body.decode('utf-8')))
        elif response.code == 404:
            raise trombi_ex.TrombiNotFound('Database does not exist: %r' % name)
        else:
            raise _exception(response)


    @gen.engine
    def list(self, callback):
        response = yield gen.Task(self._fetch, '%s/%s' % (self.baseurl, '_all_dbs'))
        
        if response.code == 200:
            body = response.body.decode('utf-8')
            callback(Database(self, x) for x in json.loads(body))
        else:
            raise _exception(response)


    def add_user(self, name, password, callback, doc=None):
        userdb = Database(self, '_users')

        if not doc:
            doc = {}

        doc['type'] = 'user'
        if 'roles' not in doc:
            doc['roles'] = []

        doc['name'] = name
        doc['salt'] = str(uuid.uuid4())
        doc['password_sha'] = sha1(password + doc['salt']).hexdigest()

        if '_id' not in doc:
            doc['_id'] = 'org.couchdb.user:%s' % name

        userdb.set(doc, callback)

    def get_user(self, name, callback, attachments=False):
        userdb = Database(self, '_users')

        doc_id = name
        if not name.startswith('org.couchdb.user:'):
            doc_id = 'org.couchdb.user:%s' % name

        userdb.get(doc_id, callback, attachments=attachments)

    def update_user(self, user_doc, callback):
        userdb = Database(self, '_users')
        userdb.set(user_doc, callback)

    @gen.engine
    def update_user_password(self, username, password, callback):
        user_doc = yield gen.Task(self.get_user, username)
        user_doc['password_sha'] = sha1(password + user_doc['salt']).hexdigest()
        self.update_user(user_doc, callback)

    def delete_user(self, user_doc, callback):
        userdb = Database(self, '_users')
        userdb.delete(user_doc, callback)

    @gen.engine
    def logout(self, callback):
        url = '%s/%s' % (self.baseurl, '_session')
        reponse = yield gen.Task(self._client.fetch, url, method='DELETE')
     
        if response.code == 200:
            self.session_cookie = None
            callback(json.loads(response.body))
        else:
            raise _exception(response)

    @gen.engine
    def login(self, username, password, callback):
        body = urllib.urlencode({'name': username, 'password': password})
        url = '%s/%s' % (self.baseurl, '_session')

        response = yield gen.Task(self._client.fetch, url, method='POST', body=body)
        
        if response.code in (200, 302):
            self.session_cookie = response.headers['Set-Cookie']
            response_body = json.loads(response.body)
            callback(response_body)
        else:
            raise _exception(response)

    @gen.engine
    def session(self, callback):
        url = '%s/%s' % (self.baseurl, '_session')
        response = yield gen.Task(self._client.fetch, url)
        
        if response.code == 200:
            body = json.loads(response.body)
            callback(body)
        else:
            raise _exception(response)
        


class Database(object):
    def __init__(self, server, name):
        self.server = server
        self._json_encoder = self.server._json_encoder
        self.name = name
        self.baseurl = '%s/%s' % (self.server.baseurl, self.name)

    def _fetch(self, url, *args, **kwargs):
        # Just a convenience wrapper
        if 'baseurl' in kwargs:
            url = '%s/%s' % (kwargs.pop('baseurl'), url)
        else:
            url = '%s/%s' % (self.baseurl, url)
        return self.server._fetch(url, *args, **kwargs)

    @gen.engine
    def info(self, callback):
        response = yield gen.Task(self._fetch, '')
        if response.code == 200:
            body = response.body.decode('utf-8')
            callback(json.loads(body))
        else:
            raise _exception(response)

    @gen.engine
    def set(self, *args, **kwargs):
        cb = kwargs.pop('callback', None)
        if cb:
            args += (cb,)
        if len(args) == 2:
            data, callback = args
            doc_id = None
        elif len(args) == 3:
            doc_id, data, callback = args
        else:
            raise TypeError(
                'Database.set takes at most 2 non-keyword arguments.')

        if kwargs:
            if list(kwargs.keys()) != ['attachments']:
                if len(kwargs) > 1:
                    raise TypeError(
                        '%s are invalid keyword arguments for this function') %(
                        (', '.join(kwargs.keys())))
                else:
                    raise TypeError(
                        '%s is invalid keyword argument for this function' % (
                            list(kwargs.keys())[0]))

            attachments = kwargs['attachments']
        else:
            attachments = {}

        if doc_id is None and data.get('_id', None) is not None and data.get('_rev', None) is not None:
            # Update the existing document
            doc_id = data['_id']

        if doc_id is not None:
            url = urlquote(doc_id, safe='')
            method = 'PUT'
        else:
            url = ''
            method = 'POST'

        if len(attachments) > 0 and '_attachments' not in data:
            data['_attachments'] = {}
            
        for name, attachment in attachments.items():
            content_type, attachment_data = attachment
            if content_type is None:
                content_type = 'text/plain'
            data['_attachments'][name] = {
                'content_type': content_type,
                'data': b64encode(attachment_data).decode('utf-8'),
                }

        response = yield gen.Task(self._fetch, url, method=method, body=json.dumps(data, cls=self._json_encoder))
        
        try:
            # If the connection to the server is malfunctioning,
            # ie. the simplehttpclient returns 599 and no body,
            # don't set the content as the response.code will not
            # be 201 at that point either
            if response.body is not None:
                content = json.loads(response.body.decode('utf-8'))
        except ValueError:
            content = response.body

        if response.code == 201:
            callback(content)
        else:
            raise _exception(response)


    @gen.engine
    def get(self, doc_id, callback, attachments=False):
        doc_id = urlquote(doc_id, safe='')

        kwargs = {}

        if attachments is True:
            doc_id += '?attachments=true'
            kwargs['headers'] = HTTPHeaders(
                {'Content-Type': 'application/json',
                 'Accept': 'application/json',
             })

        response = yield gen.Task(self._fetch, doc_id, **kwargs)
        
        if response.code == 200:
            data = json.loads(response.body.decode('utf-8'))
            callback(data)
            #elif response.code == 404:
                # Document doesn't exist
                #raise trombi_ex.TrombiNotFound('Document with id `%s` not found' % doc_id)
        else:
            raise _exception(response)


    @gen.engine
    def set_attachment(self, doc, name, data, callback, type='text/plain'):
        headers = {'Content-Type': type, 'Expect': ''}

        response = yield gen.Task(self._fetch,
            '%s/%s?rev=%s' % (
                urlquote(doc['_id'], safe=''),
                urlquote(name, safe=''),
                doc['_rev']),
            method='PUT',
            body=data,
            headers=headers)
        
        if response.code != 201:
            raise _exception(response)
        
        data = json.loads(response.body.decode('utf-8'))
        assert data['id'] == doc['_id']
        callback(data)
        

    @gen.engine    
    def get_attachment(self, doc_id, attachment_name, callback):
        doc_id = urlquote(doc_id, safe='')
        attachment_name = urlquote(attachment_name, safe='')

        response = yield gen.Task(self._fetch, '%s/%s' % (doc_id, attachment_name))
 
        if response.code == 200:
            callback(response.body)
            #elif response.code == 404:
                # Document or attachment doesn't exist
                #callback(None)
        else:
            raise _exception(response)

    @gen.engine
    def delete_attachment(self, doc, attachment_name, callback):
        response = yield gen.Task(self._fetch, '%s/%s?rev=%s' % (doc['_id'], attachment_name, doc['_rev']), method='DELETE')
 
        if response.code != 200:
            raise _exception(response)
        callback(json.loads(response.body.decode('utf-8')))
        

    @gen.engine    
    def copy(self, doc, new_id, callback):
        response = yield gen.Task(self._fetch,
            '%s' % urlquote(doc['_id'], safe=''),
            allow_nonstandard_methods=True,
            method='COPY',
            headers={'Destination': str(new_id)})

        if response.code != 201:
            raise _exception(response)

        content = json.loads(response.body.decode('utf-8'))
        callback(content)

            
    @gen.engine
    def view(self, design_doc, viewname, callback, **kwargs):
        if not design_doc and viewname == '_all_docs':
            url = '_all_docs'
        else:
            url = '_design/%s/_view/%s' % (design_doc, viewname)

        # We need to pop keys before constructing the url to avoid it
        # ending up twice in the request, both in the body and as a
        # query parameter.
        keys = kwargs.pop('keys', None)

        if kwargs:
            url = '%s?%s' % (url, _jsonize_params(kwargs))

        if keys is not None:
            response = yield gen.Task(self._fetch,url,
                        method='POST',
                        body=json.dumps({'keys': keys})
                        )
        else:
            response = yield gen.Task(self._fetch,url)

        if response.code == 200:
            body = response.body.decode('utf-8')
            callback(json.loads(body))
        else:
            raise _exception(response)

    @gen.engine
    def list(self, design_doc, listname, viewname, callback, **kwargs):
        url = '_design/%s/_list/%s/%s/' % (design_doc, listname, viewname)
        if kwargs:
            url = '%s?%s' % (url, _jsonize_params(kwargs))

        response = yield gen.Task(self._fetch, url)
        if response.code == 200:
            callback(response.body)
        else:
            raise _exception(response)

    @gen.engine
    def temporary_view(self, callback, map_fun, reduce_fun=None,
                       language='javascript', **kwargs):
        url = '_temp_view'
        if kwargs:
            url = '%s?%s' % (url, _jsonize_params(kwargs))

        body = {'map': map_fun, 'language': language}
        if reduce_fun:
            body['reduce'] = reduce_fun

        response = yield gen.Task(self._fetch,url, method='POST',
                    body=json.dumps(body),
                    headers={'Content-Type': 'application/json'})

        if response.code == 200:
            body = response.body.decode('utf-8')
            callback(json.loads(body))
        else:
            raise _exception(response)


    @gen.engine
    def delete(self, data, callback):
        # TODO/ mb add validation
        doc_id = urlquote(data['_id'], safe='')
        response = yield gen.Task(self._fetch,
            '%s?rev=%s' % (doc_id, data['_rev']),
            method='DELETE',
            )
         
        try:
            content = json.loads(response.body.decode('utf-8'))
        except ValueError:
            raise _exception(response)

        if response.code == 200:
            callback(content)
        else:
            raise _exception(response)


    @gen.engine
    def bulk_docs(self, data, callback, all_or_nothing=False):
        docs = []
        for element in data:
            if isinstance(element, Document):
                docs.append(element.raw())
            else:
                docs.append(element)

        payload = {'docs': docs}
        if all_or_nothing is True:
            payload['all_or_nothing'] = True

        response = yield gen.Task(self._fetch, '_bulk_docs', method='POST', body=json.dumps(payload))
 
        if response.code == 200 or response.code == 201:
            try:
                content = json.loads(response.body.decode('utf-8'))
            except ValueError:
                raise _exception(response)
            else:
                callback(content)
        else:
            raise _exception(response)


    @gen.engine
    def changes(self, callback, timeout=None, feed='normal', **kw):
        stream_buffer = []

        def _stream(text):
            stream_buffer.append(text.decode('utf-8'))
            chunks = ''.join(stream_buffer).split('\n')

            # The last chunk is either an empty string or an
            # incomplete line. Save it for the next round. The [:]
            # syntax is used because of variable scoping.
            stream_buffer[:] = [chunks.pop()]

            for chunk in chunks:
                if not chunk.strip():
                    continue

                try:
                    obj = json.loads(chunk)
                except ValueError:
                    # JSON parsing failed. Apparently we have some
                    # gibberish on our hands, just discard it.
                    log.warning('Invalid changes feed line: %s' % chunk)
                    continue

                # "Escape" the streaming_callback context by invoking
                # the handler as an ioloop callback. This makes it
                # possible to start new HTTP requests in the handler
                # (it is impossible in the streaming_callback
                # context). Tornado runs these callbacks in the order
                # they were added, so this works correctly.
                #
                # This also relieves us from handling exceptions in
                # the handler.
                cb = functools.partial(callback, obj)
                self.server.io_loop.add_callback(cb)

        couchdb_params = kw
        couchdb_params['feed'] = feed
        params = dict()
        if timeout is not None:
            # CouchDB takes timeouts in milliseconds
            couchdb_params['timeout'] = timeout * 1000
            params['request_timeout'] = timeout + 1
        url = '_changes?%s' % urlencode(couchdb_params)
        if feed == 'continuous':
            params['streaming_callback'] = _stream

        log.debug('Fetching changes from %s with params %s', url, params)
        response = yield gen.Task(self._fetch, url, **params)

        log.debug('Changes feed response: %s', response)
        if response.code != 200:
            raise _exception(response)
        if feed == 'continuous':
            # Feed terminated, call callback with None to indicate
            # this, if the mode is continous
            callback(None)
        else:
            body = response.body.decode('utf-8')
            callback(json.loads(body))


class Paginator(object):
    """
    Provides pseudo pagination of CouchDB documents calculated from
    the total_rows and offset of a CouchDB view as well as a user-
    defined page limit.
    """
    def __init__(self, db, limit=10):
        self._db = db
        self._limit = limit
        self.response = None
        self.count = 0
        self.start_index = 0
        self.end_index = 0
        self.num_pages = 0
        self.current_page = 0
        self.previous_page = 0
        self.next_page = 0
        self.rows = None
        self.has_next = False
        self.has_previous = False
        self.page_range = None
        self.start_doc_id = None
        self.end_doc_id = None
 
    # Dirty hack
    @gen.engine
    def get_page(self, design_doc, viewname, callback,
            key=None, doc_id=None, forward=True, **kw):
        """
        On success, callback is called with this Paginator object as an
        argument that is fully populated with the page data requested.

        Use forward = True for paging forward, and forward = False for
        paging backwargs

        The combination of key/doc_id and forward is crucial.  When
        requesting to paginate forward the key/doc_id must be the built
        from the _last_ document on the current page you are moving forward
        from.  When paginating backwards, the key/doc_id must be built
        from the _first_ document on the current page.

        """
        def _really_callback(response):
            if response.error:
                # Send the received Database.view error to the callback
                callback(response)
                return

            if forward:
                offset = response.offset
            else:
                offset = response.total_rows - response.offset - self._limit

            self.response = response
            self.count = response.total_rows
            self.start_index = offset
            self.end_index = response.offset + self._limit - 1
            self.num_pages = (self.count / self._limit) + 1
            self.current_page = (offset / self._limit) + 1
            self.previous_page = self.current_page - 1
            self.next_page = self.current_page + 1
            self.rows = [row['value'] for row in response]
            if not forward:
                self.rows.reverse()
            self.has_next = (offset + self._limit) < self.count
            self.has_previous = (offset - self._limit) >= 0
            self.page_range = [p for p in xrange(1, self.num_pages+1)]
            try:
                self.start_doc_id = self.rows[0]['_id']
                self.end_doc_id = self.rows[-1]['_id']
            except (IndexError, KeyError):
                # empty set
                self.start_doc_id = None
                self.end_doc_id = None
            callback(self)

        kwargs = {'limit': self._limit,
                  'descending': True}
        kwargs.update(kw)

        if 'startkey' not in kwargs:
            kwargs['startkey'] = key

        if kwargs['startkey'] and forward and doc_id:
            kwargs['start_doc_id'] = doc_id
        elif kwargs['startkey'] and not forward:
            kwargs['start_doc_id'] = doc_id if doc_id else ''
            kwargs['descending'] = False if kwargs['descending'] else True
            kwargs['skip'] = 1

        response = yield gen.Taskself(self._db.view, design_doc, viewname, **kwargs)
        _really_callback(response)


VALID_DB_NAME = re.compile(r'^[a-z][a-z0-9_$()+-/]*$')
