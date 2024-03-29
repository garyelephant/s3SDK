import os
import time
import json
import types
import datetime
import re
import hmac
import base64
import hashlib
import urllib
import httplib
import mimetypes


def ftype( f ):
    tp = mimetypes.guess_type( f )[ 0 ]
    return tp or 'application/octet-stream'

def fsize( f ):
    try:
        return os.path.getsize( f )
    except OSError, e:
        raise

def encode_multipart_formdata( fields, files ):
    """
    fields is a sequence of (name, value) elements for regular form fields.
    files is a sequence of (name, filename, value) elements for data to be uploaded as files
    Return (content_type, body) ready for httplib.HTTP instance
    """
    BOUNDARY = '---------------------------this_boundary$'
    CRLF = '\r\n'

    L = []
    for key, value in fields:
        L.append( '--' + BOUNDARY )
        L.append( 'Content-Disposition: form-data; name="%s"' % ( key, ) )
        L.append( '' )
        L.append( value )

    for key, filename, value in files:
        L.append( '--' + BOUNDARY )
        L.append( 'Content-Disposition: form-data; name="%s"; filename="%s"' % \
                    ( key, filename, ) )
        L.append( 'Content-Type: %s' % ftype( filename, ) )
        L.append( '' )
        L.append( value )
    L.append( '--' + BOUNDARY + '--' )
    L.append( '' )

    body = CRLF.join( L )
    content_type = 'multipart/form-data; boundary=%s' % ( BOUNDARY, )

    return content_type, body


class S3Error( Exception ): pass

class S3HTTPError( S3Error ): pass
class S3HTTPCodeError( S3HTTPError ): pass

class S3ResponseError( S3Error ): pass


class S3( object ):
    """
    python SDK for Sina Storage Service
    Auther : Slasher( shenjie1@staff.sina.com.cn )
    SVN : svn checkout http://sinastorage-clients.googlecode.com/svn/trunk/ sinastorage-clients-read-only
    Original Docs: http://sinastorage.sinaapp.com/developer/interface/aws/operate_object.html
    """

    CHUNK = 1024 * 1024
    DEFAULT_DOMAIN = 'sinastorage.com'
    DEFAULT_UP_DOMAIN = 'up.sinastorage.com'

    QUERY_SIGNATURE_KEY = ( 'ip', 'uploadId', 'partNumber' )
    GET_UNSET_HEADER_KEY = ( 's-sina-sha1', 'content-sha1', 's-sina-md5',
                             'content-md5', 'content-type', )


    def __init__( self, accesskey = None,
                        secretkey = None,
                        project = None ):

        self.accesskey = accesskey or 'SYS0000000000SANDBOX'
        self.ACCESSKEY = accesskey or 'SYS0000000000SANDBOX'

        if len( self.accesskey ) != len( 'SYS0000000000SANDBOX' ) \
                or '0' not in self.accesskey:
            raise S3Error, "accesskey '%s' is illegal." % \
                    ( self.accesskey, )

        self.nation = self.accesskey.split( '0' )[0].lower()
        self.nation = 'sae' if self.nation == '' else self.nation

        if self.nation == 'sae':
            self.accesskey = self.accesskey[ -10: ].lower()
        else:
            self.accesskey = self.accesskey.split( '0' )[-1].lower()

        self.secretkey = secretkey or '1' * 40
        self.project = project or 'sandbox'

        self.domain = S3.DEFAULT_DOMAIN
        self.port = 80
        self.timeout = 60

        self.expires = None

        self.need_auth = False

        self.is_ssl = False
        self.ssl_auth = {}

        self.vhost = False

    def set_https( self, ssl = True,
                         port = 443,
                         timeout = 180,
                         **kwargs ):

        self.is_ssl = bool( ssl )
        self.port = port
        self.timeout = timeout

        self.ssl_auth[ 'key_file' ] = kwargs.get( 'key_file', '' )
        self.ssl_auth[ 'cert_file' ] = kwargs.get( 'cert_file', '' )


    def get_upload_idc( self ):

        self.domain = S3.DEFAULT_UP_DOMAIN

        verb = 'GET'
        uri = '/?extra&op=domain.json'

        out = self._http_requst( verb, uri, out = True )

        return json.loads( out )

    def get_upload_id( self, key, query = None, headers = None ):

        self.domain = S3.DEFAULT_UP_DOMAIN

        verb = 'POST'
        extra = '?uploads'

        rh = { 'Content-Type' : ftype( key ) }
        rh.update( headers or {} )

        uri = self._get_uri( verb, key, extra = extra, \
                             query_string = query, requst_header = rh )

        out = self._http_requst( verb, uri, requst_header = rh, out = True )

        r = re.compile( '<UploadId>(.{32})</UploadId>' )
        r = r.search( out )

        if r:
            return r.groups()[0]
        else:
            raise S3ResponseError, out

    def get_list_parts( self, key, uploadid, query = None, headers = None ):

        self.domain = S3.DEFAULT_UP_DOMAIN

        verb = 'GET'

        qs = { 'uploadId' : str( uploadid ) }
        qs.update( query or {} )

        uri = self._get_uri( verb, key, \
                             query_string = qs, requst_header = headers )

        out = self._http_requst( verb, uri, \
                                 requst_header = headers, out = True )

        tr = re.compile( '<IsTruncated>(True|False)</IsTruncated>' )
        tr = tr.search( out )

        if tr and tr.groups()[0] == 'True':
            tr = True
        else:
            tr = False

        pr = re.compile( '<PartNumber>([0-9]*)</PartNumber>' )
        pr = pr.findall( out )

        if pr:
            pr = [ int( i ) for i in pr ]
            pr.sort()
        else:
            tr = True
            pr = []

        return pr[ : ]

    def upload_part_file( self, key, uploadid, partnum, partfile, query = None, headers = None ):

        self.domain = S3.DEFAULT_UP_DOMAIN

        verb = 'PUT'

        qs = { 'uploadId' : str( uploadid ), 'partNumber' : str( partnum ) }
        qs.update( query or {} )

        rh = { 'Content-Type' : ftype( partfile ), \
               'Content-Length' : str( fsize( partfile ) ) }
        rh.update( headers or {} )

        uri = self._get_uri( verb, key, query_string = qs, requst_header = rh )

        return self._http_requst( verb, uri, requst_header = rh, infile = partfile )

    def upload_part_data( self, key, uploadid, partnum, partdata, query = None, headers = None ):

        self.domain = S3.DEFAULT_UP_DOMAIN

        verb = 'PUT'

        qs = { 'uploadId' : str( uploadid ), 'partNumber' : str( partnum ) }
        qs.update( query or {} )

        rh = { 'Content-Type' : ftype( '' ), \
               'Content-Length' : str( len( partdata ) ) }
        rh.update( headers or {} )

        uri = self._get_uri( verb, key, query_string = qs, requst_header = rh )

        return self._http_requst( verb, uri, requst_header = rh, indata = partdata )

    def merge_parts_file( self, key, uploadid, mergefile, query = None, headers = None ):

        self.domain = S3.DEFAULT_UP_DOMAIN

        verb = 'POST'

        qs = { 'uploadId' : str( uploadid ) }
        qs.update( query or {} )

        rh = { 'Content-Type' : 'text/xml', \
               'Content-Length' : str( fsize( mergefile ) ) }
        rh.update( headers or {} )

        uri = self._get_uri( verb, key, query_string = qs, requst_header = rh )

        return self._http_requst( verb, uri, requst_header = rh, infile = mergefile )

    def merge_parts_data( self, key, uploadid, mergedata, query = None, headers = None ):

        self.domain = S3.DEFAULT_UP_DOMAIN

        verb = 'POST'

        qs = { 'uploadId' : str( uploadid ) }
        qs.update( query or {} )

        rh = { 'Content-Type' : 'text/xml', \
               'Content-Length' : str( len( mergedata ) ) }
        rh.update( headers or {} )

        uri = self._get_uri( verb, key, query_string = qs, requst_header = rh )

        return self._http_requst( verb, uri, requst_header = rh, indata = mergedata )


    def post_file( self, key, fn, headers = None, fields = None ):

        uri = '/'
        host = self.project + '.' + S3.DEFAULT_DOMAIN
        hd = { 'Host' : host }
        hd.update( headers or {} )

        fd = [ ( 'key', key ) ]

        policy, ssig = self._get_signature_policy()
        fd += [ ( 'AWSAccessKeyId', self.ACCESSKEY ),
                ( 'Policy', policy ),
                ( 'Signature', ssig ), ]

        if fields is not None:
            for k, v in fields.items():
                fd += [ ( k, v ) ]

        content = ''
        with open( fn, 'rb' ) as fhandle:
            while True:
                data = fhandle.read( self.CHUNK )
                if data == '':
                    break
                content += data

        resp = self._mulitpart_post( uri,
                                     fields = fd,
                                     files = [ ( 'file', fn, content ) ],
                                     headers = hd )

        if resp.status not in ( httplib.OK, \
                                httplib.CREATED, \
                                httplib.NO_CONTENT ):
            raise S3HTTPCodeError, self._resp_format( resp )

        return self._resp_format( resp )


    def upload_file( self, key, fn, query = None, headers = None ):

        verb = 'PUT'

        rh = { 'Content-Type' : ftype( fn ), \
               'Content-Length' : str( fsize( fn ) ) }
        rh.update( headers or {} )

        uri = self._get_uri( verb, key, query_string = query, requst_header = rh )

        return self._http_requst( verb, uri, requst_header = rh, infile = fn )

    def upload_data( self, key, data, query = None, headers = None ):

        verb = 'PUT'

        rh = { 'Content-Type' : ftype( key ), \
               'Content-Length' : str( len( data ) ) }
        rh.update( headers or {} )

        uri = self._get_uri( verb, key, query_string = query, requst_header = rh )

        return self._http_requst( verb, uri, requst_header = rh, indata = data )

    def upload_relax( self, key, fsha1, flen, query = None, headers = None ):

        verb = 'PUT'
        extra = '?relax'

        rh = { 'Content-Length' : str( 0 ), \
               's-sina-sha1' : str( fsha1 ), \
               's-sina-length' : str( flen ) }
        rh.update( headers or {} )

        uri = self._get_uri( verb, key, extra = extra, \
                             query_string = query, requst_header = rh )

        return self._http_requst( verb, uri, requst_header = rh )

    def copy_file( self, key, src, project = None, query = None, headers = None ):

        prj = str( project or self.project )

        verb = 'PUT'
        extra = '?copy'

        rh = { 'Content-Length' : str( 0 ), \
               'x-amz-copy-source' :  "/%s/%s" % ( prj, src, ) }
        rh.update( headers or {} )

        uri = self._get_uri( verb, key, extra = extra, \
                             query_string = query, requst_header = rh )

        return self._http_requst( verb, uri, requst_header = rh )

    def copy_file_from_project( self, key, src, project, query = None, headers = None ):

        return self.copy_file( key, src, project, query = query, headers = headers )

    def get_file( self, key, query = None, headers = None ):

        verb = 'GET'

        uri = self._get_uri( verb, key, query_string = query, requst_header = headers )

        return self._http_requst( verb, uri, requst_header = headers, out = True )

    def get_file_url( self, key, query = None, headers = None ):

        verb = 'GET'
        uri = self._get_uri( verb, key, query_string = query, requst_header = headers )

        url = '{domain}:{port}{uri}'.format( domain = self.domain,
                                             port = self.port,
                                             uri = uri, )

        return url

    def get_file_meta( self, key, query = None, headers = None ):

        verb = 'GET'
        extra = '?meta'

        uri = self._get_uri( verb, key, extra = extra, \
                             query_string = query, requst_header = headers )

        return self._http_requst( verb, uri, requst_header = headers, out = True )

    def get_project_list( self, query = None, headers = None ):

        verb = 'GET'

        qs = { 'formatter' : 'json' }
        qs.update( query or {} )

        uri = self._get_uri( verb, query_string = qs, requst_header = headers )

        return self._http_requst( verb, uri, requst_header = headers, out = True )

    def get_files_list( self, prefix = None, \
                              marker = None, \
                              maxkeys = None, \
                              delimiter = None, \
                              query = None, headers = None ):

        verb = 'GET'

        qs = { 'formatter' : 'json', \
               'prefix' : str( prefix or '' ), \
               'marker' : str( marker or '' ), \
               'max-keys' : str( maxkeys or 10 ), \
               'delimiter' : str( delimiter or '' ) }
        qs.update( query or {} )

        uri = self._get_uri( verb, query_string = qs, requst_header = headers )

        return self._http_requst( verb, uri, requst_header = headers, out = True )

    def update_file_meta( self, key, meta = None, query = None, headers = None ):

        verb = 'PUT'
        extra = '?meta'

        rh = { 'Content-Length' : str( 0 ) }
        metas = meta or {}

        meta = {}
        meta.update( headers or {} )
        meta.update( metas )
        for k in meta:
            if k.lower() in ( 'content-md5', 'content-type', \
                              'content-length', 'content-sha1' ):
                continue
            rh[ k ] = str( meta[ k ] )

        uri = self._get_uri( verb, key, extra = extra, \
                             query_string = query, requst_header = rh )

        return self._http_requst( verb, uri, requst_header = rh )

    def delete_file( self, key, query = None ):

        verb = 'DELETE'

        uri = self._get_uri( verb, key, query_string = query )

        return self._http_requst( verb, uri, httpcode = httplib.NO_CONTENT )


    def _http_requst( self, verb, uri, \
                            requst_header = None, \
                            infile = None, \
                            indata = None, \
                            out = False, \
                            httpcode = None ):

        requst_header = requst_header or {}

        verb = verb.upper()
        code = int( httpcode or httplib.OK )

        if indata is not None:
            resp = self._requst_put_data( verb, uri, requst_header, indata )
        elif infile is not None:
            resp = self._requst_put_file( verb, uri, requst_header, infile )
        else:
            resp = self._requst( verb, uri, requst_header )

        if resp.status != code:
            raise S3HTTPCodeError, self._resp_format( resp )

        if out:
            data = ''
            while True:
                chunk = resp.read( self.CHUNK )

                if chunk == '':
                    break
                data += chunk

            return data

        return self._resp_format( resp )

    def _resp_format( self, resp ):

        d = { 'status' : resp.status,
              'reason' : resp.reason,
              'out' : resp.read() }

        return d

    def _requst( self, verb, uri, rh ):

        header = self._fix_requst_header( verb, rh )

        h = self._http_handle()
        h.putrequest( verb, uri )
        for k in header:
            h.putheader( k, header[ k ] )
        h.endheaders()

        resp = h.getresponse()

        return resp

    def _requst_put_file( self, verb, uri, rh, fn ):

        header = self._fix_requst_header( verb, rh )

        try:
            h = self._http_handle()
            h.putrequest( verb, uri )
            for k in header:
                h.putheader( k, header[ k ] )
            h.endheaders()

            f = open( fn, 'rb' )

            while True:
                data = f.read( self.CHUNK )
                if data == '':
                    break
                h.send( data )

            resp = h.getresponse()

            return resp

        except:
            raise

        finally:
            try:
                f.close()
            except:
                pass

    def _requst_put_data( self, verb, uri, rh, data ):

        header = self._fix_requst_header( verb, rh )

        h = self._http_handle()
        h.putrequest( verb, uri )
        for k in header:
            h.putheader( k, header[ k ] )
        h.endheaders()

        h.send( data )

        resp = h.getresponse()

        return resp

    def _http_handle( self ):
        httplib.HTTPConnection._http_vsn = 11
        httplib.HTTPConnection._http_vsn_str = 'HTTP/1.1'

        if self.vhost and self.domain != S3.DEFAULT_UP_DOMAIN:
            host = self.project
        else:
            host = self.domain

        if self.is_ssl:
            h = httplib.HTTPSConnection( host, self.port,
                                         timeout = self.timeout,
                                         **self.ssl_auth )
        else:
            h = httplib.HTTPConnection( host, self.port,
                                        timeout = self.timeout )

        return h

    def _fix_requst_header( self, verb, requst_header ):

        rh = {}
        for k, v in requst_header.items():
            if verb == 'GET':
                lk = k.lower()
                if lk in S3.GET_UNSET_HEADER_KEY or \
                    lk.startswith( 'x-sina-' ) or \
                    lk.startswith( 'x-amz-' ):
                    continue

            if type( v ) == types.UnicodeType:
                v = v.encode( 'utf-8' )

            rh[ k ] = v

        return rh


    def _generate_query_string( self, query_string, sign = True ):

        qs = {}
        for k, v in query_string.items():
            if sign and k in self.QUERY_SIGNATURE_KEY:
                qs[ k ] = v
            elif ( not sign ) and ( k not in self.QUERY_SIGNATURE_KEY ):
                qs[ k ] = v

        qs = [ '%s=%s' % ( urllib.quote_plus( k ), urllib.quote_plus( v ) ) \
                            if k not in self.QUERY_SIGNATURE_KEY else \
                                '%s=%s' % ( k, v )
                                    for k, v in qs.items() ]
        qs.sort()
        qs = '&'.join( qs )

        return qs + '&' if qs != '' else ''

    def _generate_requst_header( self, requst_header ):

        rh = dict( [ ( k.lower(), v.encode( 'utf-8' ) ) \
                if type( v ) == types.UnicodeType else \
                    ( k.lower(), str( v ) )
                        for k, v in requst_header.items() ] )

        for t in ( 's-sina-sha1', 'content-sha1', \
                's-sina-md5', 'content-md5' ):
            if t in rh:
                rh[ 'hash-info' ] = rh[ t ]
                break

        return rh

    def _generate_expires( self ):
        et = type( self.expires )
        if et in ( types.IntType, types.LongType, types.FloatType ):
            dt = int( self.expires )
        elif et in types.StringTypes:
            dt = self.expires
        elif et == datetime.timedelta:
            dt = datetime.datetime.utcnow() + self.expires
            dt = dt.strftime( '%s' )
        elif et == datetime.datetime :
            dt = dt.strftime( '%s' )
        else:
            dt = time.time().__int__() + 30 * 60

        return str( dt )

    def _signature( self, strtosign ):
        ssig = hmac.new( self.secretkey, \
                            strtosign, \
                            hashlib.sha1 ).digest().encode( 'base64' )

        return ssig

    def _get_uri( self, verb, key = None, \
                               extra = '?', \
                               query_string = None, \
                               requst_header = None ):

        query_string = query_string or {}
        requst_header = requst_header or {}

        verb = verb.upper()
        key = '/' + ( key or '' )

        uri = key if self.vhost and self.domain != S3.DEFAULT_UP_DOMAIN \
                    else "/" + str( self.project ) + key

        extra = extra if extra.startswith( '?' ) else '?' + extra
        uri += extra if extra == '?' else extra + '&'
        uri += self._generate_query_string( query_string, sign = True )

        if not self.need_auth:
            uri += self._generate_query_string( query_string, sign = False )

            return uri.rstrip( '?&' )

        if verb == 'GET':
            hashinfo = ''
            ct = ''
            mts = []
        else:
            rh = self._generate_requst_header( requst_header )
            hashinfo = rh.get( 'hash-info', '' )
            ct = rh.get( 'content-type', '' )

            mts = [ k + ':' + v for k, v in rh.items() \
                    if k.startswith( 'x-sina-' ) or \
                        k.startswith( 'x-amz-' ) ]
            mts.sort()

        dt = self._generate_expires()

        stringtosign = '\n'.join( [ verb, hashinfo, ct, dt ] + \
                                mts + [ uri.rstrip( '?&' ) ] )
        #print stringtosign
        ssig = self._signature( stringtosign )

        uri += self._generate_query_string( query_string, sign = False )
        uri += "&".join( [  "KID=" + self.nation + "," + self.accesskey,
                            "Expires=" + dt,
                            "ssig=" + urllib.quote_plus( ssig[5:15] ), ] )

        return uri.rstrip( '?&' )


    def _generate_expires_policy( self ):
        fmt = '%Y-%m-%dT%H:%M:%S GMT'
        et = type( self.expires )
        if et in ( types.IntType, types.LongType, types.FloatType ):
            dt = time.strftime( fmt, time.localtime( int( self.expires ) ) )
        elif et == datetime.timedelta :
            dt = datetime.datetime.utcnow() + self.expires
            dt = dt.strftime( fmt )
        elif et == datetime.datetime :
            dt = dt.strftime( fmt )
        elif et == types.NoneType :
            dt = datetime.datetime.utcnow()
            dt += datetime.timedelta( seconds = 30 * 60 )
            dt = dt.strftime( fmt )
        else:
            dt = time.time().__int__() + 30 * 60
            dt = time.strftime( fmt, time.localtime( dt ) )

        dt = dt[:-4] + '.000Z'

        return dt

    def _get_signature_policy( self ):
        policy = { 'expiration' : self._generate_expires_policy(),
                   'conditions' : [ { 'bucket' : self.project },
                                    [ 'starts-with', '$key', '' ] ], }

        policy = base64.b64encode( json.dumps( policy ).encode( 'utf-8' ) )
        ssig = base64.b64encode( hmac.new( self.secretkey, \
                                        policy, hashlib.sha1 ).digest() )

        return policy, ssig

    def _mulitpart_post( self, uri, fields = [], files = [], headers = None ):
        content_type, body = encode_multipart_formdata( fields, files )

        h = {   'content-type': content_type,
                'content-length': str( len( body ) ), }
        h.update( headers or {} )

        conn = self._http_handle()
        conn.request( 'POST', uri, body, h )

        resp = conn.getresponse()

        return resp


