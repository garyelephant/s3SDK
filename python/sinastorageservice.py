import os
import sys
import stat
import copy
import time
import types
import datetime

import re
import hmac
import hashlib
import urllib
import httplib
import mimetypes



def ftype( f ):
    tp = mimetypes.guess_type( f )[ 0 ]
    return tp or ''


def fsize( f ):
    return os.path.getsize( f )



class S3Error( Exception ): pass

class S3HTTPError( S3Error ): pass
class S3HTTPCodeError( S3HTTPError ): pass

class S3ResponseError( S3Error ): pass


class S3( object ):

    """
    python SDK for Sina Storage Service
    SVN : svn checkout http://sinastorage-clients.googlecode.com/svn/trunk/ sinastorage-clients-read-only
    Original Docs: http://sinastorage.sinaapp.com/developer/interface/aws/operate_object.html
    """

    DEFAULT_DOMAIN = 'sinastorage.com'
    DEFAULT_UP_DOMAIN = 'up.sinastorage.com'

    CHUNK = 1024 * 1024

    EXTRAS = [ 'copy', ]
    QUERY_STRINGS = [ 'ip', 'foo', ]
    REQUST_HEADER = [ 'x-sina-info', 'x-sina-info-int',  ]
    QUERY_EXTEND = [ 'formatter', 'urlencode', 'rd', 'fn', 'Cheese',
                     'delimiter', 'marker', 'max-keys', 'prefix',
                     ]

    VERB2HTTPCODE = { 'DELETE' : httplib.NO_CONTENT }

    def __init__( self, accesskey = None,
                        secretkey = None,
                        project = None ):

        self.accesskey = accesskey or 'SYS0000000000SANDBOX'

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

        self.reset()


    def reset( self ):

        self.domain = self.DEFAULT_DOMAIN
        self.up_domain = self.DEFAULT_UP_DOMAIN

        self.port = 80
        self.timeout = 60

        self.expires = time.time().__int__() + 30 * 60

        self.extra = '?'
        self.query_string = {}
        self.requst_header = {}
        self.query_extend = {}

        self.is_ssl = False
        self.ssl_auth = {}

        self.need_auth = False

        self.vhost = False

        self._reset_intra()

    def _reset_intra( self ):

        self.intra_query = {}
        self.intra_header = {}
        self.intra_query_extend = {}


    def set_attr( self, **kwargs ):

        for k in kwargs:
            try:
                fun = getattr( self, 'set_' + k )

                if isCallable( fun ):
                    fun( kwargs[ k ] )

            except AttributeError:
                continue

    def set_https( self, ssl = True,
                         port = 4443,
                         timeout = 180,
                         **kwargs ):
        self.is_ssl = ssl
        self.port = port
        self.timeout = timeout

        self.ssl_auth['key_file'] = kwargs.get( 'key_file', '')
        self.ssl_auth['cert_file'] = kwargs.get( 'cert_file', '')

    def set_domain( self, domain ):
        self.domain = domain

    def set_port( self, port ):
        self.port = int( port )

    def set_timeout( self, timeout ):
        self.timeout = int( timeout )

    def set_expires( self, expires ):
        self.expires = expires

    def set_expires_delta( self, delta ):
        self.expires = time.time().__int__() + int( delta )

    def set_extra( self, extra = '?' ):
        self.extra = extra

    def set_need_auth( self, auth = True ):
        self.need_auth = auth

    def set_vhost( self, vhost = True ):

        if vhost:
            self.domain = self.project
        else:
            self.domain = self.DEFAULT_DOMAIN

        self.vhost = bool( vhost )

    def set_query_string( self, qs = None ):

        self.query_string.update( qs or {} )

    def set_requst_header( self, rh = None ):

        self.requst_header.update( rh or {} )

    def set_query_extend( self, qs = None ):

        self.query_extend.update( qs or {} )


    # large file upload steps:
    # 1. get upload idc : get a domain to hold during uploading a file
    # 2. get upload id  : get a uploadid to bind during uploading parts
    # 3. upload part    : upload a part
    # 4. list parts     : list the parts that are uploaded to server
    # 5. merge part     : merge all parts after uplaod all parts

    def get_upload_idc( self ):

        func = "get_upload_idc error='{error}'"

        self.set_domain( self.up_domain )

        verb = 'GET'
        uri = '/?extra&op=domain.json'

        out = self._normal_return( func, verb, uri, out = True )

        domain = out.strip().strip( '"' )

        return domain


    def get_upload_id( self, key, ct = None ):

        func = "get_upload_id error='{error}'"

        if self.domain == self.DEFAULT_DOMAIN:
            self.set_domain( self.up_domain )

        self.intra_query[ None ] = 'uploads'
        self.intra_header[ 'Content-Type' ] = str( ct or '' )

        verb = 'POST'
        uri = self._get_uri(  verb, key )

        out = self._normal_return( func, verb, uri, out = True )

        out = out.strip()
        out = out.replace( '\n', '' ).replace( '\r', '' )

        r = re.compile( '<UploadId>(.{32})</UploadId>' )
        r = r.search( out )

        if r:
            return r.groups()[0]
        else:
            raise S3ResponseError, func.format( error = \
                "key={key} out={info}'".format( key = key, info = out ), )


    def get_list_parts( self, key, uploadid ):

        func = "get_list_parts error='{error}'"

        if self.domain == self.DEFAULT_DOMAIN:
            self.set_domain( self.up_domain )

        self.intra_query[ 'uploadId' ] = str( uploadid )

        verb = 'GET'
        uri = self._get_uri( verb, key )

        out = self._normal_return( func, verb, uri, out = True )

        out = out.strip()

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
        #return ( tr, pr[ : ] )


    def upload_part( self, key, uploadid, partnum, partfile,
                            ct = None, cl = None ):

        func = "upload_part error='{error}'"

        if self.domain == self.DEFAULT_DOMAIN:
            self.set_domain( self.up_domain )

        self.intra_query[ 'uploadId' ] = str( uploadid )
        self.intra_query[ 'partNumber' ] = str( partnum )

        self.intra_header[ 'Content-Type' ] = str( ct or ftype( partfile ) )
        self.intra_header[ 'Content-Length' ] = str( cl or fsize( partfile ) )

        verb = 'PUT'
        uri = self._get_uri( verb, key )

        return self._normal_return( func, verb, uri, infile = partfile )


    def merge_parts( self, key, uploadid, mergefile,
                            ct = None, cl = None ):

        func = "merge_parts error='{error}'"

        if self.domain == self.DEFAULT_DOMAIN:
            self.set_domain( self.up_domain )

        self.intra_query[ 'uploadId' ] = str( uploadid )

        self.intra_header[ 'Content-Type' ] = str( ct or ftype( mergefile ) )
        self.intra_header[ 'Content-Length' ] = str( cl or fsize( mergefile ) )

        verb = 'POST'
        uri = self._get_uri( verb, key )

        return self._normal_return( func, verb, uri, infile = mergefile )



    def upload_file( self, key, fn ):

        func = "upload_file error='{error}'"

        self.intra_header[ 'Content-Type' ] = str( ftype( fn ) )
        self.intra_header[ 'Content-Length' ] = str( fsize( fn ) )

        verb = 'PUT'
        uri = self._get_uri( verb, key )

        return self._normal_return( func, verb, uri, infile = fn )


    def upload_file_relax( self, key, fsha1, flen ):

        func = "upload_file_relax error='{error}'"

        self.intra_query[ None ] = 'relax'

        self.intra_header[ 'Content-Length' ] = str( 0 )
        self.intra_header[ 's-sina-sha1' ] = str( fsha1 )
        self.intra_header[ 's-sina-length' ] = str( flen )

        verb = 'PUT'
        uri = self._get_uri( verb, key )

        return self._normal_return( func, verb, uri )


    def copy_file( self, key, src, project = None ):

        func = "copy_file error='{error}'"

        prj = str( project or self.project )

        self.intra_query[ None ] = 'copy'

        self.intra_header[ 'Content-Length' ] = str( 0 )
        self.intra_header[ 'x-amz-copy-source' ] = "/%s/%s" % ( prj, src, )

        verb = 'PUT'
        uri = self._get_uri( verb, key )

        return self._normal_return( func, verb, uri )


    def copy_file_from_project( self, key, src, project ):

        return self.copy_file( key, src, project )


    def get_file( self, key ):

        func = "get_file error='{error}'"

        verb = 'GET'
        uri = self._get_uri( verb, key )

        return self._normal_return( func, verb, uri, out = True )


    def get_file_url( self, key ):

        func = "get_file_url error='{error}'"

        verb = 'GET'
        uri = self._get_uri( verb, key )

        url = '{domain}:{port}{uri}'.format(
                domain = self.domain,
                port = self.port,
                uri = uri, )

        return url


    def get_file_meta( self, key ):

        func = "get_file_meta error='{error}'"

        self.intra_query[ None ] = 'meta'

        verb = 'GET'
        uri = self._get_uri( verb, key )

        return self._normal_return( func, verb, uri, out = True )


    def get_list( self ):

        func = "get_list error='{error}'"

        self.intra_query_extend[ 'formatter' ] = 'json'

        verb = 'GET'
        uri = self._get_uri( verb )

        return self._normal_return( func, verb, uri, out = True )


    def list_files( self, prefix = None,
                          marker = None,
                          maxkeys = None,
                          delimiter = None ):

        func = "list_files error='{error}'"

        self.intra_query_extend[ 'formatter' ] = 'json'
        self.intra_query_extend[ 'prefix' ] = str( prefix or '' )
        self.intra_query_extend[ 'marker' ] = str( marker or '' )
        self.intra_query_extend[ 'max-keys' ] = str( maxkeys or 10 )
        self.intra_query_extend[ 'delimiter' ] = str( delimiter or '' )

        verb = 'GET'
        uri = self._get_uri( verb )

        return self._normal_return( func, verb, uri, out = True )


    def update_meta( self, key, meta = None ):

        func = "update_meta error='{error}'"

        meta = ( meta or {} ).copy()

        self.intra_query[ None ] = 'meta'

        self.intra_header[ 'Content-Length' ] = str( 0 )

        for k in meta:
            if k.lower() in (   'content-md5',
                                'content-type',
                                'content-length',
                                'content-sha1',
                                ):
                continue

            self.intra_header[ k ] = str( meta[ k ] )

        verb = 'PUT'
        uri = self._get_uri( verb, key )

        return self._normal_return( func, verb, uri )


    def delete_file( self, key ):

        func = "delete_file error='{error}'"

        verb = 'DELETE'
        uri = self._get_uri( verb, key )

        return self._normal_return( func, verb, uri )


    def _normal_return( self, func, verb, uri,
                    infile = None,
                    out = False,
                    httpcode = None ):

        verb = verb.upper()
        code = int( httpcode or self.VERB2HTTPCODE.get( verb, httplib.OK ) )

        try:
            resp = self._requst( verb, uri ) \
                    if infile is None \
                    else self._requst_put( verb, uri, infile )

            if resp.status != code:

                raise S3HTTPCodeError, func.format( \
                        error = self._resp_format( resp ), )

            if out:
                data = ''
                while True:
                    chunk = resp.read( self.CHUNK )

                    if chunk == '':
                        break
                    data += chunk

                return data

            return self._resp_format( resp )

        except Exception, e:

            raise


    def _resp_format( self, resp ):

        r = "code={code} reason={reason} out={out}".format(
                    code = resp.status,
                    reason = resp.reason,
                    out = resp.read().strip().replace( '\n', ' ' ).\
                                        replace( '\r', ' ' ), )

        return r


    def _requst( self, verb, uri ):

        header = {}
        header.update( self.intra_header )
        header.update( self.requst_header )

        for k in header:
            if type( header[ k ] ) == types.UnicodeType:
                header[ k ] = header[ k ].encode( 'utf-8' )

        self._reset_intra()

        try:
            h = self._http_handle()
            h.putrequest( verb, uri )
            for k in header:
                h.putheader( k, header[ k ] )
            h.endheaders()

            resp = h.getresponse()

            return resp

        except httplib.HTTPException, e:

            raise
            #raise S3HTTPError, " {verb} {uri} out={e}".format(
            #                verb = verb,
            #                uri = uri,
            #                e = repr( e ), )


    def _requst_put( self, verb, uri, fn ):

        header = {}
        header.update( self.intra_header )
        header.update( self.requst_header )

        for k in header:
            if type( header[ k ] ) == types.UnicodeType:
                header[ k ] = header[ k ].encode( 'utf-8' )

        self._reset_intra()

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

        except httplib.HTTPException, e:

            raise
            #raise S3HTTPError, " {verb} {uri} fn={fn} out={e}".format(
            #                verb = verb,
            #                uri = uri,
            #                fn = fn,
            #                e = repr( e ), )
        except OSError:
            raise
        except IOError:
            raise

        except:
            raise

        finally:
            try:
                f.close()
            except:
                pass


    def _http_handle( self ):

        try:
            if self.is_ssl:
                h = httplib.HTTPSConnection(    self.domain, self.port,
                                                timeout = self.timeout,
                                                **self.ssl_auth )
            else:
                h = httplib.HTTPConnection(     self.domain, self.port,
                                                timeout = self.timeout )
        except httplib.HTTPException, e:

            raise
            #raise S3HTTPError, "Connect %s:%s %s" % \
            #        ( self.domain, self.port, repr( e ), )

        return h



    def _step_extra( self ):

        extra = self.extra
        extra += self.intra_query.pop( None, '' )

        return extra

    def _step_qs( self ):

        query_string = {}
        query_string.update( self.intra_query )
        query_string.update( self.query_string )

        qs = [ '%s=%s' % ( k, v ) for k, v in query_string.items() ]
        qs.sort()
        qs = '&'.join( qs )

        return qs + '&' if qs != '' else ''

    def _step_rh( self ):

        requst_header = {}
        requst_header.update( self.intra_header )
        requst_header.update( self.requst_header )

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

    def _step_expires( self ):

        et = type( self.expires )
        if et in ( types.IntType, types.LongType, types.FloatType ):
            dt = str( int( self.expires ) )
        elif et in types.StringTypes :
            dt = str( self.expires )
        elif et == types.NoneType :
            dt = datetime.datetime.utcnow()
            dt = dt.strftime( '%a, %d %b %Y %H:%M:%S +0000' )
        elif et == datetime.timedelta :
            dt = datetime.datetime.utcnow() + self.expires
            dt = dt.strftime( '%a, %d %b %Y %H:%M:%S +0000' )
        elif et == datetime.datetime :
            dt = dt.strftime( '%a, %d %b %Y %H:%M:%S +0000' )
        else:
            dt = time.time().__int__() + 30 * 60

        return dt

    def _step_qs_extend( self ):

        qs_extend = {}
        qs_extend.update( self.intra_query_extend )
        qs_extend.update( self.query_extend )

        qs = [ '%s=%s' % ( k, v ) for k, v in qs_extend.items() ]
        qs.sort()
        qs = '&'.join( qs )

        return qs + '&' if qs != '' else ''

    def _fix_requst_header( self, verb = 'GET' ):

        fix_key = [ 's-sina-sha1',
                    'content-sha1',
                    's-sina-md5',
                    'content-md5',
                    'content-type', ]

        for d in [ self.intra_header, self.requst_header ]:
            for k in d.keys():
                kk = k.lower()
                if kk in fix_key or \
                    kk.startswith( 'x-sina-' ) or \
                    kk.startswith( 'x-amz-' ):

                    del d[ k ]

    def _signature( self, strtosign ):

        ssig = hmac.new( self.secretkey, \
                            strtosign, \
                            hashlib.sha1 ).digest().encode( 'base64' )

        return ssig

    def _get_uri( self, verb, key = None ):

        verb = verb.upper()
        key = '/' + ( key or '' )

        if self.vhost:
            uri = key
        else:
            uri = "/" + str( self.project ) + key

        extra = self._step_extra()
        if extra != '?':
            uri += extra + '&'
        else:
            uri += extra

        qs = self._step_qs()
        uri += qs

        if not self.need_auth:

            qs_ex = self._step_qs_extend()
            uri += qs_ex

            return uri.rstrip( '?&' )

        rh = self._step_rh()

        hashinfo = rh.get( 'hash-info', '' )
        ct = rh.get( 'content-type', '' )

        mts = [ k + ':' + v for k, v in rh.items() \
                if k.startswith( 'x-sina-' ) or \
                    k.startswith( 'x-amz-' ) ]
        mts.sort()

        if verb == 'GET':
            hashinfo = ''
            ct = ''
            mts = []

            self._fix_requst_header( verb )

        dt = self._step_expires()

        stringtosign = '\n'.join( [ verb, hashinfo, ct, dt ] + \
                                mts + [ uri.rstrip( '?&' ) ] )
        ssig = self._signature( stringtosign )

        qs_ex = self._step_qs_extend()
        uri += qs_ex

        uri += "&".join( [  "KID=" + self.nation.lower() + "," + self.accesskey,
                            "Expires=" + dt,
                            "ssig=" + urllib.quote_plus( ssig[5:15] ), ] )

        return uri.rstrip( '?&' )



if __name__ == '__main__':

    pass

