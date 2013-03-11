
import os
import sys
import stat
import time
import datetime
import types


# # compatible with python2.4
# import hashlib
try:
    import hashlib
    sha1 = hashlib.sha1
except ImportError:
    import sha
    class Sha1:
        digest_size = 20
        def new( self, inp = '' ):
            return sha.sha( inp )

    sha1 = Sha1()

import hmac
import httplib
import urllib



def fsize( f ):
    st = os.fstat( f.fileno() )
    return st[ stat.ST_SIZE ]


class S3Error( Exception ): pass


class S3( object ):
    
    """ python SDK for Sina Storage Service """
    
    DEFAULT_DOMAIN = 'sinastorage.com'
    DEFAULT_UP_DOMAIN = 'up.sinastorage.com'
    HTTP_OK = 200
    HTTP_DELETE = 204
    
    def __init__( self, accesskey = None, secretkey = None, project = None ):
        
        self.accesskey = 'SYS0000000000SANDBOX' if accesskey is None else accesskey
        
        if len( self.accesskey ) != len( 'SYS0000000000SANDBOX' ) \
                or '0' not in self.accesskey:
            raise S3Error, 'accesskey "%s" is illegal.' % self.accesskey
        
        self.nation = self.accesskey.split( '0' )[0].lower()
        self.accesskey = self.accesskey.split( '0' )[-1].lower()
        
        self.secretkey = '1' * 40 if secretkey is None else secretkey
        self.project = 'sandbox' if project is None else project
        
        self.domain = self.DEFAULT_DOMAIN
        self.up_domain = self.DEFAULT_UP_DOMAIN
        
        self.port = 80
        self.timeout = 60
        
        self.expires = 30 * 60
        
        self.extra = '?'
        self.query = {}
        
        self.is_ssl = False
        self.ssl_auth = {}
        
    
    def set_https( self, **ssl ):
        
        self.is_ssl = True
        self.port = 4443
        self.timeout = 3 * 60
        
        self.ssl_auth['key_file'] = ssl.get( 'key_file', '')
        self.ssl_auth['cert_file'] = ssl.get( 'cert_file', '')
    
    def set_domain( self, domain ):
        self.domain = domain
    
    def set_extra( self, extra ):
        self.extra = extra
    
    def set_expires( self, expires ):
        self.expires = expires
    
    
    # large file upload step:
    # 1. get upload idc : get a domain to hold during uploading a file
    # 2. get upload id  : get a uploadid to bind during uploading parts
    # 3. upload part    : upload a part
    # 4. list parts     : list the parts that are uploaded to server
    # 5. merge part     : merge all parts after uplaod all parts
    
    def get_upload_idc( self ):
        
        self.set_domain( self.up_domain )
        
        try:
            h = self._http_handle()
            h.putrequest( 'GET', '/?extra&op=domain.json' )
            h.endheaders()
            resp = h.getresponse()
            
            return resp.status == self.HTTP_OK, resp
        
        except Exception, e:
            raise S3Error, " Get upload idc error : '%s' " % \
                    ( repr( e ), )
        
        finally:
            pass
    
    def get_upload_id( self, key ):
        
        if self.domain == self.DEFAULT_DOMAIN:
            self.set_domain( self.up_domain )
        
        self.extra = '?uploads'
        
        args = self.uploadquery( 'POST', key )
        uri = args[ 0 ]
        
        try:
            h = self._http_handle()
            h.putrequest( 'POST', uri )
            h.endheaders()
            resp = h.getresponse()
            
            if resp.status != self.HTTP_OK:
                return False, resp
            
            data = resp.read()
            
            import re
            
            r = re.compile( '<UploadId>(.{32})</UploadId>' )
            r = r.search( data )
            
            if r:
                return True, r.groups()[0]
            else:
                raise S3Error, " '%s' get uploadid failed. return '%s'" % \
                        ( key, data, )
            
        except Exception, e:
            raise S3Error, " '%s' get uploadid error : '%s'" % \
                            ( key, repr( e ), )
    
    def upload_part( self, key, uploadid, partnum, partfile ):
        
        if self.domain == self.DEFAULT_DOMAIN:
            self.set_domain( self.up_domain )
        
        flen = os.path.getsize( partfile )
        
        self.extra = '?partNumber=%s&uploadId=%s' % \
                ( str( partnum ), str( uploadid ), )
        
        args = self.uploadquery( 'PUT', key )
        uri = args[ 0 ]
        
        f = open( partfile, 'rb' )
        try:
            h = self._http_handle()
            h.putrequest( 'PUT', uri )
            h.putheader( "Content-Length", str( flen ) )
            h.endheaders()
            
            while True:
                data = f.read( 1024 * 1024 )
                if data == '':
                    break
                h.send( data )
                
            resp = h.getresponse()
            
            return resp.status == self.HTTP_OK, resp
        
        except Exception, e:
            raise S3Error, " '%s' upload part '%s:%s', error : '%s'" % \
                            ( key, uploadid, str( partnum ), repr( e ), )
        
        finally:
            f.close()
    
    def list_parts( self, key, uploadid ):
        
        if self.domain == self.DEFAULT_DOMAIN:
            self.set_domain( self.up_domain )
        
        self.extra = '?uploadId=%s' % ( uploadid, )
        args = self.downloadquery( 'GET', key )
        
        uri = args[ 0 ]
        
        try:
            h = self._http_handle()
            h.putrequest( 'GET', uri )
            h.endheaders()
            
            resp = h.getresponse()
            
            if resp.status != self.HTTP_OK:
                return False, resp
            
            data = resp.read().strip()
            
            import re
            
            tr = re.compile( '<IsTruncated>(True|False)</IsTruncated>' )
            tr = tr.search( data )
            
            if tr and tr.groups()[0] == 'True':
                tr = True
            else:
                tr = False
            
            pr = re.compile( '<PartNumber>([0-9]*)</PartNumber>' )
            pr = pr.findall( data )
            
            if pr:
                pr = [ int( i ) for i in pr ]
                pr.sort()
            else:
                tr = True
                pr = []
            
            return True, pr
            #return True, ( tr, pr[ : ] )
        
        except Exception, e:
            raise S3Error, " '%s' list parts, uploadid '%s', error : '%s'" % \
                            ( key, uploadid, repr( e ), )
    
    def merge_parts( self, key, uploadid, mergefile ):
        
        if self.domain == self.DEFAULT_DOMAIN:
            self.set_domain( self.up_domain )
        
        flen = os.path.getsize( mergefile )
        
        self.extra = '?uploadId=%s' % ( uploadid, )
        args = self.uploadquery( 'POST', key )
        
        uri = args[ 0 ]
        
        f = open( mergefile, 'rb' )
        try:
            h = self._http_handle()
            h.putrequest( 'POST', uri )
            h.putheader( "Content-Length", str( flen ) )
            h.endheaders()
            
            while True:
                data = f.read( 1024 * 1024 )
                if data == '':
                    break
                h.send( data )
                
            resp = h.getresponse()
            
            return resp.status == self.HTTP_OK, resp
        
        except Exception, e:
            raise S3Error, " '%s' merge file, uplodid '%s', error : '%s'" % \
                            ( key, uploadid, repr( e ), )
        
        finally:
            f.close()
    
    
    def upload( self, key, fn ):
        
        resp = self._put( key, fn )
        
        return resp.status == self.HTTP_OK, resp
    
    
    def download( self, key ):
        
        return self.getFile( key )
    
    
    def getFile( self, key ):
        
        args = self.downloadquery( key )
        
        uri = args[ 0 ]
        
        try:
            h = self._http_handle()
            h.putrequest( 'GET', uri )
            h.endheaders()
            
            resp = h.getresponse()
            
            return resp.status == self.HTTP_OK, resp.read()
            
        except Exception, e:
            raise S3Error, "getfile '%s' error : '%s'" % \
                        ( key, repr( e ), )
    
    def getFileUrl( self, key ):
        
        args = self.downloadquery( key )
        
        uri = args[ 0 ]
        
        return True, 'http://%s%s' % \
                    ( self.DEFAULT_DOMAIN, uri )
    
    def _put( self, key, fn ):
        
        flen = os.path.getsize( fn )
        
        args = self.uploadquery( 'PUT', key )
        
        uri = args[ 0 ]
        
        print uri
        
        f = open( fn, 'rb' )
        try:
            h = self._http_handle()
            h.putrequest( 'PUT', uri )
            h.putheader( "Content-Length", str( flen ) )
            h.endheaders()
            
            while True:
                data = f.read( 1024 * 1024 )
                if data == '':
                    break
                h.send( data )
            resp = h.getresponse()
            return resp
        
        finally:
            f.close()
    
    
    def relax_upload( self, rsha1, rlen ):
        
        self.extra = '?relax'
        
        metas = {}
        metas['s-sina-sha1'] = str( rsha1 )
        metas['s-sina-length'] = str( rlen )
    
    
    def _http_handle( self ):
        
        try:
            if self.is_ssl:
                h = httplib.HTTPSConnection(    self.domain, \
                                                self.port, \
                                                timeout = self.timeout, \
                                                **self.ssl_auth )
            else:
                h = httplib.HTTPConnection(     self.domain, \
                                                self.port, \
                                                timeout = self.timeout, )
        except httplib.HTTPException, e:
            
            raise S3Error, "Connect '%s:%s' error : '%s' " % \
                    ( self.domain, self.port, repr( e ), )
    
        return h
    
    
    def uploadquery( self, verb, key,
                     hashinfo = '',
                     expires = None,
                     metas = {},
                     **kwargs ) :
        
        hl = len( hashinfo )
        
        if hl == 40 :
            hk = 's-sina-sha1'   # sha1 hex
        elif hl == 28 :
            hk = 'Content-SHA1'  # sha1 base64
        elif hl == 32 :
            hk = 's-sina-md5'    # md5 hex
        elif hl == 24 :
            hk = 'Content-MD5'   # md5 base64
        else:
            hk = ''
        
        if expires is None:
            expires = str( time.time().__int__() + self.expires )
        
        et = type( expires )
        if et in types.StringTypes :
            dt = expires.encode( 'utf-8' )
        elif et == types.NoneType :
            dt = datetime.datetime.utcnow()
            dt = dt.strftime( '%a, %d %b %Y %H:%M:%S +0000' )
        elif et == datetime.timedelta :
            dt = datetime.datetime.utcnow() + expires
            dt = dt.strftime( '%a, %d %b %Y %H:%M:%S +0000' )
        elif et == datetime.datetime :
            dt = dt.strftime( '%a, %d %b %Y %H:%M:%S +0000' )
        
        metas = metas.copy()
        
        if hk != '':
            metas[hk] = hashinfo
        
        if 'decovered' in kwargs :
            metas['x-sina-decovered'] = bool( kwargs['decovered'] )
        
        if 'autoclean' in kwargs :
            ac = kwargs['autoclean']
            tac = type( ac )
            if tac in ( types.IntType, types.LongType ) :
                metas['s-sina-expires-ctime'] = "%d" % ( ac, )
            elif tac in types.StringTypes :
                metas['s-sina-expires'] = ac.encode( 'utf-8' )
            elif tac == datetime.datetime :
                metas['s-sina-expires'] = ac.strftime( '%a, %d %b %Y %H:%M:%S +0000' )
        
        key = key.encode( 'utf-8' )
        resource = "/" + str( self.project ) + "/" + key
    
        h = kwargs.get( 'vhost', False )
        if h:
            url = '/' + key
        else:
            url = resource
        
        if self.extra != '?':
            resource += self.extra
            url += self.extra + '&'
        else:
            url += self.extra
        
        ct = metas.get( 'Content-Type', '' )
        
        mts = [ ( str( k ).lower(), v.encode( 'utf-8' ) ) for k, v in metas.items() ]
        mts = [ k + ':' + v for k, v in mts if k.startswith( 'x-sina-' ) or k.startswith( 'x-amz-meta-' ) ]
        mts.sort()
        
        stringtosign = '\n'.join( [ verb, hashinfo, ct, dt ] + mts + [ resource ] )
        
        ssig = hmac.new( self.secretkey, stringtosign, sha1 ).digest().encode( 'base64' )
    
        metas['Date'] = dt
        metas['Authorization'] = self.nation.upper() + ' ' + self.accesskey + ':' + \
                                 ssig[5:15]
        
        url += "&".join( [  "KID=" + self.nation.lower() + "," + self.accesskey,
                            "Expires=" + dt,
                            "ssig=" + ssig[5:15], ] )
        
        return url, metas
    
    
    def downloadquery( self, key,
                       expires = None,
                       metas = {},
                       **kwargs ) :

        if expires is None:
            expires = str( time.time().__int__() + self.expires )

        et = type( expires )

        if et in types.StringTypes :
            dt = expires.encode( 'utf-8' )
        elif expires is None :
            dt = datetime.datetime.utcnow()
            dt = dt.strftime( '%a, %d %b %Y %H:%M:%S +0000' )
        elif et == datetime.timedelta :
            dt = datetime.datetime.utcnow() + expires
            dt = dt.strftime( '%a, %d %b %Y %H:%M:%S +0000' )
        elif et == datetime.datetime :
            dt = dt.strftime( '%a, %d %b %Y %H:%M:%S +0000' )

        key = key.encode( 'utf-8' )
        resource = "/" + str( self.project ) + "/" + key

        h = kwargs.get( 'vhost', False )
        # # compatible with python2.4
        # url = '/'+key if h else resource
        if h:
            url = '/' + key
        else:
            url = resource

        if self.extra != '?':
            resource += self.extra
            url += self.extra + '&'
        else:
            url += self.extra
        
        stringtosign = '\n'.join( ["GET", "", "", dt] + [ resource ] )

        ssig = hmac.new( self.secretkey, stringtosign, sha1 ).digest().encode( 'base64' )

        metas['Date'] = dt
        metas['Authorization'] = self.nation.upper() + ' ' + self.accesskey + ':' + ssig[5:15]

        url += "&".join( [  "KID=" + self.nation.lower() + "," + self.accesskey,
                            "Expires=" + dt,
                            "ssig=" + ssig[5:15], ] )
        return url, metas


if __name__ == '__main__':

    print uploadquery( 'sina', 'product',
                       'uV3F3YluFJax1cknvbcGwgjvx4QpvB+leU8dUj2o',
                       'yourproject', 'abc/def.jpg',
                       11, 'XrY7u+Ae7tCTyyK7j1rNww==',
                       expires = 'Tue, 10 Aug 2010 16:08:08 +0000',
                       metas = { 'Content-Type': 'text/plain',
                                 'Content-Encoding': 'utf-8',
                                 'X-Sina-Info': '%E4%B8%AD%E6%96%87',
                               }
                     )


    print downloadquery( 'sina', 'project',
                         'uV3F3YluFJax1cknvbcGwgjvx4QpvB+leU8dUj2o',
                         'www.mydomain.com', 'abc/def.jpg',
                         '1175139620',
                       )

    print deletequery( 'amz', '0PN5J17HBGZHT7JJ3X82',
                       'uV3F3YluFJax1cknvbcGwgjvx4QpvB+leU8dUj2o',
                       'johnsmith', 'photos/puppy.jpg',
                       metas = { 'x-amz-date': 'Tue, 27 Mar 2007 21:20:26 +0000',
                               }
                     )

    print copyquery( 'amz', '15B4D3461F177624206A',
                     'uV3F3YluFJax1cknvbcGwgjvx4QpvB+leU8dUj2o',
                     'atlantic', 'jetsam',
                     'Wed, 20 Feb 2008 22:12:21 +0000',
                     metas = {'x-amz-copy-source' : '/pacific/flotsam',
                             }
                   )


    expires = str( time.time().__int__() + 60 * 5 )

    # expires = 'Tue, 7 Feb 2012 23:45:08 +0800'

    r = downloadquery( "sys", "sandbox", "1" * 40,
                       "sandbox2", "testfile", expires
    )

    h = httplib.HTTPConnection( "218.30.126.203", 80, )
    h.request( 'GET', r[ 0 ], None, r[ 1 ] )
    resp = h.getresponse()

    print resp.status
