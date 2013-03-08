
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
    
    def __init__( self, accesskey = None, secretkey = None, project = None ):
        
        self.accesskey = 'SYS0000000000SANDBOX' if accesskey is None else accesskey
        
        if '0' not in self.accesskey:
            raise S3Error, 'accesskey "%s" is illegal.' % self.accesskey
        
        self.nation = self.accesskey.split( '0' )[0].lower()
        self.accesskey = self.accesskey.split( '0' )[-1].lower()
        
        self.secretkey = '1' * 40 if secretkey is None else secretkey
        self.project = 'sandbox' if project is None else project
        
        self.domain = 'sinastorage.com'
        self.port = 80
        self.timeout = 3 * 60
        
        self.expires = 30 * 60
        
        self.extra = '?'
        self.query = {}
    
    
    def set_https( self ):
        pass
    
    def set_domain( self, domain ):
        self.domain = domain
    
    def set_extra( self, extra ):
        self.extra = extra
    
    def upload( self ):
        pass
    
    def download( self ):
        pass
    
    
    def get_upload_idc( self ):
        
        try:
            h = httplib.HTTPConnection( 'up.sinastorage.com', self.port )
            h.putrequest( 'GET', '/?extra&op=domain.json' )
            h.endheaders()
            resp = h.getresponse()
            return resp.read().strip().strip( '"' )
        finally:
            pass
    
    
    def get_upload_id( self, key ):
        
        if self.domain == 'sinastorage.com':
            self.set_domain( 'up.sinastorage.com' )
        
        self.extra = '?uploads'
        args = self.uploadquery( 'POST', key )
        
        uri = args[ 0 ]
        
        try:
            h = httplib.HTTPConnection( self.domain, self.port )
            h.putrequest( 'POST', uri )
            h.endheaders()
            resp = h.getresponse()
            
            data = resp.read()
            
            import re
            
            r = re.compile( '<UploadId>(.{32})</UploadId>' )
            r = r.search( data )
            if r:
                return r.groups()[0]
            else:
                raise S3Error, "get uploadid failed. return '%s'" % ( data )
        finally:
            pass
    
    
    def upload_part( self, key, uploadid, partnum, partfile ):
        
        if self.domain == 'sinastorage.com':
            self.set_domain( 'up.sinastorage.com' )
        
        flen = os.path.getsize( partfile )
        
        self.extra = '?partNumber=%s&uploadId=%s' % ( str( partnum ), uploadid, )
        args = self.uploadquery( 'PUT', key )
        
        uri = args[ 0 ]
        
        f = open( partfile, 'rb' )
        try:
            h = httplib.HTTPConnection( self.domain, self.port )
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
    
    def list_parts( self, key, uploadid, ):
        
        if self.domain == 'sinastorage.com':
            self.set_domain( 'up.sinastorage.com' )
    
        self.extra = '?uploadId=%s' % ( uploadid, )
        args = self.uploadquery( 'GET', key )
        
        uri = args[ 0 ]
        
        try:
            h = httplib.HTTPConnection( self.domain, self.port )
            h.putrequest( 'GET', uri )
            h.endheaders()
            
            resp = h.getresponse()
            
            if resp.status != 200:
                raise S3Error, "list part failed. return code '%s'" % ( resp.status, )
            
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
            
            return tr, pr 
        
        finally:
            pass
    
    def merge_parts( self, key, uploadid, mergefile ):
        
        if self.domain == 'sinastorage.com':
            self.set_domain( 'up.sinastorage.com' )
        
        flen = os.path.getsize( mergefile )
        
        self.extra = '?uploadId=%s' % ( uploadid, )
        args = self.uploadquery( 'POST', key )
        
        uri = args[ 0 ]
        
        f = open( mergefile, 'rb' )
        try:
            h = httplib.HTTPConnection( self.domain, self.port )
            h.putrequest( 'POST', uri )
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
    
    def put( self, key, fn ):
        
        flen = os.path.getsize( fn )
        
        args = self.uploadquery( 'PUT', key )
        
        uri = args[ 0 ]
        
        print uri
        
        f = open( fn, 'rb' )
        try:
            h = httplib.HTTPConnection( self.domain, self.port )
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
    
    
    def relax_upload( self ):
        
        self.extra = '?relax'
        
        metas = {}
        metas['s-sina-length'] = str( Length )
    
    
    def uploadquery( self, verb, key,
                     hashinfo = '',
                     expires = None,
                     metas = {},
                     **kwargs ) :
        
        hl = len( hashinfo )
        
        if hl == 40 :  # sha1 hex
            hk = 's-sina-sha1'
        elif hl == 28 :
            hk = 'Content-SHA1'  # sha1 base64
        elif hl == 32 :
            hk = 's-sina-md5'  # md5 hex
        elif hl == 24 :
            hk = 'Content-MD5'  # md5 base64
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
        
        stringtosign = '\n'.join( [ verb, hashinfo, ct, dt ] + mts + [resource] )
        
        ssig = hmac.new( self.secretkey, stringtosign, sha1 ).digest().encode( 'base64' )
    
        metas['Date'] = dt
        metas['Authorization'] = self.nation.upper() + ' ' + self.accesskey + ':' + \
                                 ssig[5:15]
        
        url += "&".join( [  "KID=" + self.nation.lower() + "," + self.accesskey,
                            "Expires=" + dt,
                            "ssig=" + ssig[5:15], ] )
        
        return url, metas
    
    
def put( ip, port, nation, accesskey, secretkey, project, key, fn ):
    
    flen = os.path.getsize( fn )
    expires = str( time.time().__int__() + 60 * 5 )

    args = uploadquery( nation, accesskey, secretkey, project, key, flen, expires = expires )

    uri = args[ 0 ]
    auth = args[ 1 ][ 'Authorization' ].split( " " )[ 1 ].split( ":" )[ 1 ]
    print uri
    uri = uri + "?" + "&".join( [ "KID=" + nation.lower() + "," + accesskey,
                                  "Expires=" + expires,
                                  "ssig=" + urllib.quote_plus( auth ), ] )
    print uri
    # # compatible with python2.4
    # with open( fn, 'r' ) as f:
    f = open( fn, 'r' )
    try:
        h = httplib.HTTPConnection( ip, port )
        h.putrequest( 'PUT', uri )
        h.putheader( "Content-Length", str( fsize( f ) ) )
        h.endheaders()
        while True:
            data = f.read( 1024 * 1024 * 10 )
            if data == '':
                break
            h.send( data )
        # h.request( 'PUT', uri, f.fileno() )
        resp = h.getresponse()
        return resp
    finally:
        f.close()


def get( ip, port, nation, accesskey, secretkey, project, key ):
    expires = str( time.time().__int__() + 60 * 5 )

    args = downloadquery( nation, accesskey, secretkey, project, key, expires = expires )

    uri = args[ 0 ]
    auth = args[ 1 ][ 'Authorization' ].split( " " )[ 1 ].split( ":" )[ 1 ]
    
    uri = uri + "?" + "&".join( [ "KID=" + nation.lower() + "," + accesskey,
                                  "Expires=" + expires,
                                  "ssig=" + urllib.quote_plus( auth ), ] )

    h = httplib.HTTPConnection( ip, port )
    h.request( 'GET', uri )
    resp = h.getresponse()

    return resp

def uploadquery( nation, accesskey, secretkey,
                 project, key,
                 Length, hashinfo = '',
                 expires = None,
                 metas = {},
                 relax = False,
                 **kwargs ) :


    hl = len( hashinfo )

    if hl == 40 :  # sha1 hex
        hk = 's-sina-sha1'
    elif hl == 28 :
        hk = 'Content-SHA1'  # sha1 base64
    elif hl == 32 :
        hk = 's-sina-md5'  # md5 hex
    elif hl == 24 :
        hk = 'Content-MD5'  # md5 base64
    else:
        hk = ''

    et = type( expires )

    if et in types.StringTypes :
        dt = expires.encode( 'utf-8' )
    elif et == types.NoneType :
        dt = datetime.datetime.utcnow()  # +datetime.timedelta( seconds=900 )
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
    resource = "/" + str( project ) + "/" + key

    h = kwargs.get( 'vhost', False )
    # # compatible with python2.4
    # url = '/'+key if h else resource
    if h:
        url = '/' + key
    else:
        url = resource

    qs = []

    if relax :
        resource += '?relax'
        qs += ['relax']
        metas['s-sina-length'] = str( Length )
    else :
        metas['Content-Length'] = str( Length )

    ct = metas.get( 'Content-Type', '' )

    mts = [ ( str( k ).lower(), v.encode( 'utf-8' ) ) for k, v in metas.items() ]
    mts = [ k + ':' + v for k, v in mts if k.startswith( 'x-sina-' ) or k.startswith( 'x-amz-meta-' ) ]
    mts.sort()

    stringtosign = '\n'.join( ["PUT", hashinfo, ct, dt] + mts + [resource] )
    print stringtosign
    ssig = hmac.new( secretkey, stringtosign, sha1 ).digest().encode( 'base64' )

    metas['Date'] = dt
    metas['Authorization'] = nation.upper() + ' ' + accesskey + ':' + \
                             ssig[5:15]

    url += '&'.join( qs )
    
    return url, metas


def downloadquery( nation, accesskey, secretkey,
                   project, key,
                   expires = None,
                   metas = {},
                   **kwargs ) :

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
    resource = "/" + str( project ) + "/" + key

    h = kwargs.get( 'vhost', False )
    # # compatible with python2.4
    # url = '/'+key if h else resource
    if h:
        url = '/' + key
    else:
        url = resource


    stringtosign = '\n'.join( ["GET", "", "", dt] + [resource] )

    ssig = hmac.new( secretkey, stringtosign, sha1 ).digest().encode( 'base64' )

    metas['Date'] = dt
    metas['Authorization'] = nation.upper() + ' ' + accesskey + ':' + \
                             ssig[5:15]

    return url, metas


def deletequery( nation, accesskey, secretkey,
                 project, key,
                 metas = {},
                 **kwargs ) :

    key = key.encode( 'utf-8' )
    resource = "/" + str( project ) + "/" + key

    h = kwargs.get( 'vhost', False )
    # # compatible with python2.4
    # url = '/' + key if h else resource
    if h:
        url = '/' + key
    else:
        url = resource

    for k, v in metas.items():
        if "date" in k:
            et = type( metas[k] )
            if et in types.StringTypes :
                metas[k] = metas[k].encode( 'utf-8' )
            elif et == NoneType :
                metas[k] = datetime.datetime.utcnow()
                metas[k] = metas[k].strftime( '%a, %d %b %Y %H:%M:%S +0000' )
            elif et == datetime.timedelta :
                metas[k] = datetime.datetime.utcnow() + expires
                metas[k] = metas[k].strftime( '%a, %d %b %Y %H:%M:%S +0000' )
            elif et == datetime.datetime :
                metas[k] = metas[k].strftime( '%a, %d %b %Y %H:%M:%S +0000' )

    mts = [ ( str( k ).lower(), v.encode( 'utf-8' ) ) for k, v in metas.items() ]
    mts = [ k + ':' + v for k, v in mts if "date" in k ]
    mts.sort()


    stringtosign = '\n'.join( ["DELETE", "", "", "" ] + mts + [resource] )
    stringtosign.encode( 'utf-8' )

    ssig = hmac.new( secretkey, stringtosign, sha1 ).digest().encode( 'base64' )

    metas['Authorization'] = nation.upper() + ' ' + accesskey + ':' + \
                             ssig[5:15]

    return url, metas


def copyquery( nation, accesskey, secretkey,
               destinationbucket, destinationObject,
               Date,
               metas = {},
               **kwargs ) :

    destinationObject = destinationObject.encode( 'utf-8' )
    resource = "/" + str( destinationbucket ) + "/" + destinationObject

    h = kwargs.get( 'vhost', False )
    # # compatible with python2.4
    # url = '/' + destinationObject if h else resource
    if h:
        url = '/' + destinationObject
    else:
        url = resource


    et = type( Date )

    if et in types.StringTypes :
        dt = Date.encode( 'utf-8' )
    elif et == types.NoneType :
        dt = datetime.datetime.utcnow()
        dt = dt.strftime( '%a, %d %b %Y %H:%M:%S +0000' )
    elif et == datetime.timedelta :
        dt = datetime.datetime.utcnow() + expires
        dt = dt.strftime( '%a, %d %b %Y %H:%M:%S +0000' )
    elif et == datetime.datetime :
        dt = dt.strftime( '%a, %d %b %Y %H:%M:%S +0000' )


    mts = [ ( str( k ).lower(), v.encode( 'utf-8' ) ) for k, v in metas.items() ]
    mts = [ k + ':' + v for k, v in mts if 'source' in k ]
    mts.sort()

    stringtosign = '\n'.join( ["PUT", "", "", dt ] + mts + [resource] )

    ssig = hmac.new( secretkey, stringtosign, sha1 ).digest().encode( 'base64' )

    metas['Authorization'] = nation.upper() + ' ' + accesskey + ':' + \
                             ssig[5:15]

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
