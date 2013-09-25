#!/usr/bin/env python2.6
# coding: utf-8
"""
Author : Slasher
"""

__version__ = '1.0.0'

import os
import sys
import time
import traceback
import logging
import pprint

import conf
import pool
import sinastorageservice as s3


pdir = os.path.dirname( os.path.abspath( sys.argv[0] ) )
tempdir = os.path.join( pdir, 'tempfile' )

logfile = os.path.join( pdir, 'access.log' )
logging.basicConfig( filename = logfile, \
                     level = logging.DEBUG, \
                     format = '[%(asctime)s %(filename)s:%(lineno)d %(levelname)s] %(message)s' )

def _set_example( h ):

    query = { 'ip' : str( time.time().__int__() + 24 * 3600 ) + ',7.7.7.7',
              'foo' : 'bar',
              'formatter' : 'json',
              'fn' : 'rename.txt',
              'rd' : '404.html', }

    headers = { 'Content-Length' : '2013',
                'Content-Type' : 'text/plain',
                'Content-Disposition' : 'attachment; filename="ramanujan.txt"', }

    h.need_auth = True

    h.domain = 'sinastorage.com'
    h.port = 80
    h.timeout = 60
    h.expires = int( time.time() + 20 * 60 )

    h.vhost = True

    h.set_https( ssl = True,
                 port = 443,
                 timeout = 180,
                 key_file = 'somewhere',
                 cert_file = 'somewhere', )

def trace_func( func, *args, **kwargs ):
    try:
        r = func( *args, **kwargs )
        logging.info( 'run %s, return : ' % func.__name__ + repr( r ) )
        return r
    except Exception, e:
        logging.warning( 'run %s error : ' % func.__name__ + repr( e ) )
        logging.error( traceback.format_exc() )
        raise

def _handle():

    h = s3.S3( conf.accesskey, conf.secretkey, conf.project )

    return h

def test_post_file():

    h = _handle()
    h.need_auth = True

    key = 'python/POST_FILE'
    fn = os.path.join( tempdir, 'DONOT_README' )

    trace_func( h.post_file, key, fn )

def test_upload_file():

    h = _handle()
    h.need_auth = True

    key = 'python/UPLOAD_FILE'
    fn = os.path.join( tempdir, 'DONOT_README' )

    trace_func( h.upload_file, key, fn )

def test_upload_data():

    h = _handle()
    h.need_auth = True

    key = 'python/UPLOAD_DATA'
    data = 'upload data by python sdk'

    headers = { 'Content-Type' : 'text/plain' }

    trace_func( h.upload_data, key, data, headers = headers )

def test_upload_relax():

    h = _handle()
    h.need_auth = True

    key = 'python/UPLOAD_RELAX'
    fsha1 = '9dfc376a81919c0a6cd71915c97f06600f9f2737'
    flen = 8063397

    trace_func( h.upload_relax, key, fsha1, flen )

def test_copy_file():

    h = _handle()
    h.need_auth = True

    key = 'python/COPY_FILE'
    src = 'python/UPLOAD_RELAX'
    project = 'sandbox'

    trace_func( h.copy_file, key, src, project )

def test_get_file():

    h = _handle()
    h.need_auth = True

    key = 'python/UPLOAD_DATA'

    print trace_func( h.get_file, key )

def test_get_file_url():

    h = _handle()
    h.need_auth = True

    key = 'python/UPLOAD_DATA'

    print trace_func( h.get_file_url, key )

def test_get_file_meta():

    h = _handle()
    h.need_auth = True

    key = 'python/UPLOAD_DATA'

    print trace_func( h.get_file_meta, key )

def test_get_project_list():

    h = _handle()
    h.need_auth = True

    trace_func( h.get_project_list )

def test_get_files_list():

    h = _handle()
    h.need_auth = True

    prefix = ''
    marker = 'python'
    maxkeys = 5
    delimiter = ''

    print trace_func( h.get_files_list, prefix, marker, maxkeys, delimiter )

def test_update_file_meta():

    h = _handle()
    h.need_auth = True

    key = 'python/UPLOAD_DATA'
    meta = { 'Content-Disposition' : 'attachment; filename="ramanujan.txt"' }

    trace_func( h.update_file_meta, key, meta )

def test_delete_file():

    h = _handle()
    h.need_auth = True

    key = 'pyhon/DELETE_FILE'

    trace_func( h.delete_file, key )


def test_upload_dirall( dir ):

    def listdir( dir ):
        r = []
        ff = os.listdir( dir )

        ff = [ os.path.join( dir, f ) for f in ff ]

        for f in ff:
            if os.path.isfile( f ):
                r += [ f ]
            elif os.path.isdir( f ):
                r += listdir( f )[ : ]
            else:
                pass

        return r


    def _upload( key, fn ):

        h = s3.S3( conf.accesskey, conf.secretkey, conf.project )
        h.need_auth = True
        h.upload_file( key, fn )

    import pool

    threadpool = pool.WorkerPool( 10 )
    upload = threadpool.runwithpool( _upload )

    files = listdir( dir )
    keys = [ key[ len( dir ) + 1: ] for key in files ]

    for key, fn in zip( keys, files ):
        print key, fn
        upload( key, fn )

    threadpool.join()

def test_uplaod_bigfile():

    import large_file_upload

    key = 'python/big_file_upload'

    large_file_upload.upload_large( key )


if __name__ == "__main__":

    test_post_file()

    test_upload_file()
    test_upload_data()
    test_upload_relax()
    test_copy_file()

    test_get_file()
    test_get_file_url()
    test_get_file_meta()

    test_get_project_list()
    test_get_files_list()

    test_update_file_meta()
    test_get_file_meta()

    test_delete_file()

    test_upload_dirall( os.path.join( tempdir, 'updir' ) )

    test_uplaod_bigfile()

    sys.exit( 0 )

