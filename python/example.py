#!/usr/bin/env python2.6
# coding: utf-8
"""
Date:
@author
"""
__author__ = 'Slasher'
__version__ = '0.1.0'

import os
import sys
import logging

import conf
import pool
import sinastorageservice as s3


pdir = os.path.dirname( os.path.abspath( sys.argv[0] ) )
tempdir = os.path.join( pdir, 'tempfile' )

logfile = os.path.join( pdir, 'access.log' )
logging.basicConfig( filename = logfile, \
                     level = logging.DEBUG, \
                     format = '[%(asctime)s %(filename)s:%(lineno)d %(levelname)s] %(message)s' )

def _set( h ):

    h.set_need_auth( True )

    return

    h.set_domain( 'sinastorage.com' )
    h.set_port( 80 )
    h.set_timeout( 60 )

    h.set_expires( str( int( time.time() + 20 * 60 ) ) )
    h.set_expires_delta( 20 * 60 )

    h.set_extra( '?' )

    h.set_vhost( False )

    h.set_query_string( {   'ip' : '1363392928,7.7.7.7',
                            'foo' : 'bar',
                            } )

    h.set_requst_header( {  'Content-Length' : '2013',
                            'Content-Type' : 'text/plain',
                            'Content-Disposition' : 'attachment; filename="ramanujan.txt"',
                            } )

    h.set_query_extend( {   'formatter' : 'json',
                            'fn' : 'rename.txt',
                            'rd' : '404.html',
                            } )

    h.set_https(  ssl = True,
                  port = 4443,
                  timeout = 180,
                  key_file = 'somewhere',
                  cert_file = 'somewhere', )

    h.reset()


def test_uplaod_file( h ):

    key = 'DONOT_README'
    fn = os.path.join( tempdir, 'DONOT_README' )

    try:
        out = h.upload_file( key, fn )
        print out
        logging.info( "uplaod_file key='{key}' ok out='{out}'".format( key = key, out = out ) )
    except Exception, e:
        print e
        logging.error( "uplaod_file key='{key}' error out='{out}'".format( key = key, out = repr( e ) ) )


def test_upload_file_relax( h ):

    key = 'relax_upload'
    fsha1 = '9dfc376a81919c0a6cd71915c97f06600f9f2737'
    flen = 8063397

    print h.upload_file_relax( key, fsha1, flen )


def test_copy_file( h ):

    key = 'copy_upload'
    src = 'relax_upload'
    project = 'sandbox'

    print h.copy_file( key, src, project )


def test_get_file( h ):

    key = 'DONOT_README'

    print h.get_file( key )


def test_get_file_url( h ):

    key = 'DONOT_README'

    print h.get_file_url( key )


def test_get_file_meta( h ):

    key = 'relax_upload'

    print h.get_file_meta( key )


def test_get_list( h ):

    print h.get_list()


def test_list_files( h ):

    prefix = 'rela'
    marker = 'relax'
    maxkeys = 5
    delimiter = ''

    print h.list_files( prefix, marker, maxkeys, delimiter )


def test_update_meta( h ):

    key = 'relax_upload'
    meta = { 'Content-Disposition' : 'attachment; filename="painting.jpg"' }

    print h.update_meta( key, meta.copy() )


def test_delete_file( h ):

    key = 'DONOT_README_1'

    print h.delete_file( key )



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

        handle = s3.S3( conf.accesskey, conf.secretkey, conf.project )
        handle.set_need_auth()

        try:
            out = handle.upload_file( key, fn )
            logging.info( "uplaod_file key='{key}' ok out='{out}'".format( key = key, out = out ) )
        except Exception, e:
            logging.error( "uplaod_file key='{key}' error out='{out}'".format( key = key, out = repr( e ) ) )


    import pool

    threadpool = pool.WorkerPool( 10 )
    upload = threadpool.runwithpool( _upload )


    files = listdir( dir )
    keys = [ key[ len( dir ) + 1: ] for key in files ]

    for key, fn in zip( keys, files ):
        upload( key, fn )

    threadpool.join()



def test_uplaod_bigfile():

    import large_upload

    key = 'big_file_upload'

    large_upload.upload_large( key )



if __name__ == "__main__":

    #test_upload_dirall( os.path.join( tempdir, 'updir' ) )
    #test_uplaod_bigfile()

    handle = s3.S3( conf.accesskey, conf.secretkey, conf.project )
    _set( handle )

    test_uplaod_file( handle )
    #test_upload_file_relax( handle )
    #test_copy_file( handle )

    #test_get_file( handle )
    #test_get_file_url( handle )
    #test_get_file_meta( handle )

    #test_get_list( handle )
    #test_list_files( handle )

    #test_update_meta( handle )

    #test_delete_file( handle )

    sys.exit( 0 )

