#!/usr/bin/env python2.6
# coding: utf-8
"""
Author : Slasher
"""

__version__ = '1.0.0'

import os
import sys
import time
import httplib
import unittest

import sinastorageservice as s3

PROGDIR = os.path.dirname( os.path.abspath( sys.argv[0] ) )

_DEBUG_ = True

def message( *msgs ):

    if not _DEBUG_:
        return

    print '\n'.join( msgs )


class TestS3UserInterface( unittest.TestCase ):

    case_attr = ( ( 'domain', 'sinastorage.com' ),
                  ( 'domain', '113.108.216.237' ),
                  ( 'port', 80 ),
                  ( 'timeout', 30 ),
                  ( 'timeout', 60 ),
                  ( 'expires', None ),
                  ( 'expires', time.time().__int__() + 30 * 60 ),
                  ( 'need_auth', False ),
                  ( 'need_auth', True ),
                  ( 'vhost', False ),
                  )

    case_attr_plus = ( ( 'vhost', True ),
                       )

    case_query = ( ( None, None ),
                   ( 'ip', str( time.time().__int__() + 60 * 60 ) + ',1.1.1.1' ),
                   ( 'foo', 'bar' ),
                   )

    case_query_list = case_query + \
                        ( ( 'formatter', 'json' ),
                          )

    case_query_get = case_query + \
                        ( ( 'fn', 'rename.txt' ),
                          ( 'rd', '404.html' ),
                          )

    case_header = ( ( None, None ),
                    )

    case_header_put = case_header + \
                       ( ( 'Content-Type', 'text/plain' ),
                         ( 'Content-Disposition', 'attachment; filename="unittest.txt"' ),
                         )

    up_file = os.path.join( PROGDIR, 'test_sinastorageservice.py' )
    up_data = 'This is a string for upload.'


    def _s3_handle( self ):

        h = s3.S3( 'SYS0000000000SANDBOX', '1' * 40, 'sandbox' )

        return h

    def _assert_run_func( self, func, *args, **kwargs ):

        try:
            resp = func( *args, **kwargs )
            message( repr( resp ), '\n' )
        except s3.S3HTTPCodeError, e:
            raise self.failureException, 'Error Response: ' + repr( e )
        except:
            raise

    def _upload_func_case( self, funcname, *args, **kwargs ):

        message( 'test %s:' % ( funcname, ) )

        for attr_k, attr_v in self.case_attr:
            for query_k, query_v in self.case_query:
                for header_k, header_v in self.case_header_put:

                    h = self._s3_handle()
                    setattr( h, attr_k, attr_v )

                    query = { query_k : query_v } if query_k is not None \
                                else None
                    headers = { header_k : header_v } if header_k is not None \
                                else None

                    kwargs[ 'query' ] = query
                    kwargs[ 'headers' ] = headers

                    message( 'attribute : ( %s : %s )' % ( attr_k, attr_v ),
                             'query     : ' + repr( query ),
                             'header    : ' + repr( headers ) )

                    func = getattr( h, funcname )

                    self._assert_run_func( func, *args, **kwargs )

    def _download_func_case( self, funcname, *args, **kwargs ):

        message( 'test %s:' % ( funcname, ) )

        for attr_k, attr_v in self.case_attr:
            for query_k, query_v in self.case_query_get:
                for header_k, header_v in self.case_header:

                    h = self._s3_handle()
                    setattr( h, attr_k, attr_v )

                    query = { query_k : query_v } if query_k is not None \
                                else None
                    headers = { header_k : header_v } if header_k is not None \
                                else None

                    kwargs[ 'query' ] = query
                    kwargs[ 'headers' ] = headers

                    message( 'attribute : ( %s : %s )' % ( attr_k, attr_v ),
                             'query     : ' + repr( query ),
                             'header    : ' + repr( headers ) )

                    func = getattr( h, funcname )

                    self._assert_run_func( func, *args, **kwargs )

    def _list_func_case( self, funcname, *args, **kwargs ):

        message( 'test %s:' % ( funcname, ) )

        for attr_k, attr_v in self.case_attr:
            for query_k, query_v in self.case_query_list:
                for header_k, header_v in self.case_header:

                    h = self._s3_handle()
                    setattr( h, attr_k, attr_v )

                    query = { query_k : query_v } if query_k is not None \
                                else None
                    headers = { header_k : header_v } if header_k is not None \
                                else None

                    kwargs[ 'query' ] = query
                    kwargs[ 'headers' ] = headers

                    message( 'attribute : ( %s : %s )' % ( attr_k, attr_v ),
                             'query     : ' + repr( query ),
                             'header    : ' + repr( headers ) )

                    func = getattr( h, funcname )

                    self._assert_run_func( func, *args, **kwargs )

    def test_post_file( self ):

        message( 'test post_file:' )
        h = self._s3_handle()
        self._assert_run_func( h.post_file, \
                               'test_post_file', self.up_file )

    def test_upload_file( self ):

        self._upload_func_case( 'upload_file', \
                                'test_upload_file', self.up_file )

    def test_upload_data( self ):

        self._upload_func_case( 'upload_data', \
                                'test_upload_data', self.up_file )

    def test_upload_relax( self ):

        fsha1 = '56c28d38e3d4f7fb3cb4f28cafd7f6583ead414e'
        flength = 39
        self._upload_func_case( 'upload_relax', \
                               'test_upload_relax', fsha1, flength )

    def test_copy_file( self ):

        self._upload_func_case( 'copy_file', \
                                'test_copy_file', 'test_upload_relax' )

    def test_get_file( self ):

        self._download_func_case( 'get_file', \
                                  'test_copy_file' )

    def test_get_file_url( self ):

        self._download_func_case( 'get_file_url', \
                                  'test_copy_file' )

    def test_get_file_meta( self ):

        self._list_func_case( 'get_file_meta', \
                              'test_copy_file' )


    def test_get_files_list( self ):

        self._list_func_case( 'get_files_list', \
                              marker = 'test_', maxkeys = 10 )

    def test_update_file_meta( self ):

        meta = { 'Content-Disposition' : 'attachment; filename="ramanujan.txt"' }

        self._upload_func_case( 'update_file_meta', \
                                'test_copy_file', meta = meta )

    def test_delete_file( self ):

        message( 'test delete_file:' )
        h = self._s3_handle()
        self._assert_run_func( h.delete_file, \
                               'test_null_file' )


class TestS3FileType( unittest.TestCase ):

    def test_ftype( self ):

        case = ( ( '', 'application/octet-stream' ),
                 ( 'a', 'application/octet-stream' ),
                 ( 'a.txt', 'text/plain' ),
                 ( 'a.py', 'text/x-python' ),
                 ( 'a.jpg', 'image/jpeg' ),
                 ( 'a.gif', 'image/gif' ),
                 ( 'a.mp4', 'video/mp4' ),
                 ( 'a.mp3', 'audio/mpeg' ),
                 ( 'a.doc', 'application/msword' ),
                 ( 'a.ppt', 'application/vnd.ms-powerpoint' ),
                 ( 'a.pdf', 'application/pdf' ),
                  )

        for f, t in case:
            message( 'test the content-type of %s is %s' % ( f, t ) )
            self.assertEqual( s3.ftype( f ) , t, 'Expected %s:%s, %s' % \
                              ( f, s3.ftype( f ), t ) )


if __name__ == '__main__':

    unittest.main()
