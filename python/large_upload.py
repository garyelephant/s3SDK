#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Date:
@author
"""
__author__ = 'Slasher'
__version__ = '0.1.0'

import os
import sys
import time
import hashlib
import pprint


import conf
import pool
import sinastorageservice as s3

pdir = os.path.dirname( os.path.abspath( sys.argv[0] ) )
tempdir = os.path.join( pdir, 'tempfile' )
partdir = os.path.join( tempdir, 'tmp' )


def partedfile( filepath, filesplit = 1024 * 1024 * 4, cipher = 'sha1' ):

    with open( filepath, 'rb' ) as f:
        if cipher == 'sha1':
            sobj = hashlib.sha1()
        elif cipher == 'md5':
            sobj = hashlib.md5()
        else:
            return

        merge = '<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUpload>'

        filename = os.path.basename( filepath )
        tmpfiledir = os.path.join( partdir, filename )

        if not os.path.isdir( tmpfiledir ):
            os.makedirs( tmpfiledir, mode = 0755 )

        partnum = 1
        while True:
            part = f.read( filesplit )

            if part == '':
                break

            part_md5 = hashlib.md5()
            part_md5.update( part )
            hashinfo = part_md5.hexdigest()

            with open( os.path.join( tmpfiledir,
                                     '%d.%s' % ( partnum, hashinfo ) ), 'wb' ) as _f:
                _f.write( part )

            merge += '<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>' % ( partnum, hashinfo )

            sobj.update( part )

            partnum += 1
        else:
            pass

        merge += '</CompleteMultipartUpload>'

        with open( os.path.join( tmpfiledir, '.merge.xml' ), 'w' ) as _f:
            _f.write( merge )

        hashinfo = sobj.hexdigest()

        with open( os.path.join( tmpfiledir, '.hashinfo' ), 'w' ) as _f:
            _f.write( hashinfo )

        return hashinfo


def upload_large( key, fn = None ):

    f = fn or os.path.join( tempdir, 'bigfile.jpg' )
    print 'upload : ', f


    handle = s3.S3( conf.accesskey, conf.secretkey, conf.project )
    handle.set_need_auth()

    bn = os.path.basename( f )
    hashinfo = partedfile( f, 1024 * 1024 * 1, 'sha1' )

    with open( os.path.join( partdir, bn, '.key' ), 'w' ) as _f:
        _f.write( key )

    ## large file uploading interface v1
    #domain = handle.get_upload_idc()
    #print 'domain : ',domain

    #with open( os.path.join( partdir, bn, '.domain' ), 'w' ) as _f:
    #    _f.write( domain )

    #handle.set_domain( domain )

    for i in range( 1, 4 ):
        try:
            uploadid = handle.get_upload_id( key, 'image/jpeg' )
            break
        except Exception, e:
            print 'get uploadid : ', e
    else:
        sys.exit( 1 )

    with open( os.path.join( partdir, bn, '.uploadid' ), 'w' ) as _f:
        _f.write( uploadid )
    print 'uploadId : ' + uploadid


    round = 1
    while True:
        print 'Round %d: ' % ( round, )
        round += 1

        partnums = handle.get_list_parts( key, uploadid )

        parts = os.listdir( os.path.join( partdir, bn ) )
        parts = [ k for k in parts if not k.startswith( '.' ) ]
        parts.sort( key = lambda x : int( x.split( '.' )[0] ), reverse = False )

        parts = [ p for p in parts \
                 if int( p.split( '.' )[0] ) not in partnums]

        #print parts
        if parts == []:
            print 'all parts are uploaded'
            break


        def _upload_part(  key, uploadid, num, fn ):

            handle = s3.S3( conf.accesskey, conf.secretkey, conf.project )
            handle.set_need_auth()

            ## large file uploading interface v1
            #handle.set_domain( domain )

            for i in range( 1, 4 ):
                try:
                    out = handle.upload_part( key, uploadid, num, fn )
                    print 'try %d: upload part %d ok' % ( i, num )
                    return
                except Exception, e:
                    print 'try %d: upload part %d : ' % ( i, num )
                    continue
            else:
                print 'upload part %s failed' % ( num, )

        import pool

        threadpool = pool.WorkerPool( 10 )
        upload_part = threadpool.runwithpool( _upload_part )


        for part in parts:
            num, p_md5 = part.split( '.' )
            num = int( num )

            upload_part( key, uploadid, num, os.path.join( partdir, bn, part ) )

        threadpool.join()

        time.sleep( 3 )


    time.sleep( 5 )
    for i in range( 1, 4 ):
        try:
            out = handle.merge_parts( key, uploadid, os.path.join( partdir, bn, '.merge.xml' ) )

            print 'merger %s ok.' % ( key, )

            break
        except Exception, e:
            time.sleep( 3 )
            print 'try %d: merge : %s' % ( i, repr( e ) )
    else:
        print 'merger %s failed.' % ( key, )


if __name__ == '__main__':

    upload_large( 'big_file_upload' )


