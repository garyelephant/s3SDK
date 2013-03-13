#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Date:
@author
"""
__author__ = ' '
__version__ = '1.0.0'

import os
import sys
import time
import json
import hashlib
import pprint


import conf
import sinastorageservice as s3

pdir = os.path.dirname( os.path.abspath( sys.argv[0] ) )
tmpdir = os.path.join( pdir, 'tmp' )


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
        tmpfiledir = os.path.join( tmpdir, filename )

        if not os.path.isdir( tmpfiledir ):
            os.makedirs( tmpfiledir, mode = 0755 )

        partnum = 1
        while True:
            part = f.read( filesplit )

            if part == '':
                break

            part_md5 = hashlib.md5()
            part_md5.update( part )
            _md5 = part_md5.hexdigest()

            with open( os.path.join( tmpfiledir,
                                     '%d.%s' % ( partnum, _md5 ) ), 'wb' ) as _f:
                _f.write( part )

            merge += '<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>' % ( partnum, _md5 )

            sobj.update( part )

            partnum += 1
        else:
            pass

        merge += '</CompleteMultipartUpload>'

        with open( os.path.join( tmpfiledir, '.merge.xml' ), 'w' ) as _f:
            _f.write( merge )

        _md5 = sobj.hexdigest()

        with open( os.path.join( tmpfiledir, '.md5' ), 'w' ) as _f:
            _f.write( _md5 )

        return _md5


if __name__ == '__main__':

    errorfile = open( 'log.log', 'a' )

    project = conf.project
    accesskey = conf.accesskey
    secretkey = conf.secretkey

    ffile = os.path.join( pdir, 'bigfile.jpg' )
    bn = os.path.basename( ffile )

    _md5 = partedfile( ffile, 1024 * 1024 * 1, 'md5' )

    print 'upload : ', ffile

    handle = s3.S3( accesskey, secretkey, project )

    key = 'test_upload_bigfile'
    with open( os.path.join( tmpdir, bn, '.key' ), 'w' ) as _f:
        _f.write( key )

    handle.set_need_auth()
    tf, resp = handle.get_upload_idc()
    handle.purge()
    if not tf:
        print resp.read()
        sys.exit( 1 )
    else:
        domain = resp

    handle.set_domain( domain )

    with open( os.path.join( tmpdir, bn, '.domain' ), 'w' ) as _f:
        _f.write( domain )

    for i in range( 1, 4 ):
        try:
            tf, resp = handle.get_upload_id( key )
            if not tf:
                print resp.read()
            else:
                uploadid = resp
                break
        except Exception as e:
            print e
            continue
    else:
        sys.exit( 1 )

    print 'uploadId : ', uploadid
    with open( os.path.join( tmpdir, bn, '.uploadid' ), 'w' ) as _f:
        _f.write( uploadid )


    round = 1
    while True:
        print 'Round %d :' % ( round, )
        round += 1
        time.sleep( 1 )

        partnums = []
        try:
            tf, resp = handle.list_parts( key, uploadid )
            if not tf:
                print resp.read()
                continue
            else:
                partnums = resp[ : ]
        except:
            continue

        parts = os.listdir( os.path.join( tmpdir, bn ) )
        parts = [ k for k in parts if not k.startswith( '.' ) ]
        parts.sort( key = lambda x : int( x.split( '.' )[0] ), reverse = False )

        parts = [ p for p in parts \
                 if int( p.split( '.' )[0] ) not in  partnums]

        if parts == []:
            break

        for part in parts:
            num, p_md5 = part.split( '.' )

            for i in range( 1, 4 ):
                tf, resp = handle.upload_part( key, uploadid, num, os.path.join( tmpdir, bn, part ) )

                if tf:
                    print 'try %d: upload %s ok' % ( i, part )
                    break
                else:
                    print 'try %d: upload %s error %s' % ( i, part, str( resp.read() ) )
            else:
                pass

    tf, resp = handle.merge_parts( key, uploadid, os.path.join( tmpdir, bn, '.merge.xml' ) )

    if tf:
        print 'merger %s OK' % ( key, )
    else:
        print resp.read()


