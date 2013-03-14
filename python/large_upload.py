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
    print 'upload : ', ffile

    bn = os.path.basename( ffile )
    _md5 = partedfile( ffile, 1024 * 1024 * 1, 'md5' )

    handle = s3.S3( accesskey, secretkey, project )

    key = 'test_upload_bigfile'
    with open( os.path.join( tmpdir, bn, '.key' ), 'w' ) as _f:
        _f.write( key )

    handle.set_need_auth()
    tf, out = handle.get_upload_idc()

    if not tf:
        print out
        sys.exit( 1 )
    else:
        domain = out

    print domain
    with open( os.path.join( tmpdir, bn, '.domain' ), 'w' ) as _f:
        _f.write( domain )

    handle.set_domain( domain )

    for i in range( 1, 4 ):
        tf, out = handle.get_upload_id( key )
        if not tf:
            print out
        else:
            uploadid = out
            break
    else:
        sys.exit( 1 )

    with open( os.path.join( tmpdir, bn, '.uploadid' ), 'w' ) as _f:
        _f.write( uploadid )
    print 'uploadId=' + uploadid

    round = 1
    while True:
        print 'Round %d: ' % ( round, )
        round += 1
        time.sleep( 1 )

        partnums = []
        tf, out = handle.list_parts( key, uploadid )
        if not tf:
            print out
            continue
        else:
            partnums = out

        print partnums
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
                tf, out = handle.upload_part( key, uploadid, num, os.path.join( tmpdir, bn, part ) )

                if tf:
                    print 'try %d: upload %s ok' % ( i, part )
                    break
                else:
                    print 'try %d: upload %s error %s' % ( i, part, str( out ) )
            else:
                pass

    tf, out = handle.merge_parts( key, uploadid, os.path.join( tmpdir, bn, '.merge.xml' ) )

    if tf:
        print 'merger %s OK' % ( key, )
    else:
        print out


