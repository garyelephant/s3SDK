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
import datetime
import math

import random
import json
import types
import re
import glob
import pprint

import hmac
import base64
import hashlib
import httplib
import urllib
import urllib2

import sinastorageservice as s3

pdir = os.path.dirname( os.path.abspath( sys.argv[0] ) )
tmpdir = os.path.join( 'e:\\', 'tmp' )


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
        
        with open( os.path.join( tmpfiledir, '.merge' ), 'w' ) as _f:
            _f.write( merge )
        
        _md5 = sobj.hexdigest()
        
        with open( os.path.join( tmpfiledir, '.env' ), 'w' ) as _f:
            _f.write( _md5 )
        
        return _md5


if __name__ == '__main__':
    
    errorfile = open( 'log.log', 'a' )
    
    files = filter( lambda x: x.split( '.' )[-1] in ( 'mkv', 'rmvb', 'mp4', 'avi' ), \
                            glob.glob( '%s\*' % ( os.path.join( 'C:\\', 'bdacc' ), ) ) )
    
    project = 'shenjieproj'
    accesskey = 'SINA000000000SHENJIE'
    secretkey = 'tOSsCG25nJDIiAg+iW5dIBW0lhKcja5iAQ7x2kYa'
    
    for ffile in files:
        
        bn = os.path.basename( ffile )
        
        
        _md5 = partedfile( ffile, 1024 * 1024 * 4, 'md5' )
        
        if not os.path.isdir( os.path.join( tmpdir, bn ) ):
            continue
        
        print bn
        
        f = open( os.path.join( tmpdir, bn, '.env' ), 'r' )
        _md5 = f.read().strip()
        
        handle = s3.S3( accesskey, secretkey, project )
        
        key = 'film/' + _md5
        with open( os.path.join( tmpdir, bn, '.key' ), 'w' ) as _f:
            _f.write( key )
        
        _idc = handle.get_upload_idc()
        handle.set_domain( _idc )
        with open( os.path.join( tmpdir, bn, '.idc' ), 'w' ) as _f:
            _f.write( _idc )
        
        
        for i in range( 0, 3 ):
            try:
                uploadid = handle.get_upload_id( key )
                # f = open( os.path.join( tmpdir, bn, '.uploadid' ), 'r' )
                # uploadid = f.read().strip()
                
                break
            except Exception as e:
                print e
                continue
        else:
            continue
        
        print uploadid
        with open( os.path.join( tmpdir, bn, '.uploadid' ), 'w' ) as _f:
            _f.write( uploadid )
        
        truncated, partnums = handle.list_parts( key, uploadid )
        
        if not truncated:
            continue
        continue
        parts = os.listdir( os.path.join( tmpdir, bn ) )
        parts = [ k for k in parts if not k.startswith( '.' ) ]
        parts.sort( key = lambda x : int( x.split( '.' )[0] ), reverse = False )
        
        parts = [ p for p in parts \
                 if int( p.split( '.' )[0] ) not in  partnums]
        
        errorfile.write( 'start upload %s -> %s\n' % ( key, ffile ) )
        
        print parts
        
        for part in parts:
            
            num, p_md5 = part.split( '.' )
            
            for i in range( 1, 4 ):
                resp = handle.upload_part( key, uploadid, num, os.path.join( tmpdir, bn, part ) )
                
                if resp.status == 200:
                    print 'try : %d upload %s ok' % ( i, part )
                    break
                else:
                    print 'try : %d upload %s error %s' % ( i, part, str( resp.status ) )
            else:
                errorfile.write( 'upload %s part %s,%s failed\n' % ( key, num, part ) )
        
        resp = handle.merge_parts( key, uploadid, os.path.join( tmpdir, bn, '.merge' ) )
        
        if resp.status == 200:
            errorfile.write( 'end upload %s -> %s\n\n' % ( key, ffile ) )
        else:
            print resp.read()
            errorfile.write( 'merge error %s -> %s\n\n' % ( key, ffile ) )
        
        
