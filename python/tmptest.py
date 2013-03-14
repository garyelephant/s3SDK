#!/usr/bin/env python2.6
# coding: utf-8

import json
import random


if __name__ == "__main__":

    import sinastorageservice as sinastorage

    #print sinastorage.ftype( 'bigfile.jpg' )

    handle = sinastorage.S3()
    handle.set_need_auth()
    
    # resp = handle.put( 'aaaaaa', '111' )
    # print resp.status
    
    #handle.set_https(  key_file = '/data0/S3monitor/pyclient/ssl/client.key',
    #                  cert_file = '/data0/S3monitor/pyclient/ssl/client.crt' )
    
    #idc = handle.get_upload_idc()
    #print idc
    handle.set_domain( 'yf.sinastorage.com' )

    #key = 'this_is_just_for_test_%s' % random.randint( 1, 10 )
    key = 'python_sdk_upload'

    #handle.set_query_string( uploadid = '1' * 40, foo = 'bar' )
    #handle.extra = '?copy'

    #handle.set_requst_header( **{ 's-sina-sha1' : '3'*40 } )

    #handle.set_vhost()
    #print handle.domain

    #print handle.upload_file( key, 'bigfile.jpg' )
    #print handle.upload_file_relax( 'relax_1', '9dfc376a81919c0a6cd71915c97f06600f9f2737', 8063397 )
    #print handle.get_file_url( key )
    print handle.update_meta( key, { 'Content-Disposition' : 'painting.jpg' }  )
    print handle.get_file_meta( key )
    #print handle.delete_file( key )

    #print handle.get_list()
    #print handle.list_files()
    #print handle._signature( "PUT", key )
    
    #uploadid = handle.get_upload_id( key )
    #print 'uploaid :', uploadid

    #_istruncated, _parts = handle.list_parts( key, uploadid )
    #print _istruncated, _parts
    
    
#
#    resp = sinastorage.put( "218.30.126.203", 80,
#                            "sys", "sandbox", "1" * 40,
#                            "sandbox2", "testfile", "sinastorage.py"
#    )
#
#    print resp.status == 200
#
#    resp = sinastorage.get( "218.30.126.203", 80,
#                            "sys", "sandbox", "1" * 40,
#                            "sandbox2", "testfile"
#    )
#
#    print resp.status == 200
    # print resp.read()
