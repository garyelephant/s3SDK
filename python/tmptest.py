#!/usr/bin/env python2.6
# coding: utf-8


if __name__ == "__main__":

    import sinastorageservice as sinastorage

    handle = sinastorage.S3()
    
    # resp = handle.put( 'aaaaaa', '111' )
    # print resp.status
    
    #handle.set_https(  key_file = '/data0/S3monitor/pyclient/ssl/client.key',
    #                  cert_file = '/data0/S3monitor/pyclient/ssl/client.crt' )
    
    idc = handle.get_upload_idc()
    print idc
    handle.set_domain( idc )

    key = 'this_is_just_for_test'
    
    uploadid = handle.get_upload_id( key )
    print 'uploaid :', uploadid

    _istruncated, _parts = handle.list_parts( key, uploadid )
    print _istruncated, _parts
    
    
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
