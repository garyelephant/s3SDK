package sinastorage;

import java.util.Arrays;

import java.security.KeyManagementException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import sinastorage.SinaStorageService;

public class MultipartUpload{

    private final static String accesskey = "SYS0000000000SANDBOX";
    private final static String secretkey = "1111111111111111111111111111111111111111";
    private final static String project = "sandbox";

    private final SinaStorageService s3 = new SinaStorageService( accesskey,
            secretkey, project );

    public MultipartUpload() {
    }

    public String md5( String str ) {

        String md5Str = null;

        try {
            MessageDigest md = MessageDigest.getInstance( "MD5" );
            md.update( str.getBytes() );
            byte b[] = md.digest();

            int i;
            StringBuffer buf = new StringBuffer( "" );

            for (int offset = 0; offset < b.length; offset++) {
                i = b[offset];
                if (i < 0)
                    i += 256;
                if (i < 16)
                    buf.append( "0" );
                buf.append( Integer.toHexString( i ) );
            }

            md5Str = buf.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return md5Str;
    }

    public String generateMergeXml( String[] parts ) {

        String mergeXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
        mergeXml += "<CompleteMultipartUpload>";

        for (int i = 0; i < parts.length; i++) {
            mergeXml += "<Part><PartNumber>" + (i + 1);
            mergeXml += "</PartNumber><ETag>";
            mergeXml += parts[i] + "</ETag></Part>";
        }

        mergeXml += "</CompleteMultipartUpload>";

        return mergeXml;
    }

    public void testMultipart() {

        String[] partStr = { "Somebody ", "looks ", "like ", "your ", "Old ",
                "Friend", ".", " -- !@#$%^&*" };

        String[] partMD5 = new String[partStr.length];

        for (int i = 0; i < partStr.length; i++) {
            partMD5[i] = this.md5( partStr[i] );
        }

        String mergeXml = this.generateMergeXml( partMD5 );

        String key = "java_sdk_multipart.txt";
        String contentType = "text/plain";

        try {
            int[] unUpload = new int[partStr.length];
            for (int i = 0; i < partStr.length; i++) {
                unUpload[i] = i + 1;
            }

            String uploadid = this.s3.multipartGetUploadId( key, contentType );

            System.out.println( "uploadid : " + uploadid );

            boolean complete = false;
            while (!complete) {
                complete = true;

                int[] parts = this.s3.multipartGetPartsList( key, uploadid );

                for (int i = 0; i < unUpload.length; i++) {
                    if (Arrays.binarySearch( parts, unUpload[i] ) >= 0) {
                        unUpload[i] = -1;
                    }

                    if (unUpload[i] > 0) {
                        try {
                            if (this.s3.multipartPutPart( key, uploadid,
                                    unUpload[i],
                                    partStr[unUpload[i] - 1].getBytes() )) {
                                System.out.println( "upload part "
                                        + unUpload[i] + " : OK" );
                            } else {
                                complete = false;
                            }
                        } catch (Exception e1) {
                            complete = false;
                            e1.printStackTrace();
                        }
                    }
                }
            }

            for (int i = 0; i < 5; i++) {
                if (this.s3.multipartMergeParts( key, uploadid,
                        mergeXml.getBytes() )) {
                    System.out.println( "merger " + key + " : OK" );
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void setInstance() {

        /*
         * notice : You can not setVhost( true ). You only can setHost(
         * "up.sinastorage.com" ), if you must set host.
         */
        this.s3.reset();
        this.s3.setNeed_auth( true );
        try {
            this.s3.setHttps();
        } catch (KeyManagementException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    public static void main( String[] args ) throws Exception {

        MultipartUpload multipart = new MultipartUpload();

        multipart.setInstance();
        multipart.testMultipart();
    }
}
