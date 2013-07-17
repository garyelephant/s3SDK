package sinastorage;

import java.io.ByteArrayOutputStream;

import java.util.HashMap;
import java.util.Map;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import sinastorage.SinaStorageService;

public class Example{

    private final static String accesskey = "SYS0000000000SANDBOX";
    private final static String secretkey = "1111111111111111111111111111111111111111";
    private final static String project = "sandbox";

    private final SinaStorageService s3 = new SinaStorageService( accesskey,
            secretkey, project );

    public void testPostFile() {

        try {
            String up = "Somebody looks like your Old Friend.";
            boolean upload = this.s3.postFile( "java_sdk_postfile.txt",
                    up.getBytes() );

            System.out.println( upload );

            String filename = "somewhere";
            boolean upload1 = this.s3.postFile( "java_sdk_postfile1.txt",
                    filename );

            System.out.println( upload1 );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testPutFile() {

        try {
            String up = "Somebody looks like your Old Friend.";
            boolean upload = this.s3.putFile( "java_sdk_putfile.txt",
                    up.getBytes() );

            System.out.println( upload );

            String filename = "somewhere";
            boolean upload1 = this.s3.putFile( "java_sdk_putfile1.txt",
                    filename );

            System.out.println( upload1 );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testPutFileRelax() {

        try {
            boolean upload = this.s3.putFileRelax( "java_sdk_putfilerelax.jpg",
                    "9a1dda270ba97d5ae16ddf76fcf35cc320f8b0f7", 80725 );
            System.out.println( upload );
            boolean upload1 = this.s3.putFileRelax(
                    "java_sdk_putfilerelax1.jpg",
                    "9a1dda270ba97d5ae16ddf76fcf35cc320f8b0f7", 80725,
                    "image/jpeg" );
            System.out.println( upload1 );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testCopeFile() {

        try {
            boolean upload = this.s3.copyFile( "java_sdk_copyfile.jpg",
                    "java_sdk_putfilerelax.jpg" );
            System.out.println( upload );
            boolean upload1 = this.s3.copyFileFromProject(
                    "java_sdk_copyfile1.jpg", "java_sdk_putfilerelax.jpg",
                    "sandbox" );
            System.out.println( upload1 );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testGetGile() {

        try {
            ByteArrayOutputStream out = this.s3
                    .getFile( "java_sdk_putfile.txt" );

            System.out.println( out.toString() );

        } catch (Exception e) {
            System.out.println( e.getMessage() );
            e.printStackTrace();
        }
    }

    public void testGetFileUrl() {

        String url = this.s3.getFileUrl( "java_sdk_putfile.txt" );
        System.out.println( url );
    }

    public void testGetFileMeta() {

        String meta;
        try {
            meta = this.s3.getFileMeta( "java_sdk_putfile.txt" );
            System.out.println( meta );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testGetProjectList() {

        String list;
        try {
            list = this.s3.getProjectList();
            System.out.println( list );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testGetFilesList() {

        String list;
        try {
            list = this.s3.getFilesList( "", "java", 10, "" );
            System.out.println( list );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testUpdateFileMeta() {

        try {
            Map<String, String> meta = new HashMap<String, String>();
            meta.put( "Content-Disposition",
                    "attachment; filename=\"attachment.txt\"" );
            boolean upload = this.s3.updateFileMeta( "java_sdk_putfile.txt",
                    meta );
            System.out.println( upload );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testDeleteFile() {

        try {
            boolean upload = this.s3.deleteFile( "java_sdk_putfile.txt" );
            System.out.println( upload );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void setInstance() {

        /*
         * When you upload a file, you must set setNeed_auth( true ). When you
         * download a file or do other action,you can set setNeed_auth( false )
         * if your project is public. You can setHttps with certificate. You can
         * setVhost( true ), if your project is CNAME to SinaStorage or
         * SinaEdge. You can set Host, Port, Timeout, Expires(default is 20 * 60
         * seconds). You can set QueryString, RequstHeader.
         */
        this.s3.reset();
        this.s3.setNeed_auth( true );
        /*
        try {
            this.s3.setHttps();
        } catch (KeyManagementException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        this.s3.setVhost( true );
        this.s3.setHost( "sinastorage.com" );
        this.s3.setUphost( "up.sinastorage.com" );
        this.s3.setPort( 80 );
        this.s3.setTimeout( 30 );
        this.s3.setExpires( 20 * 60 );

        Map<String, String> queryString = new HashMap<String, String>();
        queryString.put( "ip", (24 * 3600 + System.currentTimeMillis() / 1000)
                + ",1.1.1.1" );
        queryString.put( "fn", "fn.txt" );
        queryString.put( "rd", "1.txt" );
        queryString.put( "foo", "bar" );
        this.s3.setQuery_string( queryString );

        Map<String, String> requstHeader = new HashMap<String, String>();
        requstHeader.put( "Content-Disposition",
                "attachment; filename=\"ramanujan.txt\"" );
        this.s3.setRequst_header( requstHeader );
        */
    }

    public static void main( String[] args ) throws Exception {

        Example test = new Example();
        test.setInstance();

        // test.testPostFile();
        test.testPutFile();
        // test.testPutFileRelax();
        // test.testCopeFile();

        // test.testGetGile();
        // test.testGetFileUrl();
        // test.testGetFileMeta();

        // test.testGetProjectList();
        // test.testGetFilesList();

        // test.testUpdateFileMeta();
        // test.testDeleteFile();
    }
}
