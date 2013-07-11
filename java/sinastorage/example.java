package sinastorage;

import java.io.ByteArrayOutputStream;

import java.util.HashMap;
import java.util.Map;

import sinastorage.sinastorageservice;

public class example{

    private String accesskey = "SYS0000000000SANDBOX";
    private String secretkey = "1111111111111111111111111111111111111111";
    private String project = "sandbox";

    private sinastorageservice s3 = new sinastorageservice( accesskey,
            secretkey, project );

    public void testPutFile() {

        try {
            String up = "Somebody looks like your Old Friend.";
            boolean upload = this.s3.putFile( "java_sdk_putfile.txt",
                    up.getBytes() );

            System.out.println( upload );

            String filename = "somewhere";
            boolean upload1 = this.s3
                    .putFile( "java_sdk_putfile.txt", filename );

            System.out.println( upload1 );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testPutFileRelax() {
        try {
            boolean upload = this.s3.putFileRelax( "java_sdk_putfilerelax.jpg",
                    "9a1dda270ba97d5ae16ddf76fcf35cc320f8b0f7", 80725 );
            // boolean upload = this.s3.putFileRelax(
            // "java_sdk_putfilerelax.jpg",
            // "9a1dda270ba97d5ae16ddf76fcf35cc320f8b0f7", 80725, "image/jpeg"
            // );
            System.out.println( upload );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testCopeFile() {
        try {
            boolean upload = this.s3.copyFile( "java_sdk_copyfile.jpg",
                    "java_sdk_putfilerelax.jpg" );
            // boolean upload = this.s3.copyFileFromProject(
            // "java_sdk_copyfile.jpg",
            // "java_sdk_putfilerelax.jpg", "prj" );
            System.out.println( upload );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testGetGile() {

        try {
            ByteArrayOutputStream out = this.s3.getFile( "DONOT_README" );

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

    public static void main( String[] args ) throws Exception {

        example test = new example();

        test.s3.setNeed_auth( true );
        // test.s3.setVhost( true );
        // test.s3.setHttps( "" );

        // test.testPutFile();
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
