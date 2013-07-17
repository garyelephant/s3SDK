package sinastorage;

import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import java.net.FileNameMap;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.net.HttpURLConnection;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import sun.misc.BASE64Encoder;

import sinastorage.S3Exception;

public class SinaStorageService{

    private final static int CHUNK = 64 * 1024;
    private final static String HOST = "sinastorage.com";
    private final static String UPHOST = "up.sinastorage.com";

    protected String accesskey = "SYS0000000000SANDBOX";
    protected String secretkey = "1111111111111111111111111111111111111111";
    protected String project = "sandbox";

    private String nation;
    private String accessuser;

    private String protocol = "http";
    private String host = SinaStorageService.HOST;
    private String up_host = SinaStorageService.UPHOST;
    private int port = 80;

    private int timeout = 60;
    private int expires = 60 * 20;

    private boolean need_auth = false;
    private boolean vhost = false;

    private boolean is_ssl = false;

    private String extra = "?";

    private Map<String, String> query_string = new HashMap<String, String>();
    private Map<String, String> requst_header = new HashMap<String, String>();
    private Map<String, String> query_specific = new HashMap<String, String>();

    private Map<String, String> intra_query = new HashMap<String, String>();
    private Map<String, String> intra_header = new HashMap<String, String>();
    private Map<String, String> intra_query_specific = new HashMap<String, String>();

    protected Map<String, Integer> verb_to_code = new HashMap<String, Integer>();

    public SinaStorageService() {

        this.splitAccesskey( this.accesskey );
        this.reset();
    }

    public SinaStorageService(String accesskey, String secretkey, String project) {

        this.accesskey = accesskey;
        this.secretkey = secretkey;
        this.project = project;

        this.splitAccesskey( this.accesskey );
        this.reset();
    }

    private void splitAccesskey( String accesskey ) {

        String acckey = accesskey.toLowerCase();

        String[] arr = acckey.split( "0" );

        this.nation = arr[0];
        if (this.nation.equals( "sae" ) || this.nation.length() == 0) {

            this.nation = "sae";
            int len = acckey.length();
            this.accessuser = acckey.substring( len - 11, len - 1 );

        } else {
            this.accessuser = arr[arr.length - 1];
        }
    }

    public void reset() {

        this.setProtocol( "http" );
        this.setHost( SinaStorageService.HOST );
        this.setUphost( SinaStorageService.UPHOST );
        this.setPort( 80 );

        this.setTimeout( 60 );
        this.setExpires( 60 * 20 );

        this.setNeed_auth( false );
        this.setVhost( false );

        this.is_ssl = false;

        this.setExtra( "?" );

        this.query_string = new HashMap<String, String>();
        this.requst_header = new HashMap<String, String>();
        this.query_specific = new HashMap<String, String>();

        this.reset_intra();

        this.verb_to_code.put( "DELETE", HttpURLConnection.HTTP_NO_CONTENT );
    }

    private void reset_intra() {

        this.intra_query = new HashMap<String, String>();
        this.intra_header = new HashMap<String, String>();
        this.intra_query_specific = new HashMap<String, String>();
    }

    protected long getFileSize( String fn ) throws Exception {

        File f = null;
        long size = -1;

        try {
            f = new File( fn );
            size = f.length();
        } catch (Exception e) {
            throw e;
        } finally {
            f = null;
        }
        return size < 0 ? null : size;
    }

    protected String getFileType( String fn ) {

        FileNameMap fNameMap = URLConnection.getFileNameMap();

        String type = fNameMap.getContentTypeFor( fn );

        return type == null ? "application/octet-stream" : type;
    }

    /**
     * post file with a filename
     * 
     * @param key
     * @param fn
     * 
     * @return boolean
     */
    public boolean postFile( String key, String fn ) throws Exception {

        long len = this.getFileSize( fn );

        String uri = "/";

        Map<String, String> headers = new HashMap<String, String>();

        if (this.vhost) {
            headers.put( "Host", this.project );
        } else {
            headers.put( "Host", this.project + "." + SinaStorageService.HOST );
        }

        String[] sign = this.getSignaturePolicy();

        ArrayList<String[]> postFields = new ArrayList<String[]>();
        postFields.add( new String[] { "key", key } );
        postFields.add( new String[] { "AWSAccessKeyId", this.accesskey } );
        postFields.add( new String[] { "Policy", sign[0] } );
        postFields.add( new String[] { "Signature", sign[1] } );
        // postFields.add( new String[]{"success_action_status", "201"} );

        byte[] content = new byte[(int) len];
        InputStream in = null;
        try {
            in = new FileInputStream( fn );
            int size = in.read( content );
            if (size <= 0) {
                throw new S3Exception( " Read " + fn + " fail." );
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e1) {
                    ;
                }
            }
        }

        return this.mulitpartPost( uri, postFields, headers, fn, content );
    }

    /**
     * post file with a byte array
     * 
     * @param key
     * @param content
     * 
     * @return boolean
     */
    public boolean postFile( String key, byte[] content ) throws Exception {

        String uri = "/";

        Map<String, String> headers = new HashMap<String, String>();

        if (this.vhost) {
            headers.put( "Host", this.project );
        } else {
            headers.put( "Host", this.project + "." + SinaStorageService.HOST );
        }

        String[] sign = this.getSignaturePolicy();

        ArrayList<String[]> fields = new ArrayList<String[]>();
        fields.add( new String[] { "key", key } );
        fields.add( new String[] { "AWSAccessKeyId", this.accesskey } );
        fields.add( new String[] { "Policy", sign[0] } );
        fields.add( new String[] { "Signature", sign[1] } );
        // fields.add( new String[]{"success_action_status", "201"} );

        return this.mulitpartPost( uri, fields, headers, "unknown", content );
    }

    /**
     * put file with a filename
     * 
     * @param key
     * @param fn
     * 
     * @return boolean
     */
    public boolean putFile( String key, String fn ) throws Exception {

        this.intra_header.put( "Content-Type", this.getFileType( fn ) );
        this.intra_header.put( "Content-Length", "" + this.getFileSize( fn ) );

        String verb = "PUT";
        String uri = this.getUri( verb, key );

        return this.requstInput( verb, uri, fn );
    }

    /**
     * put file with a byte array
     * 
     * @param key
     * @param content
     * 
     * @return boolean
     */
    public boolean putFile( String key, byte[] content ) throws Exception {

        String verb = "PUT";
        String uri = this.getUri( verb, key );

        return this.requstInput( verb, uri, content );
    }

    /**
     * put file with a sha1
     * 
     * @param key
     * @param fsha1
     * @param flen
     * 
     * @return boolean
     */
    public boolean putFileRelax( String key, String fsha1, int flen )
            throws Exception {

        this.intra_query.put( null, "relax" );

        this.intra_header.put( "Content-Length", "0" );
        this.intra_header.put( "s-sina-sha1", fsha1 );
        this.intra_header.put( "s-sina-length", "" + flen );

        String verb = "PUT";
        String uri = this.getUri( verb, key );

        return this.requst( verb, uri );
    }

    /**
     * put file with a sha1, specify Content-Type
     * 
     * @param key
     * @param fsha1
     * @param flen
     * @param ftype
     * 
     * @return boolean
     */
    public boolean putFileRelax( String key, String fsha1, int flen,
            String ftype ) throws Exception {

        this.intra_header.put( "Content-Type", ftype );

        return this.putFileRelax( key, fsha1, flen );
    }

    /**
     * copy file with a resource from your own project
     * 
     * @param key
     * @param src
     * 
     * @return boolean
     */
    public boolean copyFile( String key, String src ) throws Exception {

        this.intra_query.put( null, "copy" );

        this.intra_header.put( "Content-Length", "0" );
        this.intra_header.put( "x-amz-copy-source", "/" + this.project + "/"
                + src );

        String verb = "PUT";
        String uri = this.getUri( verb, key );

        return this.requst( verb, uri );
    }

    /**
     * copy file with a resource from a project which you can read
     * 
     * @param key
     * @param src
     * @param prj
     * 
     * @return boolean
     */
    public boolean copyFileFromProject( String key, String src, String prj )
            throws Exception {

        this.intra_query.put( null, "copy" );

        this.intra_header.put( "Content-Length", "0" );
        this.intra_header.put( "x-amz-copy-source", "/" + prj + "/" + src );

        String verb = "PUT";
        String uri = this.getUri( verb, key );

        return this.requst( verb, uri );
    }

    /**
     * get file
     * 
     * @param key
     * 
     * @return ByteArrayOutputStream
     */
    public ByteArrayOutputStream getFile( String key ) throws Exception {

        String verb = "GET";
        String uri = this.getUri( verb, key );

        return this.requstOutput( verb, uri );
    }

    /**
     * get file url
     * 
     * @param key
     * 
     * @return String
     */
    public String getFileUrl( String key ) {

        String verb = "GET";
        String uri = this.getUri( verb, key );

        return this.protocol + "://" + this.host + ":" + this.port + uri;
    }

    /**
     * get file meta
     * 
     * @param key
     * 
     * @return String
     */
    public String getFileMeta( String key ) throws Exception {

        this.intra_query.put( null, "meta" );

        String verb = "GET";
        String uri = this.getUri( verb, key );

        ByteArrayOutputStream out = this.requstOutput( verb, uri );

        return out.toString();
    }

    /**
     * get project file list
     * 
     * @return String
     */
    public String getProjectList() throws Exception {

        this.intra_query_specific.put( "formatter", "json" );

        String verb = "GET";
        String uri = this.getUri( verb, "" );

        ByteArrayOutputStream out = this.requstOutput( verb, uri );

        return out.toString();
    }

    /**
     * get files list
     * 
     * @param prefix
     * @param marker
     * @param maxkeys
     * @param delimiter
     * 
     * @return String
     */
    public String getFilesList( String prefix, String marker, int maxkeys,
            String delimiter ) throws Exception {

        this.intra_query_specific.put( "formatter", "json" );

        this.intra_query_specific.put( "prefix", prefix );
        this.intra_query_specific.put( "marker", marker );
        this.intra_query_specific.put( "max-keys", "" + maxkeys );
        this.intra_query_specific.put( "delimiter", delimiter );

        String verb = "GET";
        String uri = this.getUri( verb, "" );

        ByteArrayOutputStream out = this.requstOutput( verb, uri );

        return out.toString();
    }

    /**
     * update file meta
     * 
     * @param key
     * @param metaMap
     * 
     * @return boolean
     */
    public boolean updateFileMeta( String key, Map<String, String> metaMap )
            throws Exception {

        this.intra_query.put( null, "meta" );

        this.intra_header.put( "Content-Length", "0" );

        ArrayList<String> nullUpdate = new ArrayList<String>();
        nullUpdate.add( "content-sha1" );
        nullUpdate.add( "content-md5" );
        nullUpdate.add( "content-type" );
        nullUpdate.add( "content-length" );

        for (String k : metaMap.keySet()) {
            if (nullUpdate.contains( k.toLowerCase() )) {
                continue;
            }
            this.intra_header.put( k, metaMap.get( k ) );
        }

        String verb = "PUT";
        String uri = this.getUri( verb, key );

        return this.requst( verb, uri );
    }

    /**
     * delete file
     * 
     * @param key
     * 
     * @return boolean
     */
    public boolean deleteFile( String key ) throws Exception {

        String verb = "DELETE";
        String uri = this.getUri( verb, key );

        return this.requst( verb, uri );
    }

    /*
     * large file upload by part
     */
    /**
     * multipart get upload idc
     * 
     * @return String
     */
    public String multipartGetUploadIdc() throws Exception {

        this.setHost( SinaStorageService.UPHOST );

        String verb = "GET";
        String uri = "/?extra&op=domain.json";

        ByteArrayOutputStream out = this.requstOutput( verb, uri );

        return this.strip( out.toString().trim(), '"' );
    }

    /**
     * multipart get upload id
     * 
     * @param key
     * 
     * @return String
     */
    public String multipartGetUploadId( String key ) throws Exception {

        this.setHost( SinaStorageService.UPHOST );

        this.intra_query.put( null, "uploads" );

        String verb = "POST";
        String uri = this.getUri( verb, key );

        ByteArrayOutputStream out = this.requstOutput( verb, uri );

        String xml = out.toString();

        Pattern pattern = Pattern.compile( "<UploadId>(.{32})</UploadId>" );
        Matcher matcher = pattern.matcher( xml );

        String uploadId = null;
        if (matcher.find()) {
            uploadId = matcher.group( 1 );
        } else {
            throw new S3Exception( "Not Found uploadid, Response : '" + xml
                    + "'" );
        }

        return uploadId;
    }

    /**
     * multipart get upload id
     * 
     * @param key
     * @param ftype
     * 
     * @return String
     */
    public String multipartGetUploadId( String key, String ftype )
            throws Exception {

        this.intra_header.put( "Content-Type", ftype );

        return this.multipartGetUploadId( key );
    }

    /**
     * multipart get parts list
     * 
     * @param key
     * @param uploadid
     * 
     * @return int[]
     */
    public int[] multipartGetPartsList( String key, String uploadid )
            throws Exception {

        this.setHost( SinaStorageService.UPHOST );

        this.intra_query.put( "uploadId", uploadid );

        String verb = "GET";
        String uri = this.getUri( verb, key );

        ByteArrayOutputStream out = this.requstOutput( verb, uri );

        String xml = out.toString();

        Pattern pattern = Pattern.compile( "<PartNumber>([0-9]*)</PartNumber>" );
        Matcher matcher = pattern.matcher( xml );

        ArrayList<String> parts = new ArrayList<String>();
        while (matcher.find()) {
            parts.add( matcher.group( 1 ) );
        }

        int[] partsList = new int[parts.size()];

        for (int i = 0; i < parts.size(); i++) {
            partsList[i] = Integer.parseInt( parts.get( i ) );
        }

        Arrays.sort( partsList );

        return partsList;
    }

    /**
     * multipart put part
     * 
     * @param key
     * @param uploadid
     * @param partnum
     * @param fn
     * 
     * @return boolean
     */
    public boolean multipartPutPart( String key, String uploadid, int partnum,
            String fn ) throws Exception {

        this.setHost( SinaStorageService.UPHOST );

        this.intra_query.put( "uploadId", uploadid );
        this.intra_query.put( "partNumber", "" + partnum );

        String verb = "PUT";
        String uri = this.getUri( verb, key );

        return this.requstInput( verb, uri, fn );
    }

    /**
     * multipart put part
     * 
     * @param key
     * @param uploadid
     * @param partnum
     * @param content
     * 
     * @return boolean
     */
    public boolean multipartPutPart( String key, String uploadid, int partnum,
            byte[] content ) throws Exception {

        this.setHost( SinaStorageService.UPHOST );

        this.intra_query.put( "uploadId", uploadid );
        this.intra_query.put( "partNumber", "" + partnum );

        String verb = "PUT";
        String uri = this.getUri( verb, key );

        return this.requstInput( verb, uri, content );
    }

    /**
     * multipart merge parts
     * 
     * @param key
     * @param uploadid
     * @param fn
     * 
     * @return boolean
     */
    public boolean multipartMergeParts( String key, String uploadid, String fn )
            throws Exception {

        this.setHost( SinaStorageService.UPHOST );

        this.intra_query.put( "uploadId", uploadid );

        String verb = "POST";
        String uri = this.getUri( verb, key );

        return this.requstInput( verb, uri, fn );
    }

    /**
     * multipart merge parts
     * 
     * @param key
     * @param uploadid
     * @param content
     * 
     * @return boolean
     */
    public boolean multipartMergeParts( String key, String uploadid,
            byte[] content ) throws Exception {

        this.setHost( SinaStorageService.UPHOST );

        this.intra_query.put( "uploadId", uploadid );

        this.intra_header.put( "Content-Type", "text/xml" );
        this.intra_header.put( "Content-Length", "" + content.length );

        String verb = "POST";
        String uri = this.getUri( verb, key );

        return this.requstInput( verb, uri, content );
    }

    private String generateExpiresPolicy() {

        SimpleDateFormat sfmt = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
        String dateStr = sfmt.format( new Date( 1000 * (System
                .currentTimeMillis() / 1000 + this.expires) ) );
        dateStr += ".000Z";
        dateStr = dateStr.replace( ' ', 'T' );

        return dateStr;
    }

    private String[] getSignaturePolicy() throws Exception {

        String[] sign = new String[2];

        String policy = "{\"conditions\": [{\"bucket\": \"" + this.project
                + "\"}, [\"starts-with\", \"$key\", \"\"]], \"expiration\": \""
                + this.generateExpiresPolicy() + "\"}";

        policy = new String( policy.getBytes( "UTF-8" ), "UTF-8" );
        policy = new BASE64Encoder().encode( policy.getBytes() ).trim();

        String policyLine = "";

        // The encoded output stream must be represented in lines of no more
        // than 76 characters each
        for (int index = 0; index <= policy.length(); index += 2) {
            if ((index + 76) > policy.length()) {
                policyLine += policy.substring( index, policy.length() );
            } else {
                policyLine += policy.substring( index, 76 + index );
            }
            index += 76;
        }

        sign[0] = policyLine;

        try {
            Mac mac = Mac.getInstance( "HmacSHA1" );
            mac.init( new SecretKeySpec( this.secretkey.getBytes(), "HmacSHA1" ) );

            sign[1] = new BASE64Encoder().encode( mac.doFinal( policyLine
                    .getBytes() ) );
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

        return sign;
    }

    private boolean mulitpartPost( String uri, ArrayList<String[]> postFields,
            Map<String, String> headers, String fn, byte[] content )
            throws Exception {

        String BOUNDARY = "---------------------------this_boundary$";
        String CRLF = "\r\n";
        String contentType = "multipart/form-data; boundary=" + BOUNDARY;

        String bodyStart = "";

        for (String[] str : postFields) {
            bodyStart += "--" + BOUNDARY + CRLF;
            bodyStart += "Content-Disposition: form-data; name=\"" + str[0]
                    + "\"" + CRLF;
            bodyStart += CRLF;
            bodyStart += str[1] + CRLF;
        }

        bodyStart += "--" + BOUNDARY + CRLF;
        bodyStart += "Content-Disposition: form-data; name=\"file\"; filename=\""
                + fn + "\"" + CRLF;
        bodyStart += "Content-Type: " + this.getFileType( fn ) + CRLF;
        bodyStart += CRLF;

        String bodyEnd = CRLF;
        bodyEnd += "--" + BOUNDARY + "--" + CRLF;

        int len = bodyStart.length() + content.length + bodyEnd.length();

        headers.put( "content-type", contentType );
        headers.put( "content-length", "" + len );

        HttpURLConnection urlconn = this.getHttpHandle( "POST", uri );

        for (String k : headers.keySet()) {
            urlconn.setRequestProperty( k, new String( headers.get( k )
                    .getBytes( "UTF-8" ), "UTF-8" ) );
        }

        OutputStream outStream = urlconn.getOutputStream();
        outStream.write( bodyStart.getBytes(), 0, bodyStart.length() );
        outStream.write( content, 0, content.length );
        outStream.write( bodyEnd.getBytes(), 0, bodyEnd.length() );
        outStream.flush();

        int httpcode = urlconn.getResponseCode();

        if (httpcode != HttpURLConnection.HTTP_OK
                && httpcode != HttpURLConnection.HTTP_CREATED
                && httpcode != HttpURLConnection.HTTP_NO_CONTENT) {
            throw new S3Exception( "HTTP Response Code Error : " + httpcode );
        }

        return true;
    }

    private HttpURLConnection getHttpHandle( String verb, String uri )
            throws Exception {

        URL url = new URL( this.protocol, this.host, this.port, uri );

        HttpURLConnection urlconn;
        if (this.is_ssl) {
            urlconn = (HttpsURLConnection) url.openConnection();
        } else {
            urlconn = (HttpURLConnection) url.openConnection();
        }

        urlconn.setDoOutput( true );
        urlconn.setRequestMethod( verb );

        Map<String, String> headers = new HashMap<String, String>();
        headers.putAll( this.intra_header );
        headers.putAll( this.requst_header );
        for (String k : headers.keySet()) {
            urlconn.setRequestProperty( k, new String( headers.get( k )
                    .getBytes( "UTF-8" ), "UTF-8" ) );
        }
        this.reset_intra();

        return urlconn;
    }

    private boolean checkHttpcode( HttpURLConnection urlconn, String verb )
            throws Exception {

        int httpcode = urlconn.getResponseCode();
        int okcode = this.verb_to_code.get( verb ) == null ? HttpURLConnection.HTTP_OK
                : this.verb_to_code.get( verb );

        if (httpcode != okcode) {
            throw new S3Exception( "HTTP Response Code Error : " + httpcode );
        }

        return true;
    }

    private boolean requst( String verb, String uri ) throws Exception {

        HttpURLConnection urlconn = this.getHttpHandle( verb, uri );

        return this.checkHttpcode( urlconn, verb );
    }

    private boolean requstInput( String verb, String uri, byte[] content )
            throws Exception {

        HttpURLConnection urlconn = this.getHttpHandle( verb, uri );

        OutputStream outStream = urlconn.getOutputStream();
        outStream.write( content, 0, content.length );
        outStream.flush();

        return this.checkHttpcode( urlconn, verb );
    }

    private boolean requstInput( String verb, String uri, String fn )
            throws Exception {

        HttpURLConnection urlconn = this.getHttpHandle( verb, uri );

        OutputStream outputStream = urlconn.getOutputStream();

        byte[] buf = new byte[SinaStorageService.CHUNK];
        InputStream in = null;
        try {
            in = new FileInputStream( fn );
            while (true) {
                int size = in.read( buf );
                if (size <= 0) {
                    break;
                }
                outputStream.write( buf, 0, size );
            }
            outputStream.flush();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e1) {
                    ;
                }
            }
        }

        return this.checkHttpcode( urlconn, verb );
    }

    private ByteArrayOutputStream requstOutput( String verb, String uri )
            throws Exception {

        HttpURLConnection urlconn = this.getHttpHandle( verb, uri );

        boolean res = this.checkHttpcode( urlconn, verb );
        if (!res) {
            return null;
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[SinaStorageService.CHUNK];

        InputStream inputStream = urlconn.getInputStream();

        while (true) {
            int size = inputStream.read( buffer );
            if (size <= 0) {
                break;
            }
            out.write( buffer, 0, size );
        }

        urlconn.disconnect();

        return out;
    }

    /*
     * signature
     */

    private String generateExtra() {

        String ie = this.intra_query.remove( null );
        if (ie == null) {
            ie = "";
        }

        return this.extra.equals( "?" ) ? this.extra + ie : this.extra;
    }

    private String generateQueryString() {

        Map<String, String> qs = new HashMap<String, String>();

        qs.putAll( this.intra_query );
        qs.putAll( this.query_string );

        String qsstr = "";

        ArrayList<String> keys = new ArrayList<String>();
        for (String k : qs.keySet()) {
            keys.add( k );
        }
        Collections.sort( keys );

        for (int i = 0; i < keys.size(); i++) {
            qsstr += keys.get( i ) + "=" + qs.get( keys.get( i ) ) + "&";
        }

        return qsstr;
    }

    private String generateQueryStringSpecific() {

        Map<String, String> qs = new HashMap<String, String>();

        qs.putAll( this.intra_query_specific );
        qs.putAll( this.query_specific );

        String qsstr = "";

        for (String k : qs.keySet()) {
            qsstr += k + "=" + qs.get( k ) + "&";
        }

        return qsstr;
    }

    private Map<String, String> generateRequstHeader() {

        Map<String, String> rh = new HashMap<String, String>();

        rh.put( "hash-info", "" );
        rh.put( "content-type", "" );

        for (String k : this.intra_header.keySet()) {
            try {
                rh.put( k.toLowerCase(), new String( this.intra_header.get( k )
                        .getBytes( "UTF-8" ), "UTF-8" ) );
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        for (String k : this.requst_header.keySet()) {
            try {
                rh.put( k.toLowerCase(), new String( this.requst_header.get( k )
                        .getBytes( "UTF-8" ), "UTF-8" ) );
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

        String[] hash = { "s-sina-sha1", "content-sha1", "s-sina-md5",
                "content-md5" };

        for (int i = 0; i < hash.length; i++) {
            if (rh.containsKey( hash[i] )) {
                rh.put( "hash-info", rh.get( hash[i] ) );
                break;
            }
        }

        return rh;
    }

    private String genarateExpires() {

        long time = this.expires + System.currentTimeMillis() / 1000;

        return "" + time;
    }

    private void fixHeaders() {

        ArrayList<String> fixHeader = new ArrayList<String>();
        fixHeader.add( "s-sina-sha1" );
        fixHeader.add( "content-sha1" );
        fixHeader.add( "s-sina-md5" );
        fixHeader.add( "content-md5" );
        fixHeader.add( "content-type" );

        for (String k : this.intra_header.keySet()) {
            if (fixHeader.contains( k.toLowerCase() )
                    || k.toLowerCase().startsWith( "x-sina-" )
                    || k.toLowerCase().startsWith( "x-amz-" )) {
                this.intra_header.remove( k );
            }
        }
        for (String k : this.query_string.keySet()) {
            if (fixHeader.contains( k.toLowerCase() )
                    || k.toLowerCase().startsWith( "x-sina-" )
                    || k.toLowerCase().startsWith( "x-amz-" )) {
                this.query_string.remove( k );
            }
        }
    }

    private String getUri( String verb, String key ) {

        // key
        String uri = this.vhost ? '/' + key : '/' + this.project + '/' + key;
        // extra
        String extra = this.generateExtra();
        uri += extra.equals( "?" ) ? extra : extra + "&";
        // query string
        uri += this.generateQueryString();

        if (!this.need_auth) {
            uri += this.generateQueryStringSpecific();

            return this.rstrip( this.rstrip( uri, '&' ), '?' );
        }
        // requst header
        Map<String, String> requstHeader = this.generateRequstHeader();

        String hashInfo = requstHeader.get( "hash-info" );
        String contentType = requstHeader.get( "content-type" );

        ArrayList<String> headerToSign = new ArrayList<String>();

        for (String k : requstHeader.keySet()) {
            if (k.startsWith( "x-sina-" ) || k.startsWith( "x-amz-" )) {
                headerToSign.add( k + ":" + requstHeader.get( k ) );
            }
        }
        Collections.sort( headerToSign );

        // fix GET
        if (verb.toUpperCase().equals( "GET" )) {
            hashInfo = "";
            contentType = "";
            headerToSign.clear();

            // clear headers
            this.fixHeaders();
        }

        // expires
        String expiresTime = this.genarateExpires();
        // string to signature
        String stringtosignbuf = verb.toUpperCase() + "\n";
        stringtosignbuf += hashInfo + "\n";
        stringtosignbuf += contentType + "\n";
        stringtosignbuf += expiresTime + "\n";
        for (int i = 0; i < headerToSign.size(); i++) {
            stringtosignbuf += headerToSign.get( i ) + "\n";
        }
        stringtosignbuf += this.rstrip( this.rstrip( uri, '&' ), '?' );

        String ssig = this.generateSsig( stringtosignbuf );

        uri += this.generateQueryStringSpecific();

        uri += "KID=" + this.nation.toLowerCase() + ","
                + this.accessuser.toLowerCase() + "&";
        uri += "Expires=" + expiresTime + "&";
        uri += "ssig=" + ssig;

        return uri;
    }

    @SuppressWarnings( "deprecation" )
    private String generateSsig( String baseStr ) {

        SecretKey secretKey = null;

        try {
            byte[] keyBytes = this.secretkey.getBytes();
            secretKey = new SecretKeySpec( keyBytes, "HmacSHA1" );

            Mac mac;
            mac = Mac.getInstance( "HmacSHA1" );
            mac.init( secretKey );

            byte[] text = baseStr.getBytes();

            String ssig = new BASE64Encoder().encode( mac.doFinal( text ) )
                    .trim();

            String buf = ssig.substring( 5, 15 );
            ssig = URLEncoder.encode( buf );

            return ssig;

        } catch (Exception e) {
            e.printStackTrace();

            return null;
        }
    }

    public String lstrip( String s, char c ) {

        if (s.length() == 0) {
            return s;
        }

        while (s.charAt( 0 ) == c) {
            s = s.substring( 1, s.length() );
        }

        return s;
    }

    public String rstrip( String s, char c ) {

        if (s.length() == 0) {
            return s;
        }

        while (s.charAt( s.length() - 1 ) == c) {
            s = s.substring( 0, s.length() - 1 );
        }

        return s;
    }

    public String strip( String s, char c ) {

        return this.rstrip( this.lstrip( s, c ), c );
    }

    /*
     * Set https
     */
    public void setHttps() throws KeyManagementException,
            NoSuchAlgorithmException {

        TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager(){
            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }

            public void checkClientTrusted( X509Certificate[] certs,
                    String authType ) {
            }

            public void checkServerTrusted( X509Certificate[] certs,
                    String authType ) {
            }
        } };

        SSLContext sc = SSLContext.getInstance( "SSL" );
        sc.init( null, trustAllCerts, new java.security.SecureRandom() );
        HttpsURLConnection.setDefaultSSLSocketFactory( sc.getSocketFactory() );

        HostnameVerifier allHostsValid = new HostnameVerifier(){
            public boolean verify( String hostname, SSLSession session ) {
                return true;
            }
        };

        HttpsURLConnection.setDefaultHostnameVerifier( allHostsValid );

        this.is_ssl = true;
        this.port = 443;
        this.protocol = "https";
    }

    public void setHttps( int port ) throws KeyManagementException,
            NoSuchAlgorithmException {

        this.setHttps();
        this.port = port;
    }

    public void setHttps( int port, int timeout )
            throws KeyManagementException, NoSuchAlgorithmException {

        this.setHttps( port );
        this.timeout = timeout;
    }

    /*
     * Get And Set Private Attribute
     */
    public String getProtocol() {
        return protocol;
    }

    public void setProtocol( String protocol ) {
        this.protocol = protocol;
    }

    public String getHost() {
        return host;
    }

    public void setHost( String host ) {
        this.host = host;
    }

    public String getUphost() {
        return up_host;
    }

    public void setUphost( String host ) {
        this.up_host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort( int port ) {
        this.port = port;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout( int timeout ) {
        this.timeout = timeout;
    }

    public int getExpires() {
        return expires;
    }

    public void setExpires( int expires ) {
        this.expires = expires;
    }

    public boolean isNeed_auth() {
        return need_auth;
    }

    public void setNeed_auth( boolean need_auth ) {
        this.need_auth = need_auth;
    }

    public boolean isVhost() {
        return vhost;
    }

    public void setVhost( boolean vhost ) {
        this.vhost = vhost;
        if (vhost) {
            this.host = this.project;
        } else {
            this.host = SinaStorageService.HOST;
        }
    }

    public boolean isIs_ssl() {
        return is_ssl;
    }

    public String getExtra() {
        return extra;
    }

    public void setExtra( String extra ) {
        this.extra = extra;
    }

    public Map<String, String> getQuery_string() {
        return query_string;
    }

    public void setQuery_string( Map<String, String> query_string ) {

        ArrayList<String> signature = new ArrayList<String>();
        signature.add( "uploadID" );
        signature.add( "partNumber" );
        signature.add( "ip" );

        for (String k : query_string.keySet()) {
            if (signature.contains( k )) {
                this.query_string.put( k, query_string.get( k ) );
            } else {
                this.query_specific.put( k, query_string.get( k ) );
            }
        }
    }

    public Map<String, String> getRequst_header() {
        return requst_header;
    }

    public void setRequst_header( Map<String, String> requst_header ) {
        for (String k : requst_header.keySet()) {
            this.requst_header.put( k, requst_header.get( k ) );
        }
    }

    public Map<String, String> getQuery_specific() {
        return query_specific;
    }

    public void setQuery_specific( Map<String, String> query_specific ) {
        for (String k : query_specific.keySet()) {
            this.query_specific.put( k, query_specific.get( k ) );
        }
    }

}
