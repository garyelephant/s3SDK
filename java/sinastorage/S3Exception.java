package sinastorage;

@SuppressWarnings( "serial" )
public class S3Exception extends Exception{

    public S3Exception(String message) {
        super( message );
    }

}