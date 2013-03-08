#ifndef _SINASTORAGE_H_
#define _SINASTORAGE_H_

#define ERROR_CURL_INIT         (-1)
#define ERROR_CURL_GETHANDLE    (-2)
#define ERROR_OPEN_LOCALFILE    (-3)
#define ERROR_LOCALFILE_EMPTY   (-4)
#define ERROR_CURL_PERFORM      (-5)
#define ERROR_BASE64_TOOLONG    (-6)

#define STRINGTOSIGN_GET     "GET\n\n\n"
#define STRINGTOSIGN_PUT     "PUT\n\n\n"
#define STRINGTOSIGN_DELETE  "DELETE\n\n\n"
#define STRINGTOSIGN_RELAX   "PUT\n"
#define STRINGTOSIGN_INIT    "POST\n\n"
#define LF_CHAR              "\n"
#define SSIG_PARA            "?ssig="
#define EXPIRES_PARA         "&Expires="
#define KID_PARA             "&KID="
#define SLASH_CHAR           "/"
#define HEADER_COPY_SOURCE   "x-amz-copy-source:"
#define HEADER_S_SINA_SHA1   "s-sina-sha1:"
#define HEADER_CONTENT_MD5   "Content-MD5:"
#define HEADER_S_SINA_LENGTH "s-sina-length:"
#define RELAX_PARA           "?relax"
#define HEADER_AUTHOR        "Authorization:"
#define HEADER_DATE          "Date:"
#define ZERO_CHAR            " +0000"
#define META_PARA            "?meta"
#define HEADER_SINA_EXPIRE   "x-sina-expire:"
#define HEADER_SINA_INFO     "x-sina-info:"
#define UPLOAD_PARA          "?uploads"
#define CONTENT_TYPE_NAME    "application/xml"
#define HEADER_CONTENT_TYPE  "Content-Type:"
#define PARTNUM_PARA         "?partNumber="
#define UPLOADID_BLOCK_PARA  "&uploadId="
#define UPLOADID_COMP_PARA   "?uploadId="
#define EXPECT_CHAR          "Expect:"
#define UPLOADID_TMP_FILE    "uploadid.xml"

#define EXPIRES_LEN 15
#define SSIG_LEN 11

int upload(const char *hostname,const char *project,const char *remotepath,
const char *localpath,const char *kid,const char *secretkey,int timeout,
int connecttimeout);

int download(const char *hostname,const char *project,const char *remotepath,
const char *localpath,const char *kid,const char *secretkey,int timeout,
int connecttimeout);

int delete(const char *hostname,const char *project,const char *remotepath,
const char *kid,const char *secretkey,int timeout,int connecttimeout);

int copy(const char *hostname,const char *project,const char *remotepath,
const char *sourcepath,const char *kid,const char *secretkey,int timeout,
int connecttimeout);

int upload_relax(const char *hostname,const char *project,const char *remotepath,
const char *sourcepath,const char *kid,const char *secretkey,int timeout,
int connecttimeout);

int update_meta(const char *hostname,const char *project,const char *remotepath,
const char *sourcepath,const char *kid,const char *secretkey,int timeout,
int connecttimeout);

int upload_init(const char *hostname,const char *project,const char *remotepath,
const char *kid,const char *secretkey,char *uploadid,int timeout,
int connecttimeout);

int upload_block(const char *hostname,const char *project,const char *remotepath,
const char *sourcepath,const char *kid,const char *secretkey,const char *uploadid,
int partnum,int timeout,int connecttimeout);

int upload_complete(const char *hostname,const char *project,const char *remotepath,
const char *sourcepath,const char *kid,const char *secretkey,const char *uploadid,
int partnum,int timeout,int connecttimeout);
#endif
