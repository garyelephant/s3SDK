#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <curl/curl.h>
#include <iconv.h>
#include <openssl/sha.h>
#include <openssl/md5.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <malloc.h>
#include <time.h>
#include <unistd.h>
#include "sinastorage.h"

int base64(const unsigned char *input,int length,char *result,int reslen);
void urlencode(const char *s,int len,char *result);
void processkid(const char *kid,char *result);
void prockidauth(const char *kid,char *result);
static char *hexstr(unsigned char *buf,int len);


size_t writefunc(void *ptr,size_t size,size_t nmemb, void *stream)
{
    return fwrite(ptr,size,nmemb,stream);
}


int download_core(CURL *curlhandle, const char *hostname,const char *project,const char *remotepath,
const char *localpath,const char *kid,const char *secretkey,int timeout,int connecttimeout)
{
    FILE *f;
    curl_off_t local_file_len=-1;
    CURLcode r=CURLE_GOT_NOTHING;
    struct stat file_info;
    const EVP_MD *evp_md;
    unsigned int md_len;
    unsigned char md[1024*8];
    char stringtosignbuf[1024*8],kidbuf[50];
    char  urlbuf[1024*8],remotepathbuf[1024*8];
    char  expires[EXPIRES_LEN],ssig[SSIG_LEN];
    int i,j,k,reslen;
    time_t timer;
    reslen=1024*8;

    timer=time(NULL);
    j=time(&timer);
 
    k=j+timeout;
    sprintf(expires,"%d",k);

    sprintf(remotepathbuf,"%s%s%s",SLASH_CHAR,project,remotepath);

    processkid(kid,kidbuf);          

    sprintf(stringtosignbuf,STRINGTOSIGN_GET "%s" LF_CHAR "%s",expires,remotepathbuf);

    evp_md=EVP_sha1();
    HMAC(evp_md,secretkey,strlen(secretkey),stringtosignbuf,
            strlen(stringtosignbuf),md,&md_len);
    i=base64(md,md_len,stringtosignbuf,reslen);
    if(i!=0)
    return ERROR_BASE64_TOOLONG;

    for(i=5;i<15;i++)
    {
        ssig[i-5]=stringtosignbuf[i];
    }
    ssig[10]='\0';

    urlencode(ssig,strlen(ssig),stringtosignbuf);    

    sprintf(urlbuf,"%s%s" SSIG_PARA "%s" EXPIRES_PARA "%s" KID_PARA "%s",hostname,
            remotepathbuf,stringtosignbuf,expires,kidbuf);

    if(stat(localpath,&file_info)==0)
    {
        local_file_len=file_info.st_size;
    }

    f=fopen(localpath,"wb+");
    if(f==NULL)
    {
        return ERROR_OPEN_LOCALFILE;
    }

    curl_easy_setopt(curlhandle,CURLOPT_URL,urlbuf);
    curl_easy_setopt(curlhandle,CURLOPT_VERBOSE,1);
    curl_easy_setopt(curlhandle,CURLOPT_WRITEDATA,f);
    curl_easy_setopt(curlhandle,CURLOPT_WRITEFUNCTION,writefunc);
    curl_easy_setopt(curlhandle,CURLOPT_CONNECTTIMEOUT,connecttimeout);

    r=curl_easy_perform(curlhandle);
    fclose(f);
    if(r==CURLE_OK)
        return 0;
    else
    {
        return ERROR_CURL_PERFORM;
    }
}


int upload_core(CURL *curlhandle, const char *hostname,const char *project,const char *remotepath,
const char *localpath,const char *kid,const char *secretkey,int timeout,int connecttimeout)
{
    FILE *f;
    CURLcode r=CURLE_GOT_NOTHING;
    const EVP_MD *evp_md;
    unsigned int md_len;
    unsigned char md[1024*8];
    char  stringtosignbuf[1024*8],kidbuf[50];
    char  urlbuf[1024*8],remotepathbuf[1024*8];
    char  expires[EXPIRES_LEN],ssig[SSIG_LEN];
    int i,j,k,reslen;
    time_t timer;
    reslen=1024*8;

    timer=time(NULL);
    j=time(&timer);
 
    k=j+timeout;
    sprintf(expires,"%d",k);

    sprintf(remotepathbuf,"%s%s%s",SLASH_CHAR,project,remotepath);

    processkid(kid,kidbuf);

    sprintf(stringtosignbuf,STRINGTOSIGN_PUT "%s" LF_CHAR "%s",expires,remotepathbuf);

    evp_md=EVP_sha1();
    HMAC(evp_md,secretkey,strlen(secretkey),stringtosignbuf,
            strlen(stringtosignbuf),md,&md_len);
    i=base64(md,md_len,stringtosignbuf,reslen);
    if(i!=0)
    return ERROR_BASE64_TOOLONG;

    for(i=5;i<15;i++)
    {
        ssig[i-5]=stringtosignbuf[i];
    }
    ssig[10]='\0';

    urlencode(ssig,strlen(ssig),stringtosignbuf);

    sprintf(urlbuf,"%s%s" SSIG_PARA "%s" EXPIRES_PARA "%s" KID_PARA "%s",hostname,
            remotepathbuf,stringtosignbuf,expires,kidbuf);

    if(NULL == (f=fopen(localpath,"r")))
    {
      return ERROR_OPEN_LOCALFILE;
    }

    fseek(f,0,SEEK_END);
    int sendSize = ftell(f);
    if(sendSize < 0)
    {
        fclose(f);
        return ERROR_LOCALFILE_EMPTY;
    }

    fseek(f,0,SEEK_SET);

    curl_easy_setopt(curlhandle,CURLOPT_URL,urlbuf);
    curl_easy_setopt(curlhandle,CURLOPT_VERBOSE,1);
    curl_easy_setopt(curlhandle,CURLOPT_READDATA,f);
    curl_easy_setopt(curlhandle,CURLOPT_UPLOAD,1);
    curl_easy_setopt(curlhandle,CURLOPT_INFILESIZE,sendSize);
    curl_easy_setopt(curlhandle,CURLOPT_CONNECTTIMEOUT,connecttimeout);

    r=curl_easy_perform(curlhandle);
    fclose(f);
    if(r==CURLE_OK)
        return 0;
    else
    {
        return ERROR_CURL_PERFORM;
    }

}

int upload_relax_core(CURL *curlhandle,const char *hostname,const char *project,const char *remotepath,
const char *sourcepath,const char *kid,const char *secretkey,int timeout,int connecttimeout)
{
    FILE *f;
    CURLcode r=CURLE_GOT_NOTHING;
    struct curl_slist *headerlist=NULL;
    const EVP_MD *evp_md;
    unsigned int md_len;
    unsigned char md[1024*8];
    char  stringtosignbuf[1024*8],lengthbuf[50],sha1buf[80],authorbuf[80];
    char  urlbuf[1024*8],remotepathbuf[1024*8],datebuf[50],kidbuf[50];
    char  ssig[SSIG_LEN];
    int i,reslen;
    time_t timer;
    struct tm *tm;
    reslen=1024*8;


    timer=time(NULL);
    tm=localtime(&timer);
    strftime(datebuf,50,"%a, %d %b %Y %T",tm);
    sprintf(remotepathbuf,"%s%s%s",SLASH_CHAR,project,remotepath);

    prockidauth(kid,kidbuf);

    if(NULL == (f=fopen(sourcepath,"r")))
    {
      return ERROR_OPEN_LOCALFILE;
    }

    fseek(f,0,SEEK_END);
    int sendSize = ftell(f);
    if(sendSize < 0)
    {
        fclose(f);
        return ERROR_LOCALFILE_EMPTY;
    }

    fseek(f,0,SEEK_SET);
    fread(stringtosignbuf,sendSize,1,f);
    SHA1(stringtosignbuf,strlen(stringtosignbuf),sha1buf);

    sprintf(md,"%s",hexstr(sha1buf,20));
    sprintf(sha1buf,"%s%s",HEADER_S_SINA_SHA1,md);
    sprintf(stringtosignbuf,STRINGTOSIGN_RELAX "%s" LF_CHAR LF_CHAR "%s"
            ZERO_CHAR LF_CHAR "%s" RELAX_PARA,md,datebuf,remotepathbuf);

    evp_md=EVP_sha1();
    HMAC(evp_md,secretkey,strlen(secretkey),stringtosignbuf,
            strlen(stringtosignbuf),md,&md_len);
    i=base64(md,md_len,stringtosignbuf,reslen);
    if(i!=0)
    return ERROR_BASE64_TOOLONG;

    for(i=5;i<15;i++)
    {
        ssig[i-5]=stringtosignbuf[i];
    }
    ssig[10]='\0';


    sprintf(urlbuf,"%s%s" RELAX_PARA,hostname,remotepathbuf);
    sprintf(authorbuf,HEADER_AUTHOR "%s%s",kidbuf,ssig);
    sprintf(lengthbuf,"%s%d",HEADER_S_SINA_LENGTH,sendSize);
    sprintf(md,HEADER_DATE "%s" ZERO_CHAR,datebuf);
  
    headerlist=curl_slist_append(headerlist,sha1buf);
    headerlist=curl_slist_append(headerlist,lengthbuf);
    headerlist=curl_slist_append(headerlist,authorbuf);
    headerlist=curl_slist_append(headerlist,md);    

    curl_easy_setopt(curlhandle,CURLOPT_URL,urlbuf);
    curl_easy_setopt(curlhandle,CURLOPT_VERBOSE,1);
    curl_easy_setopt(curlhandle,CURLOPT_UPLOAD,1);
    curl_easy_setopt(curlhandle,CURLOPT_INFILESIZE,0);
    curl_easy_setopt(curlhandle,CURLOPT_HTTPHEADER,headerlist);
    curl_easy_setopt(curlhandle,CURLOPT_CONNECTTIMEOUT,connecttimeout);
    curl_easy_setopt(curlhandle,CURLOPT_TIMEOUT,timeout);

    r=curl_easy_perform(curlhandle);
    if(r==CURLE_OK)
        return 0;
    else
    {
        return ERROR_CURL_PERFORM;
    }
}


int delete_core(CURL *curlhandle,const char *hostname,const char *project,const char *remotepath,
const char *kid,const char *secretkey,int timeout,int connecttimeout)
{
    CURLcode r=CURLE_GOT_NOTHING;
    const EVP_MD *evp_md;
    unsigned int md_len;
    unsigned char md[1024*8];
    char  stringtosignbuf[1024*8],kidbuf[50];
    char  urlbuf[1024*8],remotepathbuf[1024*8];
    char  expires[EXPIRES_LEN],ssig[SSIG_LEN];
    int i,j,k,reslen;
    time_t timer;
    reslen=1024*8;

    timer=time(NULL);
    j=time(&timer);
 
    k=j+timeout;
    sprintf(expires,"%d",k);

    sprintf(remotepathbuf,"%s%s%s",SLASH_CHAR,project,remotepath);

    processkid(kid,kidbuf);

    sprintf(stringtosignbuf,STRINGTOSIGN_DELETE "%s" LF_CHAR "%s",expires,remotepathbuf);

    evp_md=EVP_sha1();
    HMAC(evp_md,secretkey,strlen(secretkey),stringtosignbuf,
            strlen(stringtosignbuf),md,&md_len);
    i=base64(md,md_len,stringtosignbuf,reslen);
    if(i!=0)
    return ERROR_BASE64_TOOLONG;

    for(i=5;i<15;i++)
    {
        ssig[i-5]=stringtosignbuf[i];
    }
    ssig[10]='\0';

    urlencode(ssig,strlen(ssig),stringtosignbuf);

    sprintf(urlbuf,"%s%s" SSIG_PARA "%s" EXPIRES_PARA "%s" KID_PARA "%s",hostname,
           remotepathbuf,stringtosignbuf,expires,kidbuf);

    curl_easy_setopt(curlhandle,CURLOPT_URL,urlbuf);
    curl_easy_setopt(curlhandle,CURLOPT_VERBOSE,1);
    curl_easy_setopt(curlhandle,CURLOPT_CUSTOMREQUEST,"DELETE"); 
    curl_easy_setopt(curlhandle,CURLOPT_CONNECTTIMEOUT,connecttimeout);

    r=curl_easy_perform(curlhandle);
    if(r==CURLE_OK)
        return 0;
    else
    {
        return ERROR_CURL_PERFORM;
    }


}

int copy_core(CURL *curlhandle,const char *hostname,const char *project,const char *remotepath,
const char *sourcepath,const char *kid,const char *secretkey,int timeout,int connecttimeout)
{
    CURLcode r=CURLE_GOT_NOTHING;
    struct curl_slist *headerlist=NULL;
    const EVP_MD *evp_md;
    unsigned int md_len;
    unsigned char md[1024*8];
    char  stringtosignbuf[1024*8],kidbuf[50];
    char  urlbuf[1024*8],remotepathbuf[1024*8],sourcepathbuf[1024*8];
    char  expires[EXPIRES_LEN],ssig[SSIG_LEN];
    int i,j,k,reslen;
    time_t timer;
    reslen=1024*8;

    timer=time(NULL);
    j=time(&timer);
 
    k=j+timeout;
    sprintf(expires,"%d",k);

    sprintf(remotepathbuf,"%s%s%s",SLASH_CHAR,project,remotepath);

    processkid(kid,kidbuf);
    
    sprintf(urlbuf,"%s%s",HEADER_COPY_SOURCE,sourcepath);
    sprintf(stringtosignbuf,STRINGTOSIGN_PUT "%s" LF_CHAR "%s" LF_CHAR "%s",
           expires,urlbuf,remotepathbuf);

    evp_md=EVP_sha1();
    HMAC(evp_md,secretkey,strlen(secretkey),stringtosignbuf,
            strlen(stringtosignbuf),md,&md_len);
    i=base64(md,md_len,stringtosignbuf,reslen);
    if(i!=0)
    return ERROR_BASE64_TOOLONG;

    for(i=5;i<15;i++)
    {
        ssig[i-5]=stringtosignbuf[i];
    }
    ssig[10]='\0';

    urlencode(ssig,strlen(ssig),stringtosignbuf);

    sprintf(urlbuf,"%s%s" SSIG_PARA "%s" EXPIRES_PARA "%s" KID_PARA "%s",hostname,
           remotepathbuf,stringtosignbuf,expires,kidbuf);

    sprintf(sourcepathbuf,"%s%s",HEADER_COPY_SOURCE,sourcepath);    
    headerlist=curl_slist_append(headerlist,sourcepathbuf);

    curl_easy_setopt(curlhandle,CURLOPT_URL,urlbuf);
    curl_easy_setopt(curlhandle,CURLOPT_VERBOSE,1);
    curl_easy_setopt(curlhandle,CURLOPT_UPLOAD,1);
    curl_easy_setopt(curlhandle,CURLOPT_INFILESIZE,0);
    curl_easy_setopt(curlhandle,CURLOPT_HTTPHEADER,headerlist);
    curl_easy_setopt(curlhandle,CURLOPT_CONNECTTIMEOUT,connecttimeout);

    r=curl_easy_perform(curlhandle);
    if(r==CURLE_OK)
        return 0;
    else
    {
        return ERROR_CURL_PERFORM;
    }
}


int update_meta_core(CURL *curlhandle,const char *hostname,const char *project,const char *remotepath,
const char *sourcepath,const char *kid,const char *secretkey,int timeout,int connecttimeout)
{
    FILE *f;
    CURLcode r=CURLE_GOT_NOTHING;
    struct curl_slist *headerlist=NULL;
    const EVP_MD *evp_md;
    unsigned int md_len;
    unsigned char md[1024*8];
    char  stringtosignbuf[1024*8],sha1buf[80],authorbuf[80];
    char  urlbuf[1024*8],remotepathbuf[1024*8],datebuf[50],kidbuf[50];
    char  ssig[SSIG_LEN];
    int i,j,k,reslen;
    time_t timer;
    struct tm *tm;
    reslen=1024*8;


    timer=time(NULL);
    tm=localtime(&timer);
    j=time(&timer);
    k=j+timeout;
    strftime(datebuf,50,"%a, %d %b %Y %T",tm);
    sprintf(remotepathbuf,"%s%s%s",SLASH_CHAR,project,remotepath);

    prockidauth(kid,kidbuf);

    if(NULL == (f=fopen(sourcepath,"r")))
    {
      return ERROR_OPEN_LOCALFILE;
    }

    fseek(f,0,SEEK_END);
    int sendSize = ftell(f);
    if(sendSize < 0)
    {
        fclose(f);
        return ERROR_LOCALFILE_EMPTY;
    }

    fseek(f,0,SEEK_SET);
    fread(stringtosignbuf,sendSize,1,f);
    SHA1(stringtosignbuf,strlen(stringtosignbuf),sha1buf);

    sprintf(md,"%s",hexstr(sha1buf,20));
    sprintf(sha1buf,"%s%s",HEADER_S_SINA_SHA1,md);
    sprintf(stringtosignbuf,STRINGTOSIGN_RELAX "%s" LF_CHAR LF_CHAR "%s" ZERO_CHAR LF_CHAR HEADER_SINA_INFO 
           "file is modified on %s" LF_CHAR "%s" META_PARA,md,datebuf,datebuf,remotepathbuf);

    evp_md=EVP_sha1();
    HMAC(evp_md,secretkey,strlen(secretkey),stringtosignbuf,
            strlen(stringtosignbuf),md,&md_len);
    i=base64(md,md_len,stringtosignbuf,reslen);
    if(i!=0)
    return ERROR_BASE64_TOOLONG;

    for(i=5;i<15;i++)
    {
        ssig[i-5]=stringtosignbuf[i];
    }
    ssig[10]='\0';


    sprintf(urlbuf,"%s%s" META_PARA,hostname,remotepathbuf);
    sprintf(authorbuf,HEADER_AUTHOR "%s%s",kidbuf,ssig);
    sprintf(stringtosignbuf,HEADER_SINA_INFO "file is modified on %s",datebuf);
    sprintf(md,HEADER_DATE "%s" ZERO_CHAR,datebuf);
  
    headerlist=curl_slist_append(headerlist,sha1buf);
    headerlist=curl_slist_append(headerlist,stringtosignbuf);
    headerlist=curl_slist_append(headerlist,authorbuf);
    headerlist=curl_slist_append(headerlist,md);    

    curl_easy_setopt(curlhandle,CURLOPT_URL,urlbuf);
    curl_easy_setopt(curlhandle,CURLOPT_VERBOSE,1);
    curl_easy_setopt(curlhandle,CURLOPT_UPLOAD,1);
    curl_easy_setopt(curlhandle,CURLOPT_INFILESIZE,0);
    curl_easy_setopt(curlhandle,CURLOPT_HTTPHEADER,headerlist);
    curl_easy_setopt(curlhandle,CURLOPT_CONNECTTIMEOUT,connecttimeout);
    curl_easy_setopt(curlhandle,CURLOPT_TIMEOUT,timeout);

    r=curl_easy_perform(curlhandle);
    if(r==CURLE_OK)
        return 0;
    else
    {
        return ERROR_CURL_PERFORM;
    }
}

int upload_init_core(CURL *curlhandle,const char *hostname,const char *project,const char *remotepath,
const char *kid,const char *secretkey,char *uploadid,int timeout,int connecttimeout)
{
    FILE *f;
    CURLcode r=CURLE_GOT_NOTHING;
    struct curl_slist *headerlist=NULL;
    const EVP_MD *evp_md;
    unsigned int md_len;
    unsigned char md[1024*8],authorbuf[80];
    char  stringtosignbuf[1024*8],contentbuf[80],returnxmlbuf[1024*8];
    char  urlbuf[1024*8],remotepathbuf[1024*8],datebuf[50],kidbuf[50];
    char  expires[EXPIRES_LEN],ssig[SSIG_LEN];
    int i,j,k,reslen;
    time_t timer;
    struct tm *tm;
    reslen=1024*8;


    timer=time(NULL);
    tm=localtime(&timer);
    j=time(&timer);
    k=j+timeout;
    sprintf(expires,"%d",k);
    strftime(datebuf,50,"%a, %d %b %Y %T %Z",tm);
    sprintf(remotepathbuf,"%s%s%s",SLASH_CHAR,project,remotepath);

    prockidauth(kid,kidbuf);

    f=fopen(UPLOADID_TMP_FILE,"wb+");
    if(f==NULL)
    return ERROR_OPEN_LOCALFILE;

    sprintf(stringtosignbuf,STRINGTOSIGN_INIT CONTENT_TYPE_NAME LF_CHAR "%s" 
           LF_CHAR "%s" UPLOAD_PARA,datebuf,remotepathbuf);

    evp_md=EVP_sha1();
    HMAC(evp_md,secretkey,strlen(secretkey),stringtosignbuf,
            strlen(stringtosignbuf),md,&md_len);
    i=base64(md,md_len,stringtosignbuf,reslen);
    if(i!=0)
    return ERROR_BASE64_TOOLONG;

    for(i=5;i<15;i++)
    {
        ssig[i-5]=stringtosignbuf[i];
    }
    ssig[10]='\0';


    sprintf(urlbuf,"%s%s" UPLOAD_PARA,hostname,remotepathbuf);
    sprintf(authorbuf,HEADER_AUTHOR "%s%s",kidbuf,ssig);
    sprintf(md,HEADER_DATE "%s",datebuf);
    sprintf(contentbuf,"%s%s",HEADER_CONTENT_TYPE,CONTENT_TYPE_NAME);  
    sprintf(stringtosignbuf,"%s",EXPECT_CHAR);

    headerlist=curl_slist_append(headerlist,contentbuf);
    headerlist=curl_slist_append(headerlist,stringtosignbuf);
    headerlist=curl_slist_append(headerlist,authorbuf);
    headerlist=curl_slist_append(headerlist,md);    

    curl_easy_setopt(curlhandle,CURLOPT_URL,urlbuf);
    curl_easy_setopt(curlhandle,CURLOPT_VERBOSE,1);
    curl_easy_setopt(curlhandle,CURLOPT_POST,1);
    curl_easy_setopt(curlhandle,CURLOPT_POSTFIELDSIZE,0);
    curl_easy_setopt(curlhandle,CURLOPT_CONNECTTIMEOUT,connecttimeout);
    curl_easy_setopt(curlhandle,CURLOPT_TIMEOUT,timeout);
    curl_easy_setopt(curlhandle,CURLOPT_HTTPHEADER,headerlist);
    curl_easy_setopt(curlhandle,CURLOPT_WRITEFUNCTION,writefunc);
    curl_easy_setopt(curlhandle,CURLOPT_WRITEDATA,f);

    r=curl_easy_perform(curlhandle);
    if(r!=CURLE_OK)
        return ERROR_CURL_PERFORM;

    fseek(f,0,SEEK_END);
    int sendSize = ftell(f);
    if(sendSize < 0)
    {
        fclose(f);
        return ERROR_LOCALFILE_EMPTY;
    }

    fseek(f,0,SEEK_SET);
    fread(returnxmlbuf,sendSize,1,f);
    i=0;
    while(i<sendSize)
    {
        if(returnxmlbuf[i]=='U'&&returnxmlbuf[i+1]=='p'&&returnxmlbuf[i+2]=='l'&&returnxmlbuf[i+3]=='o'&&returnxmlbuf[i+4]=='a'
           &&returnxmlbuf[i+5]=='d'&&returnxmlbuf[i+6]=='I'&&returnxmlbuf[i+7]=='d')
        break;
        i++;
    }
    i+=9;
    for(j=0;j<32;j++)
    {
        uploadid[j]=returnxmlbuf[i];
        i++;
    }
    uploadid[j+1]='\0';

    fclose(f);
    return 0;
}


int upload_block_core(CURL *curlhandle, const char *hostname,const char *project,const char *remotepath,
const char *sourcepath,const char *kid,const char *secretkey,const char *uploadid,int partnum,
int timeout,int connecttimeout)
{
    FILE *f;
    CURLcode r=CURLE_GOT_NOTHING;
    struct curl_slist *headerlist=NULL;
    const EVP_MD *evp_md;
    unsigned int md_len;
    unsigned char md[1024*8];
    char  stringtosignbuf[1024*8],contentbuf[80],authorbuf[80];
    char  urlbuf[1024*8],remotepathbuf[1024*8],datebuf[50],kidbuf[50];
    char  expires[EXPIRES_LEN],ssig[SSIG_LEN];
    int i,j,k,reslen;
    time_t timer;
    struct tm *tm;
    reslen=1024*8;


    timer=time(NULL);
    tm=localtime(&timer);
    j=time(&timer);
    k=j+timeout;
    sprintf(expires,"%d",k);
    strftime(datebuf,50,"%a, %d %b %Y %T %Z",tm);
    sprintf(remotepathbuf,"%s%s%s",SLASH_CHAR,project,remotepath);

    prockidauth(kid,kidbuf);

    if(NULL == (f=fopen(sourcepath,"rb")))
    {
      return ERROR_OPEN_LOCALFILE;
    }

    fseek(f,0,SEEK_END);
    int sendSize = ftell(f);
    if(sendSize < 0)
    {
        fclose(f);
        return ERROR_LOCALFILE_EMPTY;
    }

    fseek(f,0,SEEK_SET);


    sprintf(stringtosignbuf,STRINGTOSIGN_RELAX LF_CHAR CONTENT_TYPE_NAME LF_CHAR "%s" LF_CHAR 
           "%s" PARTNUM_PARA "%d" UPLOADID_BLOCK_PARA "%s",datebuf,remotepathbuf,partnum,uploadid);
    
    evp_md=EVP_sha1();
    HMAC(evp_md,secretkey,strlen(secretkey),stringtosignbuf,
            strlen(stringtosignbuf),md,&md_len);
    i=base64(md,md_len,stringtosignbuf,reslen);
    if(i!=0)
    return ERROR_BASE64_TOOLONG;

    for(i=5;i<15;i++)
    {
        ssig[i-5]=stringtosignbuf[i];
    }
    ssig[10]='\0';


    sprintf(urlbuf,"%s%s" PARTNUM_PARA "%d" UPLOADID_BLOCK_PARA "%s",hostname,remotepathbuf,partnum,
           uploadid);
    sprintf(authorbuf,HEADER_AUTHOR "%s%s",kidbuf,ssig);
    sprintf(stringtosignbuf,"%s",EXPECT_CHAR);
    sprintf(md,HEADER_DATE "%s",datebuf);
    sprintf(contentbuf,"%s%s",HEADER_CONTENT_TYPE,CONTENT_TYPE_NAME);  

    headerlist=curl_slist_append(headerlist,contentbuf);
    headerlist=curl_slist_append(headerlist,stringtosignbuf);
    headerlist=curl_slist_append(headerlist,authorbuf);
    headerlist=curl_slist_append(headerlist,md);    

    curl_easy_setopt(curlhandle,CURLOPT_URL,urlbuf);
    curl_easy_setopt(curlhandle,CURLOPT_VERBOSE,1);
    curl_easy_setopt(curlhandle,CURLOPT_READDATA,f);
    curl_easy_setopt(curlhandle,CURLOPT_UPLOAD,1);
    curl_easy_setopt(curlhandle,CURLOPT_INFILESIZE,sendSize);
    curl_easy_setopt(curlhandle,CURLOPT_CONNECTTIMEOUT,connecttimeout);
    curl_easy_setopt(curlhandle,CURLOPT_TIMEOUT,timeout);
    curl_easy_setopt(curlhandle,CURLOPT_HTTPHEADER,headerlist);

    r=curl_easy_perform(curlhandle);
    fclose(f);
    if(r==CURLE_OK)
        return 0;
    else
    {
        return ERROR_CURL_PERFORM;
    }
}

int upload_complete_core(CURL *curlhandle, const char *hostname,const char *project,const char *remotepath,
const char *sourcepath,const char *kid,const char *secretkey,const char *uploadid,int partnum,int timeout,
int connecttimeout)
{
    FILE *f;
    CURLcode r=CURLE_GOT_NOTHING;
    struct curl_slist *headerlist=NULL;
    const EVP_MD *evp_md;
    unsigned int md_len;
    unsigned char md[1024*8];
    char  stringtosignbuf[1024*8],contentbuf[80],authorbuf[80];
    char  urlbuf[1024*8],remotepathbuf[1024*8],datebuf[50],kidbuf[50];
    char  expires[EXPIRES_LEN],ssig[SSIG_LEN];
    int i,j,k,reslen;
    time_t timer;
    struct tm *tm;
    reslen=1024*8;



    timer=time(NULL);
    tm=localtime(&timer);
    j=time(&timer);
    k=j+timeout;
    sprintf(expires,"%d",k);
    strftime(datebuf,50,"%a, %d %b %Y %T %Z",tm);
    sprintf(remotepathbuf,"%s%s%s",SLASH_CHAR,project,remotepath);

    prockidauth(kid,kidbuf);

    if(NULL == (f=fopen(sourcepath,"r")))
    {
       return ERROR_OPEN_LOCALFILE;
    }

    fseek(f,0,SEEK_END);
    int sendSize = ftell(f);
    if(sendSize < 0)
    {
        fclose(f);
        return ERROR_LOCALFILE_EMPTY;
    }

    fseek(f,0,SEEK_SET);

    sprintf(stringtosignbuf,STRINGTOSIGN_INIT CONTENT_TYPE_NAME LF_CHAR "%s" LF_CHAR 
           "%s" UPLOADID_COMP_PARA "%s",datebuf,remotepathbuf,uploadid);

    evp_md=EVP_sha1();
    HMAC(evp_md,secretkey,strlen(secretkey),stringtosignbuf,
            strlen(stringtosignbuf),md,&md_len);
    i=base64(md,md_len,stringtosignbuf,reslen);
    if(i!=0)
    return ERROR_BASE64_TOOLONG;

    for(i=5;i<15;i++)
    {
        ssig[i-5]=stringtosignbuf[i];
    }
    ssig[10]='\0';


    sprintf(urlbuf,"%s%s" UPLOADID_COMP_PARA "%s",hostname,remotepathbuf,uploadid);
    sprintf(authorbuf,HEADER_AUTHOR "%s%s",kidbuf,ssig);
    sprintf(stringtosignbuf,"%s",EXPECT_CHAR);    
    sprintf(contentbuf,"%s%s",HEADER_CONTENT_TYPE,CONTENT_TYPE_NAME); 
    sprintf(md,HEADER_DATE "%s",datebuf);
  
    headerlist=curl_slist_append(headerlist,contentbuf);
    headerlist=curl_slist_append(headerlist,stringtosignbuf);
    headerlist=curl_slist_append(headerlist,authorbuf);
    headerlist=curl_slist_append(headerlist,md);    

    curl_easy_setopt(curlhandle,CURLOPT_URL,urlbuf);
    curl_easy_setopt(curlhandle,CURLOPT_VERBOSE,1);
    curl_easy_setopt(curlhandle,CURLOPT_READDATA,f);
    curl_easy_setopt(curlhandle,CURLOPT_POST,1);
    curl_easy_setopt(curlhandle,CURLOPT_POSTFIELDSIZE,sendSize);
    curl_easy_setopt(curlhandle,CURLOPT_CONNECTTIMEOUT,connecttimeout);
    curl_easy_setopt(curlhandle,CURLOPT_TIMEOUT,timeout);
    curl_easy_setopt(curlhandle,CURLOPT_HTTPHEADER,headerlist);

    r=curl_easy_perform(curlhandle);
    if(r==CURLE_OK)
        return 0;
    else
    {
        return ERROR_CURL_PERFORM;
    }
}


void processkid(const char *kid,char *result)
{
  int i,j,k,m;  
    
  k=strlen(kid);
  for(i=0;i<k/2;i++)
  {
     if(kid[i]!='0')
     {
       if(kid[i]<'A'||kid[i]>'Z')  
       {
         result[i]=kid[i];       
       } 
       else
       {
         result[i]=kid[i]+32;
       } 
     }
     else
     break;
  }
  result[i]=',';
  j=i;

  for(i=k/2;i<k;i++)
  {
     if(kid[i]!='0')
     break;
  }

  for(m=i;m<k;m++)
  {
     if(kid[m]<'A'||kid[m]>'Z')
     {
       result[j+1]=kid[m];
     }
     else
     {
       result[j+1]=kid[m]+32;
     }  
     j++;
  }
  result[j+1]='\0';

}

void prockidauth(const char *kid,char *result)
{
  int i,j,k,m;  
    
  k=strlen(kid);
  for(i=0;i<k/2;i++)
  {
     if(kid[i]!='0')
     {
       if((kid[i]>='A')&&(kid[i]<='Z'))  
       {
         result[i]=kid[i];       
       } 
       else
       {
         result[i]=kid[i]-32;
       } 
     }
     else
     break;
  }
  result[i]=' ';
  j=i;

  for(i=k/2;i<k;i++)
  {
     if(kid[i]!='0')
     break;
  }

  for(m=i;m<k;m++)
  {
     if(kid[m]<'A'||kid[m]>'Z')
     {
       result[j+1]=kid[m];
     }
     else
     {
       result[j+1]=kid[m]+32;
     }  
     j++;
  }
  result[j+1]=':';
  result[j+2]='\0';
}


void urlencode(const char *s,int len,char *result)
{
    const char *from,*end;
    unsigned char hexchars[]="0123456789ABCDEF";
    char c;

    from=s;
    end=s+len;
    while(from<end)
    {
        c=*from++;
        if(c==' ')
        {
            *result++='+';
        }
        else if((c<'0'&& c!='-'&& c!='.')
                ||(c<'A'&& c>'9')
                ||(c>'Z'&& c<'a'&& c!='_')
                ||(c>'z')){
            result[0]='%';
            result[1]=hexchars[c>>4];
            result[2]=hexchars[c&15];
            result+=3;
        }else{
            *result++=c;
        }
    }
    *result=0;
}

int base64(const unsigned char *input, int length,char *result,int reslen)
{
    BIO *bmem,*b64;
    BUF_MEM *bptr;

    b64=BIO_new(BIO_f_base64());
    bmem=BIO_new(BIO_s_mem());
    b64=BIO_push(b64,bmem);
    BIO_write(b64,input,length);
    BIO_flush(b64);
    BIO_get_mem_ptr(b64,&bptr);

    if((bptr->length)>=reslen)
    return ERROR_BASE64_TOOLONG;
    memcpy(result,bptr->data,bptr->length-1);
    result[bptr->length-1]=0;

    BIO_free_all(b64);
    return 0;
}


static char *hexstr(unsigned char *buf,int len)
{
    const char *set ="0123456789abcdef";
    static char str[80],*tmp;
    unsigned char *end;
    
    end=buf+len; 
    tmp=&str[0];
    while(buf<end)
          {
      *tmp++=set[(*buf)>>4];
      *tmp++=set[(*buf)&0x0F];
      buf++;
           }

    *tmp='\0';
    return str;
}

int upload(const char *hostname,const char *project,const char *remotepath,const char *localpath,
const char *kid,const char *secretkey,int timeout,int connecttimeout)
{
    int res;
    CURLcode return_code;
    CURL *curlhandle=NULL;
    
    return_code=curl_global_init(CURL_GLOBAL_ALL);
    if(CURLE_OK!=return_code)
    {
      return ERROR_CURL_INIT;
    }
    
    curlhandle=curl_easy_init();
    if(NULL==curlhandle)
    {
      return ERROR_CURL_GETHANDLE;
    }
    
    res=upload_core(curlhandle,hostname,project,remotepath,localpath,kid,secretkey,timeout,connecttimeout);

    curl_easy_cleanup(curlhandle);
    curl_global_cleanup();

    if(res!=0)
        return res;
    else
        return 0;
}

int upload_relax(const char *hostname,const char *project,const char *remotepath,const char *sourcepath,
const char *kid,const char *secretkey,int timeout,int connecttimeout)
{
    int res;
    CURLcode return_code;
    CURL *curlhandle=NULL;

    return_code=curl_global_init(CURL_GLOBAL_ALL);
    if(CURLE_OK!=return_code)
    {
      return ERROR_CURL_INIT;
    }

    curlhandle=curl_easy_init();
    if(NULL==curlhandle)
    {
      return ERROR_CURL_GETHANDLE;
    }

    res=upload_relax_core(curlhandle,hostname,project,remotepath,sourcepath,kid,secretkey,timeout,connecttimeout);
    
    curl_easy_cleanup(curlhandle);
    curl_global_cleanup();

    if(res!=0)
        return res;
    else
        return 0;
}

int download(const char *hostname,const char *project,const char *remotepath,const char *localpath,
const char *kid,const char *secretkey,int timeout,int connecttimeout)
{
    int res;
    CURLcode return_code;
    CURL *curlhandle=NULL;

    return_code=curl_global_init(CURL_GLOBAL_ALL);
    if(CURLE_OK!=return_code)
    {
      return ERROR_CURL_INIT;
    }

    curlhandle=curl_easy_init();
    if(NULL==curlhandle)
    {
      return ERROR_CURL_GETHANDLE;
    }

    res=download_core(curlhandle,hostname,project,remotepath,localpath,kid,secretkey,timeout,connecttimeout);
    
    curl_easy_cleanup(curlhandle);
    curl_global_cleanup();

    if(res!=0)
        return res;
    else
        return 0;
}

int delete(const char *hostname,const char *project,const char *remotepath,const char *kid,
const char *secretkey,int timeout,int connecttimeout)
{
    int res;
    CURLcode return_code;
    CURL *curlhandle=NULL;

    return_code=curl_global_init(CURL_GLOBAL_ALL);
    if(CURLE_OK!=return_code)
    {
      return ERROR_CURL_INIT;
    }

    curlhandle=curl_easy_init();
    if(NULL==curlhandle)
    {
      return ERROR_CURL_GETHANDLE;
    }

    res=delete_core(curlhandle,hostname,project,remotepath,kid,secretkey,timeout,connecttimeout);
    
    curl_easy_cleanup(curlhandle);
    curl_global_cleanup();

    if(res!=0)
        return res;
    else
        return 0;
}

int copy(const char *hostname,const char *project,const char *remotepath,const char *sourcepath,
const char *kid,const char *secretkey,int timeout,int connecttimeout)
{
    int res;
    CURLcode return_code;
    CURL *curlhandle=NULL;

    return_code=curl_global_init(CURL_GLOBAL_ALL);
    if(CURLE_OK!=return_code)
    {
      return ERROR_CURL_INIT;
    }

    curlhandle=curl_easy_init();
    if(NULL==curlhandle)
    {
      return ERROR_CURL_GETHANDLE;
    }

    res=copy_core(curlhandle,hostname,project,remotepath,sourcepath,kid,secretkey,timeout,connecttimeout);
    
    curl_easy_cleanup(curlhandle);
    curl_global_cleanup();

    if(res!=0)
        return res;
    else
        return 0;
}

int update_meta(const char *hostname,const char *project,const char *remotepath,const char *sourcepath,
const char *kid,const char *secretkey,int timeout,int connecttimeout)
{
    int res;
    CURLcode return_code;
    CURL *curlhandle=NULL;

    return_code=curl_global_init(CURL_GLOBAL_ALL);
    if(CURLE_OK!=return_code)
    {
      return ERROR_CURL_INIT;
    }

    curlhandle=curl_easy_init();
    if(NULL==curlhandle)
    {
      return ERROR_CURL_GETHANDLE;
    }

    res=update_meta_core(curlhandle,hostname,project,remotepath,sourcepath,kid,secretkey,timeout,connecttimeout);
    
    curl_easy_cleanup(curlhandle);
    curl_global_cleanup();

    if(res!=0)
        return res;
    else
        return 0;
}

int upload_init(const char *hostname,const char *project,const char *remotepath,const char *kid,
const char *secretkey,char *uploadid, int timeout,int connecttimeout)
{
    int res;
    CURLcode return_code;
    CURL *curlhandle=NULL;

    return_code=curl_global_init(CURL_GLOBAL_ALL);
    if(CURLE_OK!=return_code)
    {
      return ERROR_CURL_INIT;
    }

    curlhandle=curl_easy_init();
    if(NULL==curlhandle)
    {
      return ERROR_CURL_GETHANDLE;
    }

    res=upload_init_core(curlhandle,hostname,project,remotepath,kid,secretkey,uploadid,timeout,connecttimeout);
    
    curl_easy_cleanup(curlhandle);
    curl_global_cleanup();

    if(res!=0)
        return res;
    else
        return 0;
}

int upload_block(const char *hostname,const char *project,const char *remotepath,const char *sourcepath,
const char *kid,const char *secretkey,const char *uploadid,int partnum,int timeout,int connecttimeout)
{
    int res;
    CURLcode return_code;
    CURL *curlhandle=NULL;

    return_code=curl_global_init(CURL_GLOBAL_ALL);
    if(CURLE_OK!=return_code)
    {
      return ERROR_CURL_INIT;
    }

    curlhandle=curl_easy_init();
    if(NULL==curlhandle)
    {
      return ERROR_CURL_GETHANDLE;
    }

    res=upload_block_core(curlhandle,hostname,project,remotepath,sourcepath,kid,secretkey,uploadid,partnum,
                       timeout,connecttimeout);
    
    curl_easy_cleanup(curlhandle);
    curl_global_cleanup();

    if(res!=0)
        return res;
    else
        return 0;
}

int upload_complete(const char *hostname,const char *project,const char *remotepath,const char *sourcepath,
const char *kid,const char *secretkey,const char *uploadid,int partnum,int timeout,int connecttimeout)
{
    int res;
    CURLcode return_code;
    CURL *curlhandle=NULL;

    return_code=curl_global_init(CURL_GLOBAL_ALL);
    if(CURLE_OK!=return_code)
    {
      return ERROR_CURL_INIT;
    }

    curlhandle=curl_easy_init();
    if(NULL==curlhandle)
    {
      return ERROR_CURL_GETHANDLE;
    }

    res=upload_complete_core(curlhandle,hostname,project,remotepath,sourcepath,kid,secretkey,uploadid,partnum,timeout,
                          connecttimeout);
    
    curl_easy_cleanup(curlhandle);
    curl_global_cleanup();

    if(res!=0)
        return res;
    else
        return 0;
}
