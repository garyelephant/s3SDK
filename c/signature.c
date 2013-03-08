#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/buffer.h>
#include <string.h>
#include "signature.h"

int base64(const unsigned char *input,int length,char *result,int reslen);
void urlencode(const char *s,int len,char *result);


int signature(const char *secretkey, const char *stringtosign, char *result)
{
    const EVP_MD *evp_md;
    unsigned int md_len;
    unsigned char md[1024*8],resultbuf[20];
    char stringtosignbuf[1024*8],ssig[SSIG_LEN];
    int i,reslen;
    reslen=1024*8;

    sprintf(stringtosignbuf,"%s",stringtosign);   
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

    urlencode(ssig,strlen(ssig),resultbuf);    
 
    memcpy(result,resultbuf,strlen(resultbuf));
    result[strlen(resultbuf)]=0;   
    return 0;
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

