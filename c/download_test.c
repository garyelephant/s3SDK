#include "sinastorage.h"


int main(int c,char *argv[])
{
  char *hostname="sinastorage.com";
  char *project="sandbox";
  char *remotepath="/ty/test1.c";
  char *localpath="sp.c";
  char *kid="SYS0000001000SANDBOX";
  char *secretkey="1111111111111111111111111111111111111111";
  int timeout=900;
  int connecttimeout=1;

  download(hostname,project,remotepath,localpath,kid,secretkey,timeout,connecttimeout);
  return 0;
}

