#include "sinastorage.h"

int main(int c,char *argv[])
{
  char *hostname="sinastorage.com";
  char *project="sandbox";
  char *remotepath="/ty/test1.c";
  char *localpath="new1.c";
  char *kid="SYS0000000000SANDBOX";
  char *secretkey="1111111111111111111111111111111111111111";
  int timeout=1800;
  int connecttimeout=2;

  upload(hostname,project,remotepath,localpath,kid,secretkey,timeout,connecttimeout);
  return 0;
}
