#include "sinastorage.h"

int main(int c,char *argv[])
{
  char *hostname="sinastorage.com";
  char *project="sandbox";
  char *remotepath="/4.c";
  char *sourcepath="/sandbox/ty/9.c";
  char *kid="SYS0000000000SANDBOX";
  char *secretkey="1111111111111111111111111111111111111111";
  int timeout=1800;
  int connecttimeout=1;

  copy(hostname,project,remotepath,sourcepath,kid,secretkey,timeout,connecttimeout);
  return 0;
}
