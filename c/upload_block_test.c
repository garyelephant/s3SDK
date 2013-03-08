#include "sinastorage.h"

int main(int c,char *argv[])
{
  char *hostname="sinastorage.com";
  char *project="sandbox";
  char *remotepath="/ty/9.c";
  char *sourcepath="/tmp/new1.c";
  char *kid="SYS0000000000SANDBOX";
  char *secretkey="1111111111111111111111111111111111111111";
  char *uploadid="acb6511ecf7d4eaeb48a8da855c32cec";
  int partnum=1;
  int timeout=1800;
  int connecttimeout=1;

  upload_block(hostname,project,remotepath,sourcepath,kid,secretkey,uploadid,
               partnum,timeout,connecttimeout);
  return 0;
}
