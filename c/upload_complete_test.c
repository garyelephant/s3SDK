#include "sinastorage.h"

int main(int c,char *argv[])
{
  char *hostname="sinastorage.com";
  char *project="sandbox";
  char *remotepath="/ty/9.c";
  char *sourcepath="new1.xml";
  char *kid="SYS0000000000SANDBOX";
  char *secretkey="1111111111111111111111111111111111111111";
  char *uploadid="f3c8de9b51a44d83ac657b55f791f863";
  int partnum=2;
  int timeout=1800;
  int connecttimeout=1;

  upload_complete(hostname,project,remotepath,sourcepath,kid,secretkey,uploadid,
                  partnum,timeout,connecttimeout);
  return 0;
}
