#include <stdio.h>
#include "signature.h"

int main(int argc, char **argv)
{
   char *secretkey="1111111111111111111111111111111111111111";
   char *stringtosign="GET\n\n\n1175139620\n/sinastorage.com/sandbox/123.jpg";
   char result[20];
   int res;
   
   res=signature(secretkey,stringtosign,result);
   if(res)
   printf("signature failed!\n");
   else
   printf("signature success,result:%s\n",result);
}

