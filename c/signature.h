#ifndef _SIGNATURE_H_
#define _SIGNATURE_H_

#define ERROR_BASE64_TOOLONG  (-1)

#define SSIG_LEN 11

int signature(const char *secretkey, const char *stringtosign, char *result);

#endif
