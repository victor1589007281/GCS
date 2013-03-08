#ifndef TC_MYCONNECT_H_
#define TC_MYCONNECT_H_

/* Maximum plaintext length */
#define MAX_PLAINTEXT_LEN 64

#define MAX_CIPHERTEXT_LEN_0    ((MAX_PLAINTEXT_LEN) / 8 * 8 + ((MAX_PLAINTEXT_LEN % 8) ? 8: 0))
#define MAX_CIPHERTEXT_LEN_1    ((MAX_CIPHERTEXT_LEN_0) / 3 * 4 + 4)
/* Maximum ciphertext length */
#define MAX_CIPHERTEXT_LEN      ((MAX_CIPHERTEXT_LEN_1) * 3)

#ifdef  __cplusplus
extern "C" {
#endif

extern int tc_encrypt(unsigned char *in, int in_len, unsigned char *out, int *out_len);
extern int tc_decrypt(unsigned char *in, int in_len, unsigned char *out, int *out_len);

#ifdef  __cplusplus
}
#endif

#endif
