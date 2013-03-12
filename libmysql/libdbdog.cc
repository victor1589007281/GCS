////////////////////////////////////////////////////////////////////////////////
// IDEA encode / decode

#if defined(__WIN__)
#include <winsock2.h>
#else
#include <sys/param.h>
#include <netinet/in.h>
#endif
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "libdbdog.h"
#include "mysql_version.h"

#define IDEA_BLOCK_SIZE 8
#define IDEA_KEY_SIZE 16

#define MAX_PLAINTEXT_LEN 64

typedef struct idea_key_s
{
    unsigned short EK[6*8+4];
    unsigned short DK[6*8+4];
} idea_key;

#define low16(x) ((x)/* & 0xFFFF*/)
typedef unsigned short uint16;	/* at LEAST 16 bits, maybe more */
typedef unsigned short word16;
typedef unsigned long word32;
typedef unsigned char byte;

#define MUL(x,y) (x = low16(x-1), t16 = low16((y)-1), \
        t32 = (word32)x*t16 + x + t16, x = low16(t32), \
        t16 = t32>>16, x = (x-t16) + (x<t16) + 1)

extern int tc_init(idea_key *self, unsigned char *key);
extern int tc_encrypt_with_key(idea_key *self, unsigned char *in, int in_len, unsigned char *out, int *out_len);
extern int tc_decrypt_with_key(idea_key *self, unsigned char *in, int in_len, unsigned char *out, int *out_len);

static uint16 mulInv(uint16 x)
{
    uint16 t0, t1;
    uint16 q, y;

    if (x <= 1)
        return x;		/* 0 and 1 are self-inverse */
    t1 = (uint16)(0x10001L / x);		/* Since x >= 2, this fits into 16 bits */
    y = 0x10001L % x;
    if (y == 1)
        return low16(1 - t1);
    t0 = 1;
    do {
        q = x / y;
        x = x % y;
        t0 += q * t1;
        if (x == 1)
            return t0;
        q = y / x;
        y = y % x;
        t1 += q * t0;
    } while (y != 1);
    return low16(1 - t1);
} /* mukInv */

static int idea_init(idea_key *self, unsigned char *key)
{
    int i, j;
    uint16 t1, t2, t3;
    word16 *DK, *EK;    

    if (!key) {
        return -1;
    }

    EK = self->EK;
    for (j = 0; j < 8; j++) {
        EK[j] = (key[0] << 8) + key[1];
        key += 2;
    }
    for (i = 0; j < 6*8+4; j++) {
        i++;
        EK[i + 7] = (EK[i & 7] << 9) | (EK[(i + 1) & 7] >> 7);
        EK += i & 8;
        i &= 7;
    }
    EK = self->EK;
    DK = self->DK+6*8+4;    
    t1 = mulInv(*EK++);
    t2 = -*EK++;
    t3 = -*EK++;
    *--DK = mulInv(*EK++);
    *--DK = t3;
    *--DK = t2;
    *--DK = t1;

    for (i = 0; i < 8 - 1; i++) {
        t1 = *EK++;
        *--DK = *EK++;
        *--DK = t1;

        t1 = mulInv(*EK++);
        t2 = -*EK++;
        t3 = -*EK++;
        *--DK = mulInv(*EK++);
        *--DK = t2;
        *--DK = t3;
        *--DK = t1;
    }
    t1 = *EK++;
    *--DK = *EK++;
    *--DK = t1;

    t1 = mulInv(*EK++);
    t2 = -*EK++;
    t3 = -*EK++;
    *--DK = mulInv(*EK++);
    *--DK = t3;
    *--DK = t2;
    *--DK = t1;

    return 0;
}

static void idea_cipher(idea_key *self, byte *block_in, 
                byte *block_out, word16 const *key)
{
    register uint16 x1, x2, x3, x4, s2, s3;
    word16 *in, *out;
    register uint16 t16;
    register word32 t32;
    int r = 8;

    in = (word16 *) block_in;
    x1 = ntohs(*in++);
    x2 = ntohs(*in++);
    x3 = ntohs(*in++);
    x4 = ntohs(*in);
    do {
        MUL(x1, *key++);
        x2 += *key++;
        x3 += *key++;
        MUL(x4, *key++);

        s3 = x3;
        x3 ^= x1;
        MUL(x3, *key++);
        s2 = x2;
        x2 ^= x4;
        x2 += x3;
        MUL(x2, *key++);
        x3 += x2;

        x1 ^= x2;
        x4 ^= x3;

        x2 ^= s3;
        x3 ^= s2;
    } while (--r);
    MUL(x1, *key++);
    x3 += *key++;
    x2 += *key++;
    MUL(x4, *key);

    out = (word16 *) block_out;

    *out++ = htons(x1);
    *out++ = htons(x3);
    *out++ = htons(x2);
    *out = htons(x4);

    block_out[IDEA_BLOCK_SIZE] = '\0';
}


static int idea_encrypt(idea_key *self, 
              unsigned char *in, 
              unsigned char *out)
{
    if (!in || !out) {
        return -1;
    }
    idea_cipher(self, in, out, self->EK);
    return 0;
}

static int idea_decrypt(idea_key *self,
              unsigned char *in, 
              unsigned char *out)
{
    if (!in || !out) {
        return -1;
    }
    idea_cipher(self, in, out, self->DK);
    return 0;
}

////////////////////////////////////////////////////////////////////////////////
// base64 encode and decode

/*
** Translation Table as described in RFC1113
*/
static const char cb64[]="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/*
** Translation Table to decode (created by author)
*/
static const char cd64[]="|$$$}rstuvwxyz{$$$$$$$>?@ABCDEFGHIJKLMNOPQRSTUVW$$$$$$XYZ[\\]^_`abcdefghijklmnopq";

/*
** encodeblock
**
** encode 3 8-bit binary bytes as 4 '6-bit' characters
*/
static void base64_encodeblock(unsigned char in[3], unsigned char out[4], int len )
{
    out[0] = cb64[ in[0] >> 2 ];
    out[1] = cb64[ ((in[0] & 0x03) << 4) | ((in[1] & 0xf0) >> 4) ];
    out[2] = (unsigned char) (len > 1 ? cb64[ ((in[1] & 0x0f) << 2) | ((in[2] & 0xc0) >> 6) ] : '=');
    out[3] = (unsigned char) (len > 2 ? cb64[ in[2] & 0x3f ] : '=');
}

/*
** decodeblock
**
** decode 4 '6-bit' characters into 3 8-bit binary bytes
*/
static void base64_decodeblock(unsigned char in[4], unsigned char out[3] )
{   
    out[ 0 ] = (unsigned char ) (in[0] << 2 | in[1] >> 4);
    out[ 1 ] = (unsigned char ) (in[1] << 4 | in[2] >> 2);
    out[ 2 ] = (unsigned char ) (((in[2] << 6) & 0xc0) | in[3]);
}

static int base64_encodestring(unsigned char *string_in, int in_len, unsigned char *string_out, int *out_len)
{
    unsigned char in[3], out[5];
    int i, len;
    int in_len_idx = 0;
    int out_len_idx = 0;

    if (!string_in || !string_out) {
        return -1;
    }

    while( in_len_idx < in_len) {
        len = 0;
        for( i = 0; i < 3; i++ ) {
            if (in_len_idx < in_len) {
                in[i] = (unsigned char)string_in[in_len_idx++];
                len ++;
            } else {
                in[i] = '\0';
            }
        }
        if( len ) {
            base64_encodeblock( in, out, len );
            out[4] = '\0'; // for stdout
            for( i = 0; i < 4; i++ ) {
                string_out[out_len_idx++] = out[i];
            }
        }
    }
    string_out[out_len_idx] = '\0';
    *out_len = out_len_idx;

    return 0;
}

static int base64_decodestring(unsigned char *string_in, int in_len, unsigned char *string_out, int string_out_len, int *out_len)
{
    unsigned char in[4], out[3], v;
    int i, len;
    int in_len_idx = 0;
    int out_len_idx = 0;
    
    if (!string_in || !string_out) {
        return -1;
    }
    if (in_len <= 0) {
        return -1;
    }

    while( in_len_idx < in_len ) {
        for( len = 0, i = 0; i < 4 && in_len_idx <= in_len; i++ ) {
            v = 0;
            while(in_len_idx <= in_len && v == 0 ) {
                v = (unsigned char) string_in[in_len_idx++];
                v = (unsigned char) ((v < 43 || v > 122) ? 0 : cd64[ v - 43 ]);
                if( v ) {
                    v = (unsigned char) ((v == '$') ? 0 : v - 61);
                }
            }
            if (in_len_idx > in_len) {
                in[i] = 0;
            } else {
                len++;
                if( v ) {
                    in[ i ] = (unsigned char) (v - 1);
                }
            }
        }
        if( len ) {
            base64_decodeblock( in, out );
            for( i = 0; i < len - 1; i++ ) {
				if (out_len_idx >= string_out_len) 
					return -1;

                string_out[out_len_idx++] =  out[i];
            }
        }
    }
    string_out[out_len_idx] = '\0';
    *out_len = out_len_idx;

    return 0;
}


////////////////////////////////////////////////////////////////////////////////
// urlencode and urldecode

const char HEX2DEC[256] =
{
    /*       0  1  2  3   4  5  6  7   8  9  A  B   C  D  E  F */
    /* 0 */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* 1 */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* 2 */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* 3 */  0, 1, 2, 3,  4, 5, 6, 7,  8, 9,-1,-1, -1,-1,-1,-1,

    /* 4 */ -1,10,11,12, 13,14,15,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* 5 */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* 6 */ -1,10,11,12, 13,14,15,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* 7 */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,

    /* 8 */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* 9 */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* A */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* B */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,

    /* C */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* D */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* E */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* F */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1
};

static void url_decode(unsigned char * psrc, int len, unsigned  char * pdst, int * plen)
{
    // Note from RFC1630:  "Sequences which start with a percent sign
    // but are not followed by two hexadecimal characters (0-9, A-F) are reserved
    // for future extension"
    const int SRC_LEN = len ;
    const unsigned char * const SRC_END = psrc + SRC_LEN ;
    const unsigned char * const SRC_LAST_DEC = SRC_END - 2;   // last decodable '%'

    char * const pstart = (char *)malloc(SRC_LEN) ;
    char * pend = pstart ;

    while (psrc < SRC_LAST_DEC) {
       if (*psrc == '%') {
            char dec1, dec2;
            if (-1 != (dec1 = HEX2DEC[*(psrc + 1)])
                && -1 != (dec2 = HEX2DEC[*(psrc + 2)]))  {
                *pend++ = (dec1 << 4) + dec2;
                psrc += 3;
                continue;
            }
        }
        *pend++ = *psrc++;
    }

    // the last 2- chars
    while (psrc < SRC_END) {
        *pend++ = *psrc++;
    }
    *plen = (pend - pstart) ;
    memcpy(pdst, pstart, *plen) ;
    if (pstart) {
        free(pstart);
    }
    pdst[*plen] = '\0';
}

// Only alphanum is safe.
static char SAFE[256] =
{
    /*      0 1 2 3  4 5 6 7  8 9 A B  C D E F */
    /* 0 */ 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,
    /* 1 */ 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,
    /* 2 */ 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,
    /* 3 */ 1,1,1,1, 1,1,1,1, 1,1,0,0, 0,0,0,0,

    /* 4 */ 0,1,1,1, 1,1,1,1, 1,1,1,1, 1,1,1,1,
    /* 5 */ 1,1,1,1, 1,1,1,1, 1,1,1,0, 0,0,0,0,
    /* 6 */ 0,1,1,1, 1,1,1,1, 1,1,1,1, 1,1,1,1,
    /* 7 */ 1,1,1,1, 1,1,1,1, 1,1,1,0, 0,0,0,0,

    /* 8 */ 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,
    /* 9 */ 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,
    /* A */ 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,
    /* B */ 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,

    /* C */ 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,
    /* D */ 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,
    /* E */ 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,
    /* F */ 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0
};

static void url_encode(unsigned char * psrc, int len, unsigned char * pdst, int * plen)
{

    const char DEC2HEX[16 + 1] = "0123456789ABCDEF";
    const int SRC_LEN = len ;
    unsigned char * const pstart = (unsigned char *)malloc(SRC_LEN * 3) ;
    unsigned char * pend = pstart;
    const unsigned char * const SRC_END = psrc + SRC_LEN;
    for (; psrc < SRC_END; ++psrc) {
       if (SAFE[*psrc]) {
            *pend++ = *psrc;
       } else {
            // escape this char
            *pend++ = '%';
            *pend++ = DEC2HEX[*psrc >> 4];
            *pend++ = DEC2HEX[*psrc & 0x0F];
       }
    }
    *plen = pend - pstart ;
    memcpy(pdst, pstart, *plen) ;
    if (pstart){
        free(pstart);
    }
    pdst[*plen] = '\0';
}

int tc_init(idea_key *self, unsigned char *key)
{
    return idea_init(self, key);
}

int tc_encrypt_with_key(idea_key *self, unsigned char *in, int in_len, unsigned char *out, int *out_len)
{
    int res = 0;

    if (!in) {
        fprintf(stderr, "ERROR: plaintext is a null pointer!\n");
        return -1;
    }
    if (in_len <= 0) {
        fprintf(stderr, "ERROR: plaintext length must always be greater than 0!\n");
        return -1;
    }
    if (in_len > MAX_PLAINTEXT_LEN) {
        fprintf(stderr, "ERROR: plaintext length must always be less than or equal to %d!\n", MAX_PLAINTEXT_LEN);
        return -1;
    }
    if (!out) {
        fprintf(stderr, "ERROR: ciphertext buffer is a null pointer!\n");
        return -1;
    }

    unsigned char *idea_out = NULL;
    int len = in_len / IDEA_BLOCK_SIZE * IDEA_BLOCK_SIZE + ((in_len % IDEA_BLOCK_SIZE) ? IDEA_BLOCK_SIZE: 0);
    idea_out = (unsigned char *)malloc(sizeof(unsigned char) * (len + 1));
    if (!idea_out) {
        fprintf(stderr, "ERROR: no memory for a %08X sized request!\n", len + 1);
        return -1;
    }
    unsigned char *base64_in = idea_out;
    unsigned char *p = idea_out;

    // IDEA encode
    for (; ; ) {
        res = idea_encrypt(self, in, p);
        if (res) {
            fprintf(stderr, "ERROR: This should never happen!\n");
            goto error;
        }
        in_len -= IDEA_BLOCK_SIZE;
        if (in_len <= 0) {
            break;
        }
        p += IDEA_BLOCK_SIZE;
        in += IDEA_BLOCK_SIZE;
    }

    // Base64 encode
    int base64_in_len = len;
    int base64_len = len / 3 * 4 + 4;
    unsigned char *base64_out = NULL;
    base64_out = (unsigned char *)malloc(sizeof(unsigned char) * (base64_len + 1));
    if (!base64_out) {
        fprintf(stderr, "ERROR: no memory for a %08X sized request!\n", base64_len + 1);
        res = -1;
        goto error;
    }
    int base64_out_len;
    res = base64_encodestring(base64_in, base64_in_len, base64_out, &base64_out_len);
    if (res != 0) {
        fprintf(stderr, "ERROR: NULL pointer, go away!\n");
        goto error;
    }

    // urlencode
    unsigned char *url_in = base64_out;
    int url_in_len = base64_out_len;
    url_encode(url_in, url_in_len, out, out_len);

error:
    if (idea_out) {
        free(idea_out);
    }
    if (base64_out) {
        free(base64_out);
    }

    return res;
}

int tc_encrypt(unsigned char *in, int in_len, unsigned char *out, int *out_len)
{
    int res = 0;

    idea_key key;
    idea_key *self = &key;
    char key_value[IDEA_KEY_SIZE + 1];
    memset(key_value, 0, IDEA_KEY_SIZE + 1);
    strncpy(key_value, IDEA_KEY, IDEA_KEY_SIZE) ;
    res = tc_init(&key, (unsigned char *)key_value);
    if (res) {
        return res;
    }

    res = tc_encrypt_with_key(self, in, in_len, out, out_len);
    return res;
}

int tc_decrypt_with_key(idea_key *self, unsigned char *in, int in_len, unsigned char *out, int *out_len)
{
    int res = 0;

    if (!in) {
        fprintf(stderr, "ERROR: ciphertext is a null pointer!\n");
        return -1;
    }
    if (in_len <= 0) {
        fprintf(stderr, "ERROR: ciphertext length must always be greater than 0!\n");
        return -1;
    }
    if (!out) {
        fprintf(stderr, "ERROR: plaintext buffer is a null pointer!\n");
        return -1;
    }
    if (in_len >= MAX_CIPHERTEXT_LEN) {
        fprintf(stderr, "ERROR: ciphertext length must always be less than or equal to %d!\n", MAX_CIPHERTEXT_LEN);
        return -1;
    }

    // urldecode
    unsigned char *url_out = NULL;
    url_out = (unsigned char *)malloc(sizeof(unsigned char) * (in_len + 1));
    if (!url_out) {
        fprintf(stderr, "ERROR: no memory for a %08X sized request!\n", in_len + 1);
        res = -1;
        return res;
    }
    int url_out_len;
    url_decode(in, in_len, url_out, &url_out_len);

    // Base64 decode
    int base64_in_len = url_out_len;
    unsigned char *base64_in = url_out;
    int base64_out_len = (base64_in_len / 4 + 1) * 3;
    unsigned char *base64_out  = NULL;
    base64_out = (unsigned char *)malloc(sizeof(unsigned char) * (base64_out_len + 1));
    if (!base64_out) {
        fprintf(stderr, "ERROR: no memory for a %08X sized request!\n", base64_out_len + 1);
        res = -1;
        goto error;
    }
    memset(base64_out, 0, base64_out_len + 1);

	
    res = base64_decodestring(base64_in, base64_in_len, base64_out, base64_out_len, &base64_out_len);
    if (res != 0) {
        fprintf(stderr, "ERROR: NULL pointer, go away!\n");
        goto error;
    } 
 
    // IDEA decode
    unsigned char *idea_in = base64_out;
    int idea_in_len = base64_out_len;
    int idea_out_len;
    unsigned char idea_out[IDEA_BLOCK_SIZE + 1];
    *out_len = 0;
    for (; ; ) {
        res = idea_decrypt(self, idea_in, idea_out);
        if (res != 0) {
            fprintf(stderr, "ERROR: NULL pointer, go away!\n");
            goto error;
        } 
		idea_out_len = strlen((const char *)&idea_out);
        *out_len += idea_out_len;
        strncpy((char *)out, (const char *)idea_out, idea_out_len);
        idea_in += IDEA_BLOCK_SIZE;
        idea_in_len -= IDEA_BLOCK_SIZE;
        if (idea_in_len <= 0) {
            break;
        }

        out += idea_out_len;
    }

error:
    if (url_out) {
        free(url_out);
    }
    if (base64_out) {
        free(base64_out);
    }

    return res;
}

int tc_decrypt(unsigned char *in, int in_len, unsigned char *out, int *out_len)
{
	if (strlen(IDEA_KEY) == 0)
	{
		void *ptr;
		ptr = memcpy(out,in,strlen((const char*)in));
		if(ptr)
			return 0;
		else
			return 1;
	}
    int res = 0;
    idea_key key;
    idea_key *self = &key;
    unsigned char key_value[IDEA_KEY_SIZE + 1];
    memset(key_value, 0, IDEA_KEY_SIZE + 1);
    strncpy((char *)key_value, IDEA_KEY, IDEA_KEY_SIZE) ;
    res = tc_init(&key, key_value);
    if (res) {
        return res;
    }

    res = tc_decrypt_with_key(self, in, in_len, out, out_len);
    return res;
}
