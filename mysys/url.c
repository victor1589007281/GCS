#include <my_global.h>
#include <m_string.h>  /* strchr() */
#include <m_ctype.h>  /* my_isspace() */
#include <my_sys.h>
#include <url.h>

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

int url_decode(unsigned char * psrc, int len, unsigned  char ** pdst, int * plen)
{
    // Note from RFC1630:  "Sequences which start with a percent sign
    // but are not followed by two hexadecimal characters (0-9, A-F) are reserved
    // for future extension"
    const int SRC_LEN = len ;
    const unsigned char * const SRC_END = psrc + SRC_LEN ;
    const unsigned char * const SRC_LAST_DEC = SRC_END - 2;   // last decodable '%'
    char* pend;

    char * const pstart = (char *)my_malloc(SRC_LEN+1, MYF(MY_FAE));
    if (!pstart)
    	return -1;
    	
    pend = pstart ;

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
    *pdst = pstart;
    (*pdst)[*plen] = '\0';
    
    return 0;
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

int url_encode(unsigned char * psrc, int len, unsigned char ** pdst, int * plen)
{

    const char DEC2HEX[16 + 1] = "0123456789ABCDEF";
    const int SRC_LEN = len ;
    unsigned char * const pstart = (unsigned char *)my_malloc(SRC_LEN * 3 + 1, MYF(MY_FAE)) ;
    unsigned char * pend;
    const unsigned char * SRC_END;
    if (!pstart) 
    	return -1;
    pend = pstart;
    SRC_END = psrc + SRC_LEN;
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
    *pdst = pstart;
    (*pdst)[*plen] = '\0';
    
    return 0;
}
