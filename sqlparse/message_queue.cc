#include "my_global.h"
#include "message_queue.h"

void
my_smp_mb()
{
#ifdef __WIN__
    //TODO
#else
    //smp_mb();
#endif // __WIN__

}

/* ������һ��������С������ֵ */
static unsigned is2n(unsigned un)
{
    return un&(un-1);
}

static unsigned min2n(unsigned un)
{
    int mi = is2n(un);
    return mi?min2n(mi):un;
}

CMessageQueue::CMessageQueue(unsigned size /* = 25600 */)
{
    unsigned i = 0;

    size = min2n(size);

    _ring_buf_size = size;
    _msg_ring_buf = (Message*)calloc(size, sizeof(Message));
    for (i = 0; i < size; ++i)
        _msg_ring_buf[i].Msg = INVALID_MESSAGE;

    _head_pos = 0;
    _tail_pos = 0;
}

CMessageQueue::~CMessageQueue()
{
    free(_msg_ring_buf);
    _msg_ring_buf = NULL;

    _head_pos = 0;
    _tail_pos = 0;
}

int CMessageQueue::send_message(unsigned msg,void* param1,void* param2,void* param3,void* param4)
{
    //������ _tail_pos
    Message* pObj = &_msg_ring_buf[_tail_pos];
    if(pObj->Msg != INVALID_MESSAGE)
    {
        //������
        return -1;
    }

    //my_smp_mb();

    //����Ϣ��ӵ�����β����2������ģ
    _tail_pos = (_tail_pos + 1)&(_ring_buf_size - 1);

    //����һ����Ϣ��¼,����Ϣ��¼��ֵ    
    pObj->PARAM_1 = param1;
    pObj->PARAM_2 = param2;
    pObj->PARAM_3 = param3;
    pObj->PARAM_4 = param4; 

    /* ��֤pObj->Msg���ֵ */
    my_smp_mb();
    pObj->Msg     = msg;    //::TRICKY:���ֶη����ֵ

    return 0;
}

int CMessageQueue::get_message(Message& msg)
{
    //������ _head_pos
    Message* pObj = &_msg_ring_buf[_head_pos];
    if(pObj->Msg == INVALID_MESSAGE)
    {
        //����Ϊ��
        return -1;
    }

    //my_smp_mb();

    //ȡһ����Ϣ
    _head_pos = (_head_pos + 1)&(_ring_buf_size - 1);    

    //������Ϣ
    msg = *pObj;

    my_smp_mb();
    pObj->Msg = INVALID_MESSAGE;

    return 0;
}
