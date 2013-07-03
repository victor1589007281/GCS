#ifndef __MESSAGE_QUEUE_H

#define INVALID_MESSAGE ((unsigned)-1)

//message_queue.h
//异步消息
typedef struct tagMessage
{
    unsigned Msg;
    void*    PARAM_1;
    void*    PARAM_2;
    void*    PARAM_3;
    void*    PARAM_4;
}Message;

class CMessageQueue
{
public:
    CMessageQueue(unsigned size = 25600);
    ~CMessageQueue();

    //无锁添加和取消息
    int  send_message(unsigned msg,void* param1,void* param2,void* param3,void* param4);
    int  get_message(Message& msg);

private:
    //ring buffer
    Message*  _msg_ring_buf;
    unsigned  _ring_buf_size;
    unsigned  _head_pos;
    unsigned  _tail_pos;   
};

#endif
