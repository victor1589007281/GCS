#ifndef MYSQL_EVENT_H
#define MYSQL_EVENT_H

#include "mysql/psi/mysql_thread.h"

/* copy and modify from innodb */
#define my_assert(expr) \
    if (!(expr)) {\
        fprintf(stderr, "my_assert(%s), FILE :%s, Line : %u", #expr, __FILE__, (uint)__LINE__);\
        assert(0);\
    }
/** Operating system event */
typedef struct mysql_event_struct*	mysql_event_t;

/** An asynchronous signal sent between threads */
struct mysql_event_struct {
#ifdef __WIN1__
	HANDLE		handle;		/*!< kernel event object, slow,
					used on older Windows */
#endif
	pthread_mutex_t	os_mutex;	/*!< this mutex protects the next
					fields */
	my_bool         is_set;		/*!< this is TRUE when the event is
					in the signaled state, i.e., a thread
					does not stop if it tries to wait for
					this event */
	ulonglong       signal_count;	/*!< this is incremented each time
					the event becomes signaled */
	pthread_cond_t  cond_var;	/*!< condition variable is used in
					waiting for the event */
};

static
mysql_event_t
mysql_event_create(
/*============*/
	const char*	name)	/*!< in: the name of the event, if NULL
				the event is created without a name */
{
	mysql_event_t	event;

#ifdef __WIN1__
    event = (mysql_event_t)malloc(sizeof(struct mysql_event_struct));

    event->handle = CreateEvent(NULL,
        TRUE,
        FALSE,
        (LPCTSTR) name);
#else
    event = (mysql_event_t)malloc(sizeof(struct mysql_event_struct));

    pthread_mutex_init(&(event->os_mutex), NULL);
    pthread_cond_init(&(event->cond_var), NULL);
    event->is_set = FALSE;

    event->signal_count = 1;

#endif
	return(event);
}

/**********************************************************//**
Sets an event semaphore to the signaled state: lets waiting threads
proceed. */
static
void
mysql_event_set(
	mysql_event_t	event)	/*!< in: event to set */
{
    my_assert(event);
#ifdef __WIN1__
    SetEvent(event->handle);
#else
    pthread_mutex_lock(&(event->os_mutex));

    if (event->is_set) {
        /* Do nothing */
    } else {
        event->is_set = TRUE;
        event->signal_count += 1;
        pthread_cond_broadcast(&(event->cond_var));
    }

    pthread_mutex_unlock(&(event->os_mutex));
#endif
}

/**********************************************************//**
Resets an event semaphore to the nonsignaled state. Waiting threads will
stop to wait for the event.
The return value should be passed to mysql_even_wait_low() if it is desired
that this thread should not wait in case of an intervening call to
mysql_event_set() between this mysql_event_reset() and the
mysql_event_wait_low() call. See comments for mysql_event_wait_low(). */
static
ulonglong
mysql_event_reset(
	mysql_event_t	event)	/*!< in: event to reset */
{
    ulonglong ret = 0;
    my_assert(event);
#ifdef __WIN1__
        ResetEvent(event->handle);
#else

    pthread_mutex_lock(&(event->os_mutex));
    if (!event->is_set) {
        /* Do nothing */
    } else {
        event->is_set = FALSE;
    }
    ret = event->signal_count;

    pthread_mutex_unlock(&(event->os_mutex));
#endif
    return(ret);
}

/**********************************************************//**
Frees an event object. */
static
void
mysql_event_free(
/*==========*/
	mysql_event_t	event)	/*!< in: event to free */
{
    my_assert(event);
#ifdef __WIN1__
    CloseHandle(event->handle);
#else
    pthread_mutex_destroy(&(event->os_mutex));

    pthread_cond_destroy(&(event->cond_var));
#endif
    free(event);
}

/**********************************************************//**
Waits for an event object until it is in the signaled state.

Typically, if the event has been signalled after the mysql_event_reset()
we'll return immediately because event->is_set == TRUE.
There are, however, situations (e.g.: sync_array code) where we may
lose this information. For example:

thread A calls mysql_event_reset()
thread B calls mysql_event_set()   [event->is_set == TRUE]
thread C calls mysql_event_reset() [event->is_set == FALSE]
thread A calls mysql_event_wait()  [infinite wait!]
thread C calls mysql_event_wait()  [infinite wait!]

Where such a scenario is possible, to avoid infinite wait, the
value returned by mysql_event_reset() should be passed in as
reset_sig_count. */
static
void
mysql_event_wait_low(
/*==============*/
	mysql_event_t	event,		/*!< in: event to wait */
	ulonglong       reset_sig_count)/*!< in: zero or the value
					returned by previous call of
					mysql_event_reset(). */
{
#ifdef __WIN1__
		DWORD	err;

		my_assert(event);
		/* Specify an infinite wait */
		err = WaitForSingleObject(event->handle, INFINITE);

		my_assert(err == WAIT_OBJECT_0);
#else
	pthread_mutex_lock(&event->os_mutex);

	if (!reset_sig_count) {
		reset_sig_count = event->signal_count;
	}

	while (!event->is_set && event->signal_count == reset_sig_count) {
		pthread_cond_wait(&(event->cond_var), &(event->os_mutex));

		/* Solaris manual said that spurious wakeups may occur: we
		have to check if the event really has been signaled after
		we came here to wait */
	}

	pthread_mutex_unlock(&event->os_mutex);
#endif
}

#define mysql_event_wait(event) mysql_event_wait_low(event, 0)
#define mysql_event_wait_time(e, t) mysql_event_wait_time_low(event, t, 0)

#define MYSQL_EVENT_WAIT_INFINITE (uint)(-1)
#define MYSQL_EVENT_WAIT_TIMEOUT 1


/**********************************************************//**
Waits for an event object until it is in the signaled state or
a timeout is exceeded. In Unix the timeout is always infinite.
@return	0 if success, MYSQL_EVENT_WAIT_TIMEOUT if timeout was exceeded */
uint
mysql_event_wait_time_low(
/*===================*/
	mysql_event_t	event,			/*!< in: event to wait */
	uint		time_in_usec,		/*!< in: timeout in
						microseconds, or
						MYSQL_EVENT_WAIT_INFINITE */
	ulonglong   reset_sig_count);	/*!< in: zero or the value
						returned by previous call of
						mysql_event_reset(). */
/*********************************************************//**
Creates an operating system mutex semaphore. Because these are slow, the
mutex semaphore of InnoDB itself (mutex_t) should be used where possible.
@return	the mutex handle */

#endif
