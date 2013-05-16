/* Copyright (c) 2008, 2010, Oracle and/or its affiliates. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software Foundation,
  51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA */

#ifndef MYSQL_THREAD_H
#define MYSQL_THREAD_H

/**
  @file mysql/psi/mysql_thread.h
  Instrumentation helpers for mysys threads, mutexes,
  read write locks and conditions.
  This header file provides the necessary declarations
  to use the mysys thread API with the performance schema instrumentation.
  In some compilers (SunStudio), 'static inline' functions, when declared
  but not used, are not optimized away (because they are unused) by default,
  so that including a static inline function from a header file does
  create unwanted dependencies, causing unresolved symbols at link time.
  Other compilers, like gcc, optimize these dependencies by default.

  Since the instrumented APIs declared here are wrapper on top
  of my_pthread / safemutex / etc APIs,
  including mysql/psi/mysql_thread.h assumes that
  the dependency on my_pthread and safemutex already exists.
*/
/*
  Note: there are several orthogonal dimensions here.

  Dimension 1: Instrumentation
  HAVE_PSI_INTERFACE is defined when the instrumentation is compiled in.
  This may happen both in debug or production builds.

  Dimension 2: Debug
  SAFE_MUTEX is defined when debug is compiled in.
  This may happen both with and without instrumentation.

  Dimension 3: Platform
  Mutexes are implemented with one of:
  - the pthread library
  - fast mutexes
  - window apis
  This is implemented by various macro definitions in my_pthread.h

  This causes complexity with '#ifdef'-ery that can't be avoided.
*/

#include "mysql/psi/psi.h"

/**
  @defgroup Thread_instrumentation Thread Instrumentation
  @ingroup Instrumentation_interface
  @{
*/

/**
  An instrumented mutex structure.
  @sa mysql_mutex_t
*/
struct st_mysql_mutex
{
  /** The real mutex. */
  pthread_mutex_t m_mutex;
  /**
    The instrumentation hook.
    Note that this hook is not conditionally defined,
    for binary compatibility of the @c mysql_mutex_t interface.
  */
  struct PSI_mutex *m_psi;
};

/**
  Type of an instrumented mutex.
  @c mysql_mutex_t is a drop-in replacement for @c pthread_mutex_t.
  @sa mysql_mutex_assert_owner
  @sa mysql_mutex_assert_not_owner
  @sa mysql_mutex_init
  @sa mysql_mutex_lock
  @sa mysql_mutex_unlock
  @sa mysql_mutex_destroy
*/
typedef struct st_mysql_mutex mysql_mutex_t;

/**
  An instrumented rwlock structure.
  @sa mysql_rwlock_t
*/
struct st_mysql_rwlock
{
  /** The real rwlock */
  rw_lock_t m_rwlock;
  /**
    The instrumentation hook.
    Note that this hook is not conditionally defined,
    for binary compatibility of the @c mysql_rwlock_t interface.
  */
  struct PSI_rwlock *m_psi;
};

/**
  An instrumented prlock structure.
  @sa mysql_prlock_t
*/
struct st_mysql_prlock
{
  /** The real prlock */
  rw_pr_lock_t m_prlock;
  /**
    The instrumentation hook.
    Note that this hook is not conditionally defined,
    for binary compatibility of the @c mysql_rwlock_t interface.
  */
  struct PSI_rwlock *m_psi;
};

/**
  Type of an instrumented rwlock.
  @c mysql_rwlock_t is a drop-in replacement for @c pthread_rwlock_t.
  @sa mysql_rwlock_init
  @sa mysql_rwlock_rdlock
  @sa mysql_rwlock_tryrdlock
  @sa mysql_rwlock_wrlock
  @sa mysql_rwlock_trywrlock
  @sa mysql_rwlock_unlock
  @sa mysql_rwlock_destroy
*/
typedef struct st_mysql_rwlock mysql_rwlock_t;

/**
  Type of an instrumented prlock.
  A prlock is a read write lock that 'prefers readers' (pr).
  @c mysql_prlock_t is a drop-in replacement for @c rw_pr_lock_t.
  @sa mysql_prlock_init
  @sa mysql_prlock_rdlock
  @sa mysql_prlock_wrlock
  @sa mysql_prlock_unlock
  @sa mysql_prlock_destroy
*/
typedef struct st_mysql_prlock mysql_prlock_t;

/**
  An instrumented cond structure.
  @sa mysql_cond_t
*/
struct st_mysql_cond
{
  /** The real condition */
  pthread_cond_t m_cond;
  /**
    The instrumentation hook.
    Note that this hook is not conditionally defined,
    for binary compatibility of the @c mysql_cond_t interface.
  */
  struct PSI_cond *m_psi;
};

/**
  Type of an instrumented condition.
  @c mysql_cond_t is a drop-in replacement for @c pthread_cond_t.
  @sa mysql_cond_init
  @sa mysql_cond_wait
  @sa mysql_cond_timedwait
  @sa mysql_cond_signal
  @sa mysql_cond_broadcast
  @sa mysql_cond_destroy
*/
typedef struct st_mysql_cond mysql_cond_t;

/*
  Consider the following code:
    static inline void foo() { bar(); }
  when foo() is never called.

  With gcc, foo() is a local static function, so the dependencies
  are optimized away at compile time, and there is no dependency on bar().
  With other compilers (HP, Sun Studio), the function foo() implementation
  is compiled, and bar() needs to be present to link.

  Due to the existing header dependencies in MySQL code, this header file
  is sometime used when it is not needed, which in turn cause link failures
  on some platforms.
  The proper fix would be to cut these extra dependencies in the calling code.
  DISABLE_MYSQL_THREAD_H is a work around to limit dependencies.
  DISABLE_MYSQL_PRLOCK_H is similar, and is used to disable specifically
  the prlock wrappers.
*/
#ifndef DISABLE_MYSQL_THREAD_H

/**
  @def mysql_mutex_assert_owner(M)
  Wrapper, to use safe_mutex_assert_owner with instrumented mutexes.
  @c mysql_mutex_assert_owner is a drop-in replacement
  for @c safe_mutex_assert_owner.
*/
#define mysql_mutex_assert_owner(M) \
  safe_mutex_assert_owner(&(M)->m_mutex)

/**
  @def mysql_mutex_assert_not_owner(M)
  Wrapper, to use safe_mutex_assert_not_owner with instrumented mutexes.
  @c mysql_mutex_assert_not_owner is a drop-in replacement
  for @c safe_mutex_assert_not_owner.
*/
#define mysql_mutex_assert_not_owner(M) \
  safe_mutex_assert_not_owner(&(M)->m_mutex)

/** Wrappers for instrumented prlock objects.  */

#define mysql_prlock_assert_write_owner(M) \
  rw_pr_lock_assert_write_owner(&(M)->m_prlock)

#define mysql_prlock_assert_not_write_owner(M) \
  rw_pr_lock_assert_not_write_owner(&(M)->m_prlock)

/**
  @def mysql_mutex_init(K, M, A)
  Instrumented mutex_init.
  @c mysql_mutex_init is a replacement for @c pthread_mutex_init.
  @param K The PSI_mutex_key for this instrumented mutex
  @param M The mutex to initialize
  @param A Mutex attributes
*/

#ifdef HAVE_PSI_INTERFACE
  #ifdef SAFE_MUTEX
    #define mysql_mutex_init(K, M, A) \
      inline_mysql_mutex_init(K, M, A, __FILE__, __LINE__)
  #else
    #define mysql_mutex_init(K, M, A) \
      inline_mysql_mutex_init(K, M, A)
  #endif
#else
  #ifdef SAFE_MUTEX
    #define mysql_mutex_init(K, M, A) \
      inline_mysql_mutex_init(M, A, __FILE__, __LINE__)
  #else
    #define mysql_mutex_init(K, M, A) \
      inline_mysql_mutex_init(M, A)
  #endif
#endif

/**
  @def mysql_mutex_destroy(M)
  Instrumented mutex_destroy.
  @c mysql_mutex_destroy is a drop-in replacement
  for @c pthread_mutex_destroy.
*/
#ifdef SAFE_MUTEX
  #define mysql_mutex_destroy(M) \
    inline_mysql_mutex_destroy(M, __FILE__, __LINE__)
#else
  #define mysql_mutex_destroy(M) \
    inline_mysql_mutex_destroy(M)
#endif

/**
  @def mysql_mutex_lock(M)
  Instrumented mutex_lock.
  @c mysql_mutex_lock is a drop-in replacement for @c pthread_mutex_lock.
  @param M The mutex to lock
*/

#if defined(SAFE_MUTEX) || defined (HAVE_PSI_INTERFACE)
  #define mysql_mutex_lock(M) \
    inline_mysql_mutex_lock(M, __FILE__, __LINE__)
#else
  #define mysql_mutex_lock(M) \
    inline_mysql_mutex_lock(M)
#endif

/**
  @def mysql_mutex_trylock(M)
  Instrumented mutex_lock.
  @c mysql_mutex_trylock is a drop-in replacement
  for @c pthread_mutex_trylock.
*/

#if defined(SAFE_MUTEX) || defined (HAVE_PSI_INTERFACE)
  #define mysql_mutex_trylock(M) \
    inline_mysql_mutex_trylock(M, __FILE__, __LINE__)
#else
  #define mysql_mutex_trylock(M) \
    inline_mysql_mutex_trylock(M)
#endif

/**
  @def mysql_mutex_unlock(M)
  Instrumented mutex_unlock.
  @c mysql_mutex_unlock is a drop-in replacement for @c pthread_mutex_unlock.
*/
#ifdef SAFE_MUTEX
  #define mysql_mutex_unlock(M) \
    inline_mysql_mutex_unlock(M, __FILE__, __LINE__)
#else
  #define mysql_mutex_unlock(M) \
    inline_mysql_mutex_unlock(M)
#endif

/**
  @def mysql_rwlock_init(K, RW)
  Instrumented rwlock_init.
  @c mysql_rwlock_init is a replacement for @c pthread_rwlock_init.
  Note that pthread_rwlockattr_t is not supported in MySQL.
  @param K The PSI_rwlock_key for this instrumented rwlock
  @param RW The rwlock to initialize
*/
#ifdef HAVE_PSI_INTERFACE
  #define mysql_rwlock_init(K, RW) inline_mysql_rwlock_init(K, RW)
#else
  #define mysql_rwlock_init(K, RW) inline_mysql_rwlock_init(RW)
#endif

/**
  @def mysql_prlock_init(K, RW)
  Instrumented rw_pr_init.
  @c mysql_prlock_init is a replacement for @c rw_pr_init.
  @param K The PSI_rwlock_key for this instrumented prlock
  @param RW The prlock to initialize
*/
#ifdef HAVE_PSI_INTERFACE
  #define mysql_prlock_init(K, RW) inline_mysql_prlock_init(K, RW)
#else
  #define mysql_prlock_init(K, RW) inline_mysql_prlock_init(RW)
#endif

/**
  @def mysql_rwlock_destroy(RW)
  Instrumented rwlock_destroy.
  @c mysql_rwlock_destroy is a drop-in replacement
  for @c pthread_rwlock_destroy.
*/
#define mysql_rwlock_destroy(RW) inline_mysql_rwlock_destroy(RW)

/**
  @def mysql_prlock_destroy(RW)
  Instrumented rw_pr_destroy.
  @c mysql_prlock_destroy is a drop-in replacement
  for @c rw_pr_destroy.
*/
#define mysql_prlock_destroy(RW) inline_mysql_prlock_destroy(RW)

/**
  @def mysql_rwlock_rdlock(RW)
  Instrumented rwlock_rdlock.
  @c mysql_rwlock_rdlock is a drop-in replacement
  for @c pthread_rwlock_rdlock.
*/
#ifdef HAVE_PSI_INTERFACE
  #define mysql_rwlock_rdlock(RW) \
    inline_mysql_rwlock_rdlock(RW, __FILE__, __LINE__)
#else
  #define mysql_rwlock_rdlock(RW) \
    inline_mysql_rwlock_rdlock(RW)
#endif

/**
  @def mysql_prlock_rdlock(RW)
  Instrumented rw_pr_rdlock.
  @c mysql_prlock_rdlock is a drop-in replacement
  for @c rw_pr_rdlock.
*/
#ifdef HAVE_PSI_INTERFACE
  #define mysql_prlock_rdlock(RW) \
    inline_mysql_prlock_rdlock(RW, __FILE__, __LINE__)
#else
  #define mysql_prlock_rdlock(RW) \
    inline_mysql_prlock_rdlock(RW)
#endif

/**
  @def mysql_rwlock_wrlock(RW)
  Instrumented rwlock_wrlock.
  @c mysql_rwlock_wrlock is a drop-in replacement
  for @c pthread_rwlock_wrlock.
*/
#ifdef HAVE_PSI_INTERFACE
  #define mysql_rwlock_wrlock(RW) \
    inline_mysql_rwlock_wrlock(RW, __FILE__, __LINE__)
#else
  #define mysql_rwlock_wrlock(RW) \
    inline_mysql_rwlock_wrlock(RW)
#endif

/**
  @def mysql_prlock_wrlock(RW)
  Instrumented rw_pr_wrlock.
  @c mysql_prlock_wrlock is a drop-in replacement
  for @c rw_pr_wrlock.
*/
#ifdef HAVE_PSI_INTERFACE
  #define mysql_prlock_wrlock(RW) \
    inline_mysql_prlock_wrlock(RW, __FILE__, __LINE__)
#else
  #define mysql_prlock_wrlock(RW) \
    inline_mysql_prlock_wrlock(RW)
#endif

/**
  @def mysql_rwlock_tryrdlock(RW)
  Instrumented rwlock_tryrdlock.
  @c mysql_rwlock_tryrdlock is a drop-in replacement
  for @c pthread_rwlock_tryrdlock.
*/
#ifdef HAVE_PSI_INTERFACE
  #define mysql_rwlock_tryrdlock(RW) \
    inline_mysql_rwlock_tryrdlock(RW, __FILE__, __LINE__)
#else
  #define mysql_rwlock_tryrdlock(RW) \
    inline_mysql_rwlock_tryrdlock(RW)
#endif

/**
  @def mysql_rwlock_trywrlock(RW)
  Instrumented rwlock_trywrlock.
  @c mysql_rwlock_trywrlock is a drop-in replacement
  for @c pthread_rwlock_trywrlock.
*/
#ifdef HAVE_PSI_INTERFACE
  #define mysql_rwlock_trywrlock(RW) \
    inline_mysql_rwlock_trywrlock(RW, __FILE__, __LINE__)
#else
  #define mysql_rwlock_trywrlock(RW) \
    inline_mysql_rwlock_trywrlock(RW)
#endif

/**
  @def mysql_rwlock_unlock(RW)
  Instrumented rwlock_unlock.
  @c mysql_rwlock_unlock is a drop-in replacement
  for @c pthread_rwlock_unlock.
*/
#define mysql_rwlock_unlock(RW) inline_mysql_rwlock_unlock(RW)

/**
  @def mysql_prlock_unlock(RW)
  Instrumented rw_pr_unlock.
  @c mysql_prlock_unlock is a drop-in replacement
  for @c rw_pr_unlock.
*/
#define mysql_prlock_unlock(RW) inline_mysql_prlock_unlock(RW)

/**
  @def mysql_cond_init(K, C, A)
  Instrumented cond_init.
  @c mysql_cond_init is a replacement for @c pthread_cond_init.
  @param C The cond to initialize
  @param K The PSI_cond_key for this instrumented cond
  @param A Condition attributes
*/
#ifdef HAVE_PSI_INTERFACE
  #define mysql_cond_init(K, C, A) inline_mysql_cond_init(K, C, A)
#else
  #define mysql_cond_init(K, C, A) inline_mysql_cond_init(C, A)
#endif

/**
  @def mysql_cond_destroy(C)
  Instrumented cond_destroy.
  @c mysql_cond_destroy is a drop-in replacement for @c pthread_cond_destroy.
*/
#define mysql_cond_destroy(C) inline_mysql_cond_destroy(C)

/**
  @def mysql_cond_wait(C)
  Instrumented cond_wait.
  @c mysql_cond_wait is a drop-in replacement for @c pthread_cond_wait.
*/
#ifdef HAVE_PSI_INTERFACE
  #define mysql_cond_wait(C, M) \
    inline_mysql_cond_wait(C, M, __FILE__, __LINE__)
#else
  #define mysql_cond_wait(C, M) \
    inline_mysql_cond_wait(C, M)
#endif

/**
  @def mysql_cond_timedwait(C, M, W)
  Instrumented cond_timedwait.
  @c mysql_cond_timedwait is a drop-in replacement
  for @c pthread_cond_timedwait.
*/
#ifdef HAVE_PSI_INTERFACE
  #define mysql_cond_timedwait(C, M, W) \
    inline_mysql_cond_timedwait(C, M, W, __FILE__, __LINE__)
#else
  #define mysql_cond_timedwait(C, M, W) \
    inline_mysql_cond_timedwait(C, M, W)
#endif

/**
  @def mysql_cond_signal(C)
  Instrumented cond_signal.
  @c mysql_cond_signal is a drop-in replacement for @c pthread_cond_signal.
*/
#define mysql_cond_signal(C) inline_mysql_cond_signal(C)

/**
  @def mysql_cond_broadcast(C)
  Instrumented cond_broadcast.
  @c mysql_cond_broadcast is a drop-in replacement
  for @c pthread_cond_broadcast.
*/
#define mysql_cond_broadcast(C) inline_mysql_cond_broadcast(C)


/**
  @def mysql_thread_create(K, P1, P2, P3, P4)
  Instrumented pthread_create.
  This function creates both the thread instrumentation and a thread.
  @c mysql_thread_create is a replacement for @c pthread_create.
  The parameter P4 (or, if it is NULL, P1) will be used as the
  instrumented thread "indentity".
  Providing a P1 / P4 parameter with a different value for each call
  will on average improve performances, since this thread identity value
  is used internally to randomize access to data and prevent contention.
  This is optional, and the improvement is not guaranteed, only statistical.
  @param K The PSI_thread_key for this instrumented thread
  @param P1 pthread_create parameter 1
  @param P2 pthread_create parameter 2
  @param P3 pthread_create parameter 3
  @param P4 pthread_create parameter 4
*/
#ifdef HAVE_PSI_INTERFACE
  #define mysql_thread_create(K, P1, P2, P3, P4) \
    inline_mysql_thread_create(K, P1, P2, P3, P4)
#else
  #define mysql_thread_create(K, P1, P2, P3, P4) \
    pthread_create(P1, P2, P3, P4)
#endif

/**
  @def mysql_thread_set_psi_id(I)
  Set the thread indentifier for the instrumentation.
  @param I The thread identifier
*/
#ifdef HAVE_PSI_INTERFACE
  #define mysql_thread_set_psi_id(I) inline_mysql_thread_set_psi_id(I)
#else
  #define mysql_thread_set_psi_id(I) do {} while (0)
#endif

static inline int inline_mysql_mutex_init(
#ifdef HAVE_PSI_INTERFACE
  PSI_mutex_key key,
#endif
  mysql_mutex_t *that,
  const pthread_mutexattr_t *attr
#ifdef SAFE_MUTEX
  , const char *src_file, uint src_line
#endif
  )
{
#ifdef HAVE_PSI_INTERFACE
  that->m_psi= PSI_server ? PSI_server->init_mutex(key, &that->m_mutex)
                          : NULL;
#else
  that->m_psi= NULL;
#endif
#ifdef SAFE_MUTEX
  return safe_mutex_init(&that->m_mutex, attr, src_file, src_line);
#else
  return pthread_mutex_init(&that->m_mutex, attr);
#endif
}

static inline int inline_mysql_mutex_destroy(
  mysql_mutex_t *that
#ifdef SAFE_MUTEX
  , const char *src_file, uint src_line
#endif
  )
{
#ifdef HAVE_PSI_INTERFACE
  if (likely(PSI_server && that->m_psi))
  {
    PSI_server->destroy_mutex(that->m_psi);
    that->m_psi= NULL;
  }
#endif
#ifdef SAFE_MUTEX
  return safe_mutex_destroy(&that->m_mutex, src_file, src_line);
#else
  return pthread_mutex_destroy(&that->m_mutex);
#endif
}

static inline int inline_mysql_mutex_lock(
  mysql_mutex_t *that
#if defined(SAFE_MUTEX) || defined (HAVE_PSI_INTERFACE)
  , const char *src_file, uint src_line
#endif
  )
{
  int result;
#ifdef HAVE_PSI_INTERFACE
  struct PSI_mutex_locker *locker= NULL;
  PSI_mutex_locker_state state;
  if (likely(PSI_server && that->m_psi))
  {
    locker= PSI_server->get_thread_mutex_locker(&state, that->m_psi, PSI_MUTEX_LOCK);
    if (likely(locker != NULL))
      PSI_server->start_mutex_wait(locker, src_file, src_line);
  }
#endif
#ifdef SAFE_MUTEX
  result= safe_mutex_lock(&that->m_mutex, FALSE, src_file, src_line);
#else
  result= pthread_mutex_lock(&that->m_mutex);
#endif
#ifdef HAVE_PSI_INTERFACE
  if (likely(locker != NULL))
    PSI_server->end_mutex_wait(locker, result);
#endif
  return result;
}

static inline int inline_mysql_mutex_trylock(
  mysql_mutex_t *that
#if defined(SAFE_MUTEX) || defined (HAVE_PSI_INTERFACE)
  , const char *src_file, uint src_line
#endif
  )
{
  int result;
#ifdef HAVE_PSI_INTERFACE
  struct PSI_mutex_locker *locker= NULL;
  PSI_mutex_locker_state state;
  if (likely(PSI_server && that->m_psi))
  {
    locker= PSI_server->get_thread_mutex_locker(&state, that->m_psi, PSI_MUTEX_TRYLOCK);
    if (likely(locker != NULL))
      PSI_server->start_mutex_wait(locker, src_file, src_line);
  }
#endif
#ifdef SAFE_MUTEX
  result= safe_mutex_lock(&that->m_mutex, TRUE, src_file, src_line);
#else
  result= pthread_mutex_trylock(&that->m_mutex);
#endif
#ifdef HAVE_PSI_INTERFACE
  if (likely(locker != NULL))
    PSI_server->end_mutex_wait(locker, result);
#endif
  return result;
}

static inline int inline_mysql_mutex_unlock(
  mysql_mutex_t *that
#ifdef SAFE_MUTEX
  , const char *src_file, uint src_line
#endif
  )
{
  int result;
#ifdef HAVE_PSI_INTERFACE
  if (likely(PSI_server && that->m_psi))
    PSI_server->unlock_mutex(that->m_psi);
#endif
#ifdef SAFE_MUTEX
  result= safe_mutex_unlock(&that->m_mutex, src_file, src_line);
#else
  result= pthread_mutex_unlock(&that->m_mutex);
#endif
  return result;
}

static inline int inline_mysql_rwlock_init(
#ifdef HAVE_PSI_INTERFACE
  PSI_rwlock_key key,
#endif
  mysql_rwlock_t *that)
{
#ifdef HAVE_PSI_INTERFACE
  that->m_psi= (PSI_server ? PSI_server->init_rwlock(key, &that->m_rwlock)
                           : NULL);
#else
  that->m_psi= NULL;
#endif
  /*
    pthread_rwlockattr_t is not used in MySQL.
  */
  return my_rwlock_init(&that->m_rwlock, NULL);
}

#ifndef DISABLE_MYSQL_PRLOCK_H
static inline int inline_mysql_prlock_init(
#ifdef HAVE_PSI_INTERFACE
  PSI_rwlock_key key,
#endif
  mysql_prlock_t *that)
{
#ifdef HAVE_PSI_INTERFACE
  that->m_psi= (PSI_server ? PSI_server->init_rwlock(key, &that->m_prlock)
                           : NULL);
#else
  that->m_psi= NULL;
#endif
  return rw_pr_init(&that->m_prlock);
}
#endif

static inline int inline_mysql_rwlock_destroy(
  mysql_rwlock_t *that)
{
#ifdef HAVE_PSI_INTERFACE
  if (likely(PSI_server && that->m_psi))
  {
    PSI_server->destroy_rwlock(that->m_psi);
    that->m_psi= NULL;
  }
#endif
  return rwlock_destroy(&that->m_rwlock);
}

#ifndef DISABLE_MYSQL_PRLOCK_H
static inline int inline_mysql_prlock_destroy(
  mysql_prlock_t *that)
{
#ifdef HAVE_PSI_INTERFACE
  if (likely(PSI_server && that->m_psi))
  {
    PSI_server->destroy_rwlock(that->m_psi);
    that->m_psi= NULL;
  }
#endif
  return rw_pr_destroy(&that->m_prlock);
}
#endif

static inline int inline_mysql_rwlock_rdlock(
  mysql_rwlock_t *that
#ifdef HAVE_PSI_INTERFACE
  , const char *src_file, uint src_line
#endif
  )
{
  int result;
#ifdef HAVE_PSI_INTERFACE
  struct PSI_rwlock_locker *locker= NULL;
  PSI_rwlock_locker_state state;
  if (likely(PSI_server && that->m_psi))
  {
    locker= PSI_server->get_thread_rwlock_locker(&state, that->m_psi,
                                                 PSI_RWLOCK_READLOCK);
    if (likely(locker != NULL))
      PSI_server->start_rwlock_rdwait(locker, src_file, src_line);
  }
#endif
  result= rw_rdlock(&that->m_rwlock);
#ifdef HAVE_PSI_INTERFACE
  if (likely(locker != NULL))
    PSI_server->end_rwlock_rdwait(locker, result);
#endif
  return result;
}

#ifndef DISABLE_MYSQL_PRLOCK_H
static inline int inline_mysql_prlock_rdlock(
  mysql_prlock_t *that
#ifdef HAVE_PSI_INTERFACE
  , const char *src_file, uint src_line
#endif
  )
{
  int result;
#ifdef HAVE_PSI_INTERFACE
  struct PSI_rwlock_locker *locker= NULL;
  PSI_rwlock_locker_state state;
  if (likely(PSI_server && that->m_psi))
  {
    locker= PSI_server->get_thread_rwlock_locker(&state, that->m_psi,
                                                 PSI_RWLOCK_READLOCK);
    if (likely(locker != NULL))
      PSI_server->start_rwlock_rdwait(locker, src_file, src_line);
  }
#endif
  result= rw_pr_rdlock(&that->m_prlock);
#ifdef HAVE_PSI_INTERFACE
  if (likely(locker != NULL))
    PSI_server->end_rwlock_rdwait(locker, result);
#endif
  return result;
}
#endif

static inline int inline_mysql_rwlock_wrlock(
  mysql_rwlock_t *that
#ifdef HAVE_PSI_INTERFACE
  , const char *src_file, uint src_line
#endif
  )
{
  int result;
#ifdef HAVE_PSI_INTERFACE
  struct PSI_rwlock_locker *locker= NULL;
  PSI_rwlock_locker_state state;
  if (likely(PSI_server && that->m_psi))
  {
    locker= PSI_server->get_thread_rwlock_locker(&state, that->m_psi,
                                                 PSI_RWLOCK_WRITELOCK);
    if (likely(locker != NULL))
      PSI_server->start_rwlock_wrwait(locker, src_file, src_line);
  }
#endif
  result= rw_wrlock(&that->m_rwlock);
#ifdef HAVE_PSI_INTERFACE
  if (likely(locker != NULL))
    PSI_server->end_rwlock_wrwait(locker, result);
#endif
  return result;
}

#ifndef DISABLE_MYSQL_PRLOCK_H
static inline int inline_mysql_prlock_wrlock(
  mysql_prlock_t *that
#ifdef HAVE_PSI_INTERFACE
  , const char *src_file, uint src_line
#endif
  )
{
  int result;
#ifdef HAVE_PSI_INTERFACE
  struct PSI_rwlock_locker *locker= NULL;
  PSI_rwlock_locker_state state;
  if (likely(PSI_server && that->m_psi))
  {
    locker= PSI_server->get_thread_rwlock_locker(&state, that->m_psi,
                                                 PSI_RWLOCK_WRITELOCK);
    if (likely(locker != NULL))
      PSI_server->start_rwlock_wrwait(locker, src_file, src_line);
  }
#endif
  result= rw_pr_wrlock(&that->m_prlock);
#ifdef HAVE_PSI_INTERFACE
  if (likely(locker != NULL))
    PSI_server->end_rwlock_wrwait(locker, result);
#endif
  return result;
}
#endif

static inline int inline_mysql_rwlock_tryrdlock(
  mysql_rwlock_t *that
#ifdef HAVE_PSI_INTERFACE
  , const char *src_file, uint src_line
#endif
  )
{
  int result;
#ifdef HAVE_PSI_INTERFACE
  struct PSI_rwlock_locker *locker= NULL;
  PSI_rwlock_locker_state state;
  if (likely(PSI_server && that->m_psi))
  {
    locker= PSI_server->get_thread_rwlock_locker(&state, that->m_psi,
                                                 PSI_RWLOCK_TRYREADLOCK);
    if (likely(locker != NULL))
      PSI_server->start_rwlock_rdwait(locker, src_file, src_line);
  }
#endif
  result= rw_tryrdlock(&that->m_rwlock);
#ifdef HAVE_PSI_INTERFACE
  if (likely(locker != NULL))
    PSI_server->end_rwlock_rdwait(locker, result);
#endif
  return result;
}

static inline int inline_mysql_rwlock_trywrlock(
  mysql_rwlock_t *that
#ifdef HAVE_PSI_INTERFACE
  , const char *src_file, uint src_line
#endif
  )
{
  int result;
#ifdef HAVE_PSI_INTERFACE
  struct PSI_rwlock_locker *locker= NULL;
  PSI_rwlock_locker_state state;
  if (likely(PSI_server && that->m_psi))
  {
    locker= PSI_server->get_thread_rwlock_locker(&state, that->m_psi,
                                                 PSI_RWLOCK_TRYWRITELOCK);
    if (likely(locker != NULL))
      PSI_server->start_rwlock_wrwait(locker, src_file, src_line);
  }
#endif
  result= rw_trywrlock(&that->m_rwlock);
#ifdef HAVE_PSI_INTERFACE
  if (likely(locker != NULL))
    PSI_server->end_rwlock_wrwait(locker, result);
#endif
  return result;
}

static inline int inline_mysql_rwlock_unlock(
  mysql_rwlock_t *that)
{
  int result;
#ifdef HAVE_PSI_INTERFACE
  if (likely(PSI_server && that->m_psi))
    PSI_server->unlock_rwlock(that->m_psi);
#endif
  result= rw_unlock(&that->m_rwlock);
  return result;
}

#ifndef DISABLE_MYSQL_PRLOCK_H
static inline int inline_mysql_prlock_unlock(
  mysql_prlock_t *that)
{
  int result;
#ifdef HAVE_PSI_INTERFACE
  if (likely(PSI_server && that->m_psi))
    PSI_server->unlock_rwlock(that->m_psi);
#endif
  result= rw_pr_unlock(&that->m_prlock);
  return result;
}
#endif

static inline int inline_mysql_cond_init(
#ifdef HAVE_PSI_INTERFACE
  PSI_cond_key key,
#endif
  mysql_cond_t *that,
  const pthread_condattr_t *attr)
{
#ifdef HAVE_PSI_INTERFACE
  that->m_psi= (PSI_server ? PSI_server->init_cond(key, &that->m_cond)
                           : NULL);
#else
  that->m_psi= NULL;
#endif
  return pthread_cond_init(&that->m_cond, attr);
}

static inline int inline_mysql_cond_destroy(
  mysql_cond_t *that)
{
#ifdef HAVE_PSI_INTERFACE
  if (likely(PSI_server && that->m_psi))
  {
    PSI_server->destroy_cond(that->m_psi);
    that->m_psi= NULL;
  }
#endif
  return pthread_cond_destroy(&that->m_cond);
}

static inline int inline_mysql_cond_wait(
  mysql_cond_t *that,
  mysql_mutex_t *mutex
#ifdef HAVE_PSI_INTERFACE
  , const char *src_file, uint src_line
#endif
  )
{
  int result;
#ifdef HAVE_PSI_INTERFACE
  struct PSI_cond_locker *locker= NULL;
  PSI_cond_locker_state state;
  if (likely(PSI_server && that->m_psi))
  {
    locker= PSI_server->get_thread_cond_locker(&state, that->m_psi, mutex->m_psi,
                                               PSI_COND_WAIT);
    if (likely(locker != NULL))
      PSI_server->start_cond_wait(locker, src_file, src_line);
  }
#endif
  result= pthread_cond_wait(&that->m_cond, &mutex->m_mutex);
#ifdef HAVE_PSI_INTERFACE
  if (likely(locker != NULL))
    PSI_server->end_cond_wait(locker, result);
#endif
  return result;
}

static inline int inline_mysql_cond_timedwait(
  mysql_cond_t *that,
  mysql_mutex_t *mutex,
  struct timespec *abstime
#ifdef HAVE_PSI_INTERFACE
  , const char *src_file, uint src_line
#endif
  )
{
  int result;
#ifdef HAVE_PSI_INTERFACE
  struct PSI_cond_locker *locker= NULL;
  PSI_cond_locker_state state;
  if (likely(PSI_server && that->m_psi))
  {
    locker= PSI_server->get_thread_cond_locker(&state, that->m_psi, mutex->m_psi,
                                               PSI_COND_TIMEDWAIT);
    if (likely(locker != NULL))
      PSI_server->start_cond_wait(locker, src_file, src_line);
  }
#endif
  result= pthread_cond_timedwait(&that->m_cond, &mutex->m_mutex, abstime);
#ifdef HAVE_PSI_INTERFACE
  if (likely(locker != NULL))
    PSI_server->end_cond_wait(locker, result);
#endif
  return result;
}

static inline int inline_mysql_cond_signal(
  mysql_cond_t *that)
{
  int result;
#ifdef HAVE_PSI_INTERFACE
  if (likely(PSI_server && that->m_psi))
    PSI_server->signal_cond(that->m_psi);
#endif
  result= pthread_cond_signal(&that->m_cond);
  return result;
}

static inline int inline_mysql_cond_broadcast(
  mysql_cond_t *that)
{
  int result;
#ifdef HAVE_PSI_INTERFACE
  if (likely(PSI_server && that->m_psi))
    PSI_server->broadcast_cond(that->m_psi);
#endif
  result= pthread_cond_broadcast(&that->m_cond);
  return result;
}

#ifdef HAVE_PSI_INTERFACE
static inline int inline_mysql_thread_create(
  PSI_thread_key key,
  pthread_t *thread, const pthread_attr_t *attr,
  void *(*start_routine)(void*), void *arg)
{
  int result;
  if (likely(PSI_server != NULL))
    result= PSI_server->spawn_thread(key, thread, attr, start_routine, arg);
  else
    result= pthread_create(thread, attr, start_routine, arg);
  return result;
}

static inline void inline_mysql_thread_set_psi_id(ulong id)
{
  if (likely(PSI_server != NULL))
  {
    struct PSI_thread *psi= PSI_server->get_thread();
    if (likely(psi != NULL))
      PSI_server->set_thread_id(psi, id);
  }
}
#endif


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


#endif /* DISABLE_MYSQL_THREAD_H */

/** @} (end of group Thread_instrumentation) */

#endif

