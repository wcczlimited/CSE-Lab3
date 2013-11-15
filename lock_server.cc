// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

#define FREE 1
#define LOCKED 0

lock_server::lock_server():
    nacquire (0)
{
    VERIFY(pthread_mutex_init(&mutex,NULL)==0);
    VERIFY(pthread_mutex_init(&scopedmutex,NULL)==0);
    VERIFY(pthread_cond_init(&cv, NULL)==0);
}

lock_server::~lock_server()
{
    VERIFY(pthread_mutex_destroy(&mutex)==0);
    VERIFY(pthread_mutex_init(&scopedmutex,NULL)==0);
    VERIFY(pthread_cond_destroy(&cv)==0);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
    lock_protocol::status ret = lock_protocol::OK;
    printf("stat request from clt %d\n", clt);
    r = nacquire;
    return ret;
}


lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
    if(lock_owner.find(lid)!=lock_owner.end() && lock_owner[lid]==clt)
        return lock_protocol::OK;
    lock_protocol::status ret = lock_protocol::OK;
    printf("acquire request from clt %d\n", clt);
    locker(lid, clt);
    lock_owner[lid] = clt;
    printf("!!!!!!!!!!!!!!!acquire success from clt %d lockid %d\n", clt, lid);
    r = ++nacquire;
    return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
    lock_protocol::status ret = lock_protocol::OK;
    printf("release request from clt %d\n", clt);
    pthread_mutex_lock(&mutex);

    table_lk[lid] = FREE;
    lock_owner[lid] = -1;
    r = --nacquire;
    //printf("[send signal] thread %d\n", clt);
    printf("@@@@@@@@@@@@@@@release success from clt %d lockid %d\n", clt, lid);
    pthread_cond_signal(&cv);
    pthread_mutex_unlock(&mutex);
    return ret;
}

void
lock_server::locker(lock_protocol::lockid_t lid,int clt)
{
    pthread_mutex_lock(&mutex);
    if(is_lock_exist(lid))
    {
        while(is_lock_locked(lid))
        {
            printf("%d wait for the signal\n", clt);
            pthread_cond_wait(&cv, &mutex);
        }
        printf("%d get the signal\n", clt);
        lock_lock(lid, clt);
        pthread_mutex_unlock(&mutex);
    }
    else
    {
        new_lock(lid, clt);
        pthread_mutex_unlock(&mutex);
    }
}

bool
lock_server::is_lock_exist(lock_protocol::lockid_t lid)
{
    ScopedLock ml(&scopedmutex);
    if(table_lk.find(lid)== table_lk.end())
        return false;
    return true;
}

bool
lock_server::is_lock_locked(lock_protocol::lockid_t lid)
{
    ScopedLock ml(&scopedmutex);
    if(table_lk[lid] == FREE)
        return false;
    return true;
}

void
lock_server::new_lock(lock_protocol::lockid_t lid,int clt)
{
    ScopedLock ml(&scopedmutex);
    printf("[new_lock %11d] thread %d\n", lid, clt);
    table_lk[lid] = LOCKED;
}

void
lock_server::lock_lock(lock_protocol::lockid_t lid,int clt)
{
    ScopedLock ml(&scopedmutex);
    printf("[lock_lock %11d] thread %d\n", lid, clt);
    table_lk[lid] = LOCKED;
}
