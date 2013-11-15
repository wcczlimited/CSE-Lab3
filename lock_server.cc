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
    printf("stat request from clt %d\n", clt);
    lock_protocol::status ret = lock_protocol::OK;
    if (lock_owner[lid] != clt)
    {
        r = -1;
        goto release;
    }
    else if (table_lk[lid] == FREE)
    {
        r = 0;
        goto release;
    }
    else
    {
        r = lock_owner[lid];
        goto release;
    }
release:
    return ret;
}


lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
    lock_protocol::status ret = lock_protocol::OK;
    fprintf(stdout,"acquire request from clt %d\n", clt);
    locker(lid, clt);
    //printf("!!!!!!!!!!!!!!!acquire success from clt %d lockid %d\n", clt, lid);
    r = ++nacquire;
    return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
    lock_protocol::status ret = lock_protocol::OK;
    printf("release request from clt %d\n", clt);
    pthread_mutex_lock(&mutex);
    if (!is_lock_exist(lid))
    {
        printf("release request from clt %d for lid %llu not exist pid %d\n", clt, lid, getpid());
        ret = lock_protocol::RPCERR;
    }
    else if (lock_owner[lid] != clt)
    {
        printf("release request from clt %d for lid %llu does not match its owner pid %d\n", clt, lid, getpid());
        ret = lock_protocol::RPCERR;
    }
    else
    {
        lock_owner[lid] = -1;
        table_lk[lid] = FREE;
        r = nacquire --;
        printf("release request from clt %d for lid %llu pid %d\n", clt, lid, getpid());
        VERIFY(pthread_cond_signal(&cv) == 0);
    }
    //printf("@@@@@@@@@@@@@@@release success from clt %d lockid %d\n", clt, lid);
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
    lock_owner[lid] = clt;
}

void
lock_server::lock_lock(lock_protocol::lockid_t lid,int clt)
{
    ScopedLock ml(&scopedmutex);
    printf("[lock_lock %11d] thread %d\n", lid, clt);
    table_lk[lid] = LOCKED;
    lock_owner[lid] = clt;
}
