// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include <map>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"

class lock_server
{

protected:
    int nacquire;
    std::map<lock_protocol::lockid_t, int > table_lk;
    std::map<lock_protocol::lockid_t, int > lock_owner;
    bool is_lock_exist(lock_protocol::lockid_t lid);
    bool is_lock_locked(lock_protocol::lockid_t lid);
    void new_lock(lock_protocol::lockid_t, int);
    void lock_lock(lock_protocol::lockid_t, int);
    void locker(lock_protocol::lockid_t, int);
    pthread_mutex_t mutex;
    pthread_mutex_t scopedmutex;
    pthread_cond_t cv;

public:
    lock_server();
    ~lock_server();
    //clt : the thread_id of lock_client
//lid : the id of lock requested
//int &: the return value type
    lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
    lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
    lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif







