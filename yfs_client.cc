// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client(extent_dst);
    lc = new lock_client(lock_dst);
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}


yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE)
    {
        printf("isfile: %lld is a file\n", inum);
        return true;
    }
    printf("isfile: %lld is a dir\n", inum);
    return false;
}

bool
yfs_client::isdir(inum inum)
{
    return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;
    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;
    lc->acquire(inum);
    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    lc->release(inum);
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    lc->acquire(ino);
    printf("[yfs_client setattr] size %d\n", size);
    std::string buf;
    fileinfo fin;
    if(getfile(ino, fin) != OK)
    {
        r =  IOERR;
        goto release;
    }
    if (yfs_read(ino,fin.size,0,buf) != OK)
    {
        r =  IOERR;
        goto release;
    }
    if(fin.size > size)
    {
        buf = buf.substr(0,size);
        if(ec->put(ino,buf) != extent_protocol::OK)
        {
            r = IOERR;
            goto release;
        }
    }
    else if(fin.size < size)
    {
        buf.append(size-fin.size,'\0');
        if(ec->put(ino,buf) != extent_protocol::OK)
        {
            r = IOERR;
            goto release;
        }
    }
release:
    lc->release(ino);
    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out , int type)
{
    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    int r = OK;
    bool found = false;
    lc->acquire(parent);
    std::string buf,str(name), sinum;
    fileinfo fin;
    if(!isdir(parent))
    {
        r = NOENT;
        goto release;
    }
    if(yfs_lookup(parent, name, found, ino_out) != OK)
    {
        r = IOERR;
        goto release;
    }
    if(found == true)
    {
        r = EXIST;
        goto release;
    }
    if(getfile(parent,fin) != OK)
    {
        r = IOERR;
        goto release;
    }
    if(yfs_read(parent,fin.size,0,buf) != OK)
    {
        r = IOERR;
        goto release;
    }
    if(ec->create(type, ino_out) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }
    sinum = filename(ino_out);
    if(fin.size!=0)
        buf.append(" ");
    buf.append(str+" "+sinum);
    std::cout << "[yfs_client create] "+str+" "+sinum << std::endl;
    if(ec->put(parent,buf) != extent_protocol::OK)
    {
        std::cout << "create IOERR" << std::endl;
        r = IOERR;
        goto release;
    }
    //ec->setmtime(parent);
release:
    std::cout << "[yfs_client create] create complete"<< std::endl;
    lc->release(parent);
    return r;
}


int
yfs_client::yfs_lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    std::list<dirent> list;
    std::string namestr;
    if(!isdir(parent))
    {
        r = NOENT;
        goto release;
    }
    if(yfs_readdir(parent,list) != OK)
    {
        r = IOERR;
        goto release;
    }

    namestr.assign(name);
    for(std::list<dirent>::iterator it = list.begin(); it!=list.end(); it++)
    {
        if(namestr.compare((*it).name)==0)
        {
            found = true;
            ino_out = (*it).inum;
            goto release;
        }
    }
release:
    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    /*
     * your lab2 code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    int r = OK;
    printf("[start lookup]\n");
    lc->acquire(parent);
    r = yfs_lookup(parent,name,found,ino_out);
    lc->release(parent);
    /*std::list<dirent> list;
    std::string namestr;
    if(!isdir(parent))
    {
        r = NOENT;
        goto release;
    }
    if(readdir(parent,list) != OK)
    {
        r = IOERR;
        goto release;
    }

    namestr.assign(name);
    for(std::list<dirent>::iterator it = list.begin(); it!=list.end(); it++)
    {
        if(namestr.compare((*it).name)==0)
        {
            found = true;
            ino_out = (*it).inum;
            goto release;
        }
    }
release:
    lc->release(parent);
    printf("[finish lookup]\n");*/
    return r;
}

int
yfs_client::yfs_readdir(inum dir, std::list<dirent> &list)
{
    /*
     * your lab2 code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    int r = OK;
    fileinfo fin;
    std::string out;
    std::stringstream stream;
    if(!isdir(dir))
    {
        r = NOENT;
        goto release;
    }
    if(getfile(dir,fin) != OK)
    {
        r = IOERR;
        goto release;
    }
    if(yfs_read(dir,fin.size,0,out) != OK)
    {
        r = IOERR;
        goto release;
    }
    stream << out;
    while(stream)
    {
        dirent temp;
        stream >> temp.name >> temp.inum;
        std::cout << "readdir " <<temp.name << " " << temp.inum << std::endl;
        list.push_back(temp);
    }
release:
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    /*
     * your lab2 code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
     lc->acquire(dir);
    int r = OK;
    r = yfs_readdir(dir,list);
    lc->release(dir);
    return r;
}

int
yfs_client::yfs_read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;
    if(ec->get(ino, data) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }
    data = data.substr(off,size);
release:
    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;
    lc->acquire(ino);
    /*
     * your lab2 code goes here.
     * note: read using ec->get().
     */
    r = yfs_read(ino,size,off,data);
    lc->release(ino);
    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
                  size_t &bytes_written)
{
    int r = OK;
    lc->acquire(ino);
    /*
     * your lab2 code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    fileinfo fin;
    std::string buf, new_data(data,size);
    //new_data = new_data.substr(0,size);

    if(getfile(ino,fin)!= OK)
    {
        r = IOERR;
        goto release;
    }
    if(yfs_read(ino,fin.size,0,buf) != OK)
    {
        r = IOERR;
        goto release;
    }

    if(off > buf.size())
    {
        std::string new_buf="";
        new_buf.append(buf);
        new_buf.append(off-buf.size(),'\0');
        new_buf.append(new_data,0,size);
        buf.assign(new_buf);
    }
    else if(off + size > buf.size())
    {
        //buf.resize(off+size);
        buf.erase(off,buf.size()-off);
        //buf.replace(off,size,new_data);
        for(int i=0; i<int(size); i++)
            buf.push_back(*(data)++);
    }
    else
    {
        if(off == 0)
        {
            buf.resize(size);
        }
        buf.replace(off,size,new_data);
    }

    bytes_written = size;
    if(ec->put(ino,buf) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }
release:
    lc->release(ino);
    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;
    lc->acquire(parent);
    /*
     * your lab2 code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    if(!isdir(parent))
        return IOERR;
    bool found = false;
    inum ino;
    if(yfs_lookup(parent, name, found, ino) != OK)
    {
        lc->release(parent);
        return IOERR;
    }
    if(!found)
    {
        lc->release(parent);
        return NOENT;
    }
    if(isdir(ino))
    {
        lc->release(parent);
        return IOERR;
    }
    lc->acquire(ino);
    extent_protocol::status res = ec->remove(ino);
    if(res != extent_protocol::OK)
    {
        lc->release(ino);
        lc->release(parent);
        return IOERR;
    }
    std::string buf;
    fileinfo fin;
    if(getfile(parent,fin)!=OK)
    {
        lc->release(ino);
        lc->release(parent);
        return IOERR;
    }
    if(yfs_read(parent,fin.size, 0, buf) != OK)
    {
        lc->release(ino);
        lc->release(parent);
        return IOERR;
    }
    unsigned long int pos = buf.find(name);
    int size = std::string(name).size() + filename(ino).size() + 2; // name + \0 + id + \0
    buf.erase(pos, size);

    size_t sz;
    if(ec->put(parent,buf) != extent_protocol::OK)
    {
        lc->release(ino);
        lc->release(parent);
        return IOERR;
    }
    //ec->setmtime(parent);

    lc->release(ino);
    lc->release(parent);
    return r;
}
