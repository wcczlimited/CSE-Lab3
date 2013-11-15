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
    int h = lc->stat(inum);
    if(h<=0)
        lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        printf("error getting attr\n");
        if(h<=0)
            lc->release(inum);
        return false;
    }

    if (a.type == extent_protocol::T_FILE)
    {
        printf("isfile: %lld is a file\n", inum);
        if(h<=0)
            lc->release(inum);
        return true;
    }
    printf("isfile: %lld is a dir\n", inum);
    if(h<=0)
        lc->release(inum);
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
    int h = lc->stat(inum);
    if(h<=0)
        lc->acquire(inum);
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
    if(h<=0)
        lc->release(inum);
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;
    int h = lc->stat(inum);
    if(h<=0)
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
    if(h<=0)
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
    int h = lc->stat(ino);
    if(h<=0)
        lc->acquire(ino);
    //printf("[yfs_client setattr] size %d\n", size);
    std::string buf;
    fileinfo fin;
    if(getfile(ino, fin) != OK)
    {
        r =  IOERR;
        goto release;
    }
    if ( read(ino,fin.size,0,buf) != OK)
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
    if(h<=0)
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
    size_t s,sz;
    int h = lc->stat(parent);
    lc->acquire(parent);
    std::string buf,str(name), sinum;
    fileinfo fin;
    if(!isdir(parent))
    {
        r = NOENT;
        goto release;
    }
    if(lookup(parent, name, found, ino_out) != OK)
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
    if(read(parent,fin.size,0,buf) != OK)
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
    sz = buf.size();
    if(write(parent, sz ,0,buf.c_str(),s)!= extent_protocol::OK)//ec->put(parent,buf) != extent_protocol::OK)
    {
        //std::cout << "create IOERR" << std::endl;
        r = IOERR;
        goto release;
    }
release:
    //std::cout << "[yfs_client create] create complete"<< std::endl;
    lc->release(parent);
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
    int h = lc->stat(parent);
    if(h<=0)
    {
        lc->acquire(parent);
    }
    std::list<dirent> list;
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
    if(h<=0)
        lc->release(parent);
    printf("[finish lookup]\n");
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
    printf("[yfs_client readdir]\n");
    int h = lc->stat(dir);
    if(h<=0)
        lc->acquire(dir);
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
    if(read(dir,fin.size,0,out) != OK)
    {
        r = IOERR;
        goto release;
    }
    stream << out;
    while(stream)
    {
        dirent temp;
        stream >> temp.name >> temp.inum;
        //std::cout << "readdir " <<temp.name << " " << temp.inum << std::endl;
        list.push_back(temp);
    }
release:
    if(h<=0)
        lc->release(dir);
    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    //printf("[yfs_client read]\n");
    int r = OK;
    int h = lc->stat(ino);
    std::string p_extent;
    if(h<=0)
        lc->acquire(ino);
    /*
     * your lab2 code goes here.
     * note: read using ec->get().
     */

    //printf("[yfs_client read] get\n");
    if(ec->get(ino, p_extent) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }
    //printf("[yfs_client read] get finish off %d size %d data_size %d\n", off ,size, p_extent.size());
    if(off >= p_extent.size())
    {
        data.clear();
        if(h<=0)
            lc->release(ino);
        return r;
    }

    data = p_extent.substr(off,size);
release:
    if(h<=0)
        lc->release(ino);
    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
                  size_t &bytes_written)
{
    //printf("[yfs_client write]\n");
    int r = OK;
    int h = lc->stat(ino);
    if(h<=0)
        lc->acquire(ino);
    /*
     * your lab2 code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    std::string buf, new_data(data,size);
    if(ec->get(ino,buf)!= OK)//read(ino,a.size,0,buf) != OK)
    {
        r = IOERR;
        goto release;
    }
    //printf("[yfs_client write] bufsize %d\n",buf.size());
    if(off > buf.size())
    {
        int start = buf.size();
        buf = std::string(buf.c_str(),off+size);
        for(int i=start; i<off; i++)
            buf[i]='\0';
        buf.replace(off,size,new_data);
    }
    else if(off + size > buf.size())
    {
        buf.erase(off,buf.size()-off);
        for(int i=0; i<int(size); i++)
            buf.push_back(new_data[i]);
    }
    else
    {
        for(int i=off;i<off+(int)size;i++)
            buf[i] = new_data[i-off];
    }

    bytes_written = size;
    //printf("[yfs_client write] put bufsize %d\n",buf.size());
    if(ec->put(ino,buf) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }
    //printf("[yfs_client write] put finish\n");
release:
    if(h<=0)
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
    //printf("[yfs_client unlink] lookup\n");
    if(lookup(parent, name, found, ino) != OK)
    {
        lc->release(parent);
        return IOERR;
    }
    if(!found)
    {
        lc->release(parent);
        return NOENT;
    }
    //printf("[yfs_client unlink] isdir\n");
    if(isdir(ino))
    {
        lc->release(parent);
        return IOERR;
    }
    std::string buf;

    //printf("[yfs_client unlink] get\n");
    if(ec->get(parent,buf)!=OK)//read(parent,fin.size, 0, buf) != OK)
    {
        lc->release(parent);
        return IOERR;
    }
    //printf("[yfs_client unlink] acquire\n");
    int h = lc->stat(ino);
    if(h<=0)
        lc->acquire(ino);
    //printf("[yfs_client unlink] remove\n");
    extent_protocol::status res = ec->remove(ino);
    //printf("[yfs_client unlink] remove finish\n");
    if(res != extent_protocol::OK)
    {
        lc->release(parent);
        if(h<=0)
            lc->release(ino);
        return IOERR;
    }
    unsigned long int pos = buf.find(name);
    int size = std::string(name).size() + filename(ino).size() + 2; // name + \0 + id + \0
    buf.erase(pos, size);

    //printf("[yfs_client unlink] write\n");
    size_t sz = buf.size(), s;
    if(ec->put(parent,buf) != extent_protocol::OK)//write(parent, sz ,0,buf.c_str(),s)!= extent_protocol::OK)//
    {
        if(h<=0)
            lc->release(ino);
        lc->release(parent);
        return IOERR;
    }
    //printf("[yfs_client unlink] write finsh\n");
    if(h<=0)
        lc->release(ino);
    lc->release(parent);
    return r;
}
