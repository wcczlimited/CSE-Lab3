#include "inode_manager.h"
#include <cmath>
#include <iostream>
#include <cstdio>
// disk layer -----------------------------------------
disk::disk()
{
    bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
    if (id < 0 || id >= BLOCK_NUM || buf == NULL)
        return;

    memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
    if (id < 0 || id >= BLOCK_NUM || buf == NULL)
        return;

    memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
    /*
     * your lab1 code goes here.
     * note: you should mark the corresponding bit in block bitmap when alloc.
     * you need to think about which block you can start to be allocated.
     */
    for (int i = BLOCK_NUM - 1; i >= 0; i--)
    {
        if (!check_block(i))
        {
            set_block(i);
            return i;
        }
    }
    return 0;
}

bool
block_manager::check_block(uint32_t pos)
{
    std::map <uint32_t, int>::iterator it;
    it = using_blocks.find(pos);
    if(it!=using_blocks.end())
        return true;
    return false;
}

void
block_manager::set_block(uint32_t pos)
{
    using_blocks[pos]=1;
    return;
}

void
block_manager::free_block(uint32_t id)
{
    /*
     * your lab1 code goes here.
     * note: you should unmark the corresponding bit in the block bitmap when free.
     */
    std::map<uint32_t,int>::iterator it;;
    it=using_blocks.find(id);
    if(it!=using_blocks.end())
        using_blocks.erase(it);  //delete 112;
    return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
    d = new disk();

    // format the disk
    sb.size = BLOCK_SIZE * BLOCK_NUM;
    sb.nblocks = BLOCK_NUM;
    sb.ninodes = INODE_NUM;

    set_block(0);// for boot sector
    set_block(1);// for super block
    char * sb_char = new char[BLOCK_SIZE];
    memcpy(sb_char,&sb,sizeof(superblock));
    write_block(1,sb_char);
    delete sb_char;

    // for block bitmap;
    int bit_block = ceil((double)BLOCK_NUM/(double)BLOCK_SIZE);
    for(int i = 0; i < bit_block; i++)
    {
        set_block(2+i);
    }

    // for inode table
    int tinode_block = ceil((double)INODE_NUM/(double)sizeof(inode));
    for(int i = 0; i < tinode_block; i++)
    {
        set_block( 2 + bit_block -1 + i);
    }
    rest_block = BLOCK_NUM - tinode_block - bit_block - 2;
}

void
block_manager::read_block(uint32_t id, char *buf)
{
    d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
    d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
    bm = new block_manager();
    uint32_t root_dir = build_root(extent_protocol::T_DIR);

    if (root_dir != 1)
    {
        printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
        exit(0);
    }
}

void
inode_manager::setmtime(uint32_t inum)
{
    //std::cout << "[inode_manager settime] begin"<< std::endl;
    //fprintf(stdout, "[inode_manager settime]\n");
    timespec time;
    if(clock_gettime(CLOCK_REALTIME, &time)!=0)
    {
        return;
    }
    inode* node = get_inode(inum);
    node->mtime = time.tv_nsec;
    node->atime = time.tv_nsec;
    node->ctime = time.tv_nsec;
    put_inode(inum,node);
}

uint32_t
inode_manager::build_root(uint32_t type)
{
    timespec time;
    if (clock_gettime(CLOCK_REALTIME, &time)!=0)
        return 0;
    inode *node = new inode();
    int i = 1;
    do
    {
        inode* temp = get_inode(i);
        if( temp == NULL)
        {
            node->nblock = 0;
            node->size = 0;
            node->type = type;
            node->atime = time.tv_nsec;
            node->ctime = time.tv_nsec;
            node->mtime = time.tv_nsec;
            put_inode(i, node);
            delete node;
            return i;
        }
        i++;
    }
    while(i<INODE_NUM);
    delete node;
    return 0;
}

/* Create a new file.
 * Return itime inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
    /*
     * your lab1 code goes here.
     * note: the normal inode block should begin from the 2nd inode block.
     * the 1st is used for root_dir, see inode_manager::inode_manager().
     */
    //std::cout << "[inode_manager alloc_inode]"<< std::endl;
    //fprintf(stdout, "[inode_manager alloc_inode]\n");
    timespec time;
    if (clock_gettime(CLOCK_REALTIME, &time)!=0)
        return 0;
    inode *node = new inode();
    while(true)
    {
        int i =  (int)((double(rand())/RAND_MAX)*INODE_NUM)+1;
        if(i%INODE_NUM == 0)
            i = 2;
        inode* temp = get_inode(i);
        if( temp == NULL)
        {
            node->nblock = 0;
            node->size = 0;
            node->type = type;
            node->atime = time.tv_nsec;
            node->ctime = time.tv_nsec;
            node->mtime = time.tv_nsec;
            put_inode(i, node);
            delete node;
            return i;
        }
    }
    delete node;
    return 0;
}

void
inode_manager::free_inode(uint32_t inum)
{
    /*
     * your lab1 code goes here.
     * note: you need to check if the inode is already a freed one;
     * if not, clear it, and remember to write back to disk.
     */
    inode *node = get_inode(inum);
    if(node==NULL)
        return;
    if (node->nblock >= NDIRECT)
        free_inode(node->blocks[NDIRECT]);
    for (int i = 0; i < node->nblock && i < NDIRECT; i++)
        bm->free_block(node->blocks[i]);
    node->size = 0;
    memset(node->blocks,0,NDIRECT+1);
    timespec time;
    clock_gettime(CLOCK_REALTIME,&time);
    node->atime = time.tv_nsec;
    node->mtime = time.tv_nsec;
    put_inode(inum,node);
    return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode*
inode_manager::get_inode(uint32_t inum)
{
    struct inode *ino, *ino_disk;
    char buf[BLOCK_SIZE];

    printf("\tim: get_inode %d\n", inum);

    if (inum < 0 || inum >= INODE_NUM)
    {
        printf("\tim: inum out of range\n");
        return NULL;
    }

    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    // printf("%s:%d\n", __FILE__, __LINE__);

    ino_disk = (struct inode*)buf + inum%IPB;
    if (ino_disk->type == 0)
    {
        printf("\tim: inode not exist\n");
        return NULL;
    }

    ino = (struct inode*)malloc(sizeof(struct inode));
    *ino = *ino_disk;

    return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
    char buf[BLOCK_SIZE];
    struct inode *ino_disk;

    printf("\tim: put_inode %d\n", inum);
    if (ino == NULL)
        return;

    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    ino_disk = (struct inode*)buf + inum%IPB;
    *ino_disk = *ino;
    bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum.
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
    /*
     * your lab1 code goes here.
     * note: read blocks related to inode number inum,
     * and copy them to buf_Out
     */
    timespec time;
    if(clock_gettime(CLOCK_REALTIME, &time)!=0)
        return;
    inode *node = get_inode(inum);
    if(node==NULL)
        return;
    node->atime = time.tv_nsec;
    *size = node->size;
    put_inode(inum, node);//update the information of the inode access time
    int nblock = (int)ceil((double)*size/(double)BLOCK_SIZE);
    *buf_out = new char[nblock* BLOCK_SIZE];
    char *buf = *buf_out;

    for (int i = 0; i < MIN(node->nblock,NDIRECT); i++)
    {
        bm->read_block(node->blocks[i], buf);
        buf += BLOCK_SIZE;
    }
    if (node->nblock <= NDIRECT)
    {
        return;
    }
    int indir_blockid = node->blocks[NDIRECT];
    char inblock[BLOCK_SIZE];
    bm->read_block(indir_blockid,inblock);
    int* inblobk_int = (int *)inblock;
    int length = node->nblock - NDIRECT;
    for(int i=0; i<length; i++)
    {
        bm->read_block(inblobk_int[i],buf);
        buf += BLOCK_SIZE;
    }
    return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
    /*
     * your lab1 code goes here.
     * note: write buf to blocks of inode inum.
     * you need to consider the situation when the size of buf
     * is larger or smaller than the size of original inode
     */
    timespec time;
    if (clock_gettime(CLOCK_REALTIME, &time)!=0)
        return;
    inode *node = get_inode(inum);
    if(node==NULL)
        return;
    node->mtime = time.tv_nsec;//modify time
    int bn_real,bn_cur;
    if(node->nblock>0)
    {
        if(node->nblock > NDIRECT)
        {
            //free_inode(node->blocks[NDIRECT]);
            blockid_t inb = node->blocks[NDIRECT];
            char inblock[BLOCK_SIZE];
            bm->read_block(inb,inblock);
            int* inblobk_int = (int *)inblock;
            int length = node->nblock - NDIRECT;
            for(int i=0; i<length; i++)
            {
                bm->free_block(inblobk_int[i]);
            }
            bm->free_block(node->blocks[NDIRECT]);
        }

        for (int i = 0; i < node->nblock && i < NDIRECT; i++)
            bm->free_block(node->blocks[i]);
    }
    bn_real = (int)ceil((double)size/(double)BLOCK_SIZE);
    if (bm->rest() < bn_real)
    {
        return;
    }
    node->size = size;

    node->nblock = bn_real;
    bn_cur = MIN(bn_real, NDIRECT);
    for (int i = 0; i < bn_cur; i++)
    {
        node->blocks[i] = bm->alloc_block();
        bm->write_block(node->blocks[i], buf);
        bm->minus_block();
        buf += BLOCK_SIZE;
        size -= BLOCK_SIZE;
    }

    if (bn_real > bn_cur)
    {
        node->blocks[NDIRECT] = bm->alloc_block();
        bm->minus_block();
        char inblock[BLOCK_SIZE];
        bm->read_block(node->blocks[NDIRECT],inblock);
        int* inblobk_int = (int *)inblock;
        for(int i=0 ; i<bn_real-NDIRECT; i++)
        {
            inblobk_int[i] = bm->alloc_block();
            bm->write_block(inblobk_int[i], buf);
            bm->minus_block();
            buf += BLOCK_SIZE;
            size -= BLOCK_SIZE;
        }
        bm->write_block(node->blocks[NDIRECT],inblock);
    }
    node->mtime = time.tv_nsec;
    node->atime = time.tv_nsec;
    node->ctime = time.tv_nsec;
    put_inode(inum, node);
    return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
    /*
     * your lab1 code goes here.
     * note: get the attributes of inode inum.
     * you can refer to "struct attr" in extent_protocol.h
     */
    timespec time;
    if(clock_gettime(CLOCK_REALTIME, &time)!=0)
        return;
    inode *node = get_inode(inum);
    if(node==NULL)
        return;
    node->atime = time.tv_nsec;

    setattr(a,node);
    put_inode(inum, node);//update the information of the inode access time
    return;
}

void
inode_manager::remove_file(uint32_t inum)
{
    /*
     * your lab1 code goes here
     * note: you need to consider about both the data block and inode of the file
     */
    inode* node = get_inode(inum);
    if(node==NULL)
        return;

    if (node->nblock >= NDIRECT)
        remove_file(node->blocks[NDIRECT]);
    for (int i = 0; i < node->nblock && i < NDIRECT; i++)
        bm->free_block(node->blocks[i]);
    //put a new inode to the original place
    delete node;
    node = new inode();
    put_inode(inum, node);
    //return;
}
