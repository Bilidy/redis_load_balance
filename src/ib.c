#include "redis.h"
#include "cluster.h"
#include "bio.h"
//#include "tract.h"

#include <signal.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>

#define valuate(x,y) (x)*(y) - 0.3*( (x) + (y) )*( (x) + (y) )
#define THRESHOLD_T1 0.05
#define THRESHOLD_T2 0.3
#define IB_RW_BUF_BLOCK_SIZE (1024*1024*10)    /* 10 MB per block */
#define BUF_SIZE 1024


typedef struct ibrwblock {
    
    // 缓存块已使用字节数和可用字节数
    unsigned long used, free;

    // 缓存块
    char buf[IB_RW_BUF_BLOCK_SIZE];

} ibrwblock;
/* This function free the old AOF rewrite buffer if needed, and initialize
 * a fresh new one. It tests for server.aof_rewrite_buf_blocks equal to NULL
 * so can be used for the first initialization as well. 
 *
 * 释放旧的 AOF 重写缓存，并初始化一个新的 AOF 缓存。
 *
 * 这个函数也可以单纯地用于 AOF 重写缓存的初始化。
 */
void ibRewriteBufferReset(void) {

    // 释放旧有的缓存（链表）
    if (server.intelbalance_rewrite_buf_blocks)
        listRelease(server.intelbalance_rewrite_buf_blocks);

    // 初始化新的缓存（链表）
    server.intelbalance_rewrite_buf_blocks = listCreate();
    listSetFreeMethod(server.intelbalance_rewrite_buf_blocks,zfree);
}
/* Return the current size of the IB rerwite buffer. 
 *
 * 返回 IB 重写缓存当前的大小
 */
unsigned long ibRewriteBufferSize(void) {

    // 取出链表中最后的缓存块
    listNode *ln = listLast(server.intelbalance_rewrite_buf_blocks);
    ibrwblock *block = ln ? ln->value : NULL;

    // 没有缓存被使用
    if (block == NULL) return 0;

    // 总缓存大小 = （缓存块数量-1） * IB_RW_BUF_BLOCK_SIZE + 最后一个缓存块的大小
    unsigned long size =
        (listLength(server.intelbalance_rewrite_buf_blocks)-1) * IB_RW_BUF_BLOCK_SIZE;
    size += block->used;

    return size;
}
/* Append data to the IB rewrite buffer, allocating new blocks if needed. 
 *
 * 将字符数组 s 追加到 IB 缓存的末尾，
 * 如果有需要的话，分配一个新的缓存块。
 */
void ibRewriteBufferAppend(unsigned char *s, unsigned long len) {

    // 指向最后一个缓存块
    listNode *ln = listLast(server.intelbalance_rewrite_buf_blocks);
    ibrwblock *block = ln ? ln->value : NULL;

    while(len) {
        /* If we already got at least an allocated block, try appending
         * at least some piece into it. 
         *
         * 如果已经有至少一个缓存块，那么尝试将内容追加到这个缓存块里面
         */
        if (block) {
            unsigned long thislen = (block->free < len) ? block->free : len;
            if (thislen) {  /* The current block is not already full. */
                memcpy(block->buf+block->used, s, thislen);
                block->used += thislen;
                block->free -= thislen;
                s += thislen;
                len -= thislen;
            }
        }

        // 如果 block != NULL ，那么这里是创建另一个缓存块买容纳 block 装不下的内容
        // 如果 block == NULL ，那么这里是创建缓存链表的第一个缓存块
        if (len) { /* First block to allocate, or need another block. */
            int numblocks;

            // 分配缓存块
            block = zmalloc(sizeof(*block));
            block->free = IB_RW_BUF_BLOCK_SIZE;
            block->used = 0;

            // 链接到链表末尾
            listAddNodeTail(server.intelbalance_rewrite_buf_blocks,block);

            /* Log every time we cross more 10 or 100 blocks, respectively
             * as a notice or warning. 
             *
             * 每次创建 10 个缓存块就打印一个日志，用作标记或者提醒
             */
            numblocks = listLength(server.intelbalance_rewrite_buf_blocks);
            if (((numblocks+1) % 10) == 0) {
                int level = ((numblocks+1) % 100) == 0 ? REDIS_WARNING :
                                                         REDIS_NOTICE;
                redisLog(level,"Background IB buffer size: %lu MB",
                    ibRewriteBufferSize()/(1024*1024));
            }
        }
    }
}



/* Write the buffer (possibly composed of multiple blocks) into the specified
 * fd. If a short write or any other error happens -1 is returned,
 * otherwise the number of bytes written is returned. 
 *
 * 将重写缓存中的所有内容（可能由多个块组成）写入到给定 fd 中。
 *
 * 如果没有 short write 或者其他错误发生，那么返回写入的字节数量，
 * 否则，返回 -1 。
 */
ssize_t ibRewriteBufferWrite(int fd) {
    listNode *ln;
    listIter li;
    ssize_t count = 0;

    // 遍历所有缓存块
    listRewind(server.intelbalance_rewrite_buf_blocks,&li);
    while((ln = listNext(&li))) {
        ibrwblock *block = listNodeValue(ln);
        ssize_t nwritten;

        if (block->used) {

            // 写入缓存块内容到 fd
            nwritten = write(fd,block->buf,block->used);
            if (nwritten != block->used) {
                if (nwritten == 0) errno = EIO;
                return -1;
            }

            // 积累写入字节
            count += nwritten;
        }
    }

    return count;
}

void ib_background_proc(int fd) {
    bioCreateBackgroundJob(REDIS_BIO_IB_PROC,(void*)(long)fd,NULL,NULL);
}


int ibProc(struct aeEventLoop *eventLoop, long long id, void *clientData){

    REDIS_NOTUSED(eventLoop);
    REDIS_NOTUSED(id);
    REDIS_NOTUSED(clientData);
    //如果还没有过期将过期事务写入缓存并且滑入dat文件

    if(server.intelbalance_fd == -1){
        server.intelbalance_fd = open(server.intelbalance_filename,
                                        O_WRONLY|O_APPEND|O_CREAT,0644);
        if (server.intelbalance_fd == -1) {
            redisLog(REDIS_WARNING, "Can't open IB file: %s",
                strerror(errno));
            exit(1);
        }
    }
    if(server.intelbalance_state == REDIS_IB_PRCING){
        server.intelbalance_state = REDIS_IB_OFF;
        return REDIS_IB_PERIOD_SEGMENT;
    }
    if(server.intelbalance_state == REDIS_IB_FIN_OK){
        /*******/
        /*根据set进行迁移
        /*******/
        server.intelbalance_state = REDIS_IB_OFF;
        return AE_NOMORE;
    }
    if(server.intelbalance_state == REDIS_IB_FIN_ERR){
        //addReplyError(c,"ibBackgroundProc() error");
        server.intelbalance_state = REDIS_IB_OFF;
        goto cleanup;
        
    }
    time_t aurrenttime= mstime();
    if(server.intelbalance_last_time != 0){
        //继续刷入过期事务
        if((server.intelbalance_last_time + server.intelbalance_period) > aurrenttime){
            sds buf = sdsempty();
            dictEntry *de;

            dictIterator *di = dictGetSafeIterator(server.db[0].timestamps);
            //de = dictNext(di);
            while((de = dictNext(di))!= NULL){
                sds keystr,valstr;
                
                time_t timestamp;

                // 取出键
                keystr = sdsdup(dictGetKey(de));

                // 取出值
                valstr = (sds)dictGetVal(de);
                getLongLongFromObject((sds)valstr,&timestamp);
                //过期了
                if(timestamp <= aurrenttime - 5000){
                    server.intelbalance_transnum++;
                    dict *item_dict;
                    dictEntry *de_trans;
                    dictIterator *di_trans = NULL;


                    dictEntry *dtrans=dictFind(server.db[0].trans,keystr);
                    item_dict = (dict *)dtrans->v.val;
                    
                    sds transnumsds = sdsfromlonglong(server.intelbalance_transnum);
                    long counter = 0;
                    sds transsds = sdsempty();;

                    //sdscatsds(transnumsds,)
                    di_trans = dictGetSafeIterator(item_dict);
                    while((de_trans = dictNext(di_trans)) != NULL){
                        counter++;
                        sds keystr_trans;
                        keystr_trans = sdsdup(dictGetKey(de_trans));
                        transsds = sdscatsds(transsds,keystr_trans);
                        if(counter < item_dict->ht[0].used)
                            transsds = sdscat(transsds,"\t");
                    }
                    if (counter!=0)
                    {
                        transnumsds = sdscat(transnumsds,"\t");
                        transnumsds = sdscatsds(transnumsds,sdsfromlonglong(counter));
                        transnumsds = sdscat(transnumsds,"\t");
                        transnumsds = sdscatsds(transnumsds,transsds);
                        transnumsds = sdscat(transnumsds,"\n");
                        buf = sdscatsds(buf,transnumsds);
                    }
                    dictRelease(dtrans->v.val);
                    dtrans->v.val = createStringObject(" ",1);
                    dictReleaseIterator(di_trans);
                    
                    sdsfree(transnumsds);
                    sdsfree(transsds);
                    
                    dictDelete(server.db[0].timestamps,keystr);
                    dictDelete(server.db[0].trans,keystr);

                    di = dictGetSafeIterator(server.db[0].timestamps);
                }
                
            }
            
            dictReleaseIterator(di);
            //将缓冲区中的数据写入
            printf("length of buf %d\n",sdslen(buf));
            ibRewriteBufferAppend((unsigned char*)buf,sdslen(buf));
            sdsfree(buf);
            
        }else{//该写入，并且分析dat了
            
            server.intelbalance_state = REDIS_IB_PRCING;
            ib_background_proc(server.intelbalance_fd);

            return REDIS_IB_PERIOD_SEGMENT;
        }
    }
    return REDIS_IB_PERIOD_SEGMENT;
cleanup:
    ibRewriteBufferReset();
    if(server.intelbalance_fd != -1){
        close(server.intelbalance_fd);
        server.intelbalance_fd = -1;
    }
    server.intelbalance_state = REDIS_IB_OFF;
    aeDeleteTimeEvent(server.el,server.intelbalance_time_event_id);
    server.intelbalance_time_event_id = -1;
    return AE_NOMORE;
}
long long getDistFromMetadata(robj* _key){
    sds key = sdsnew(_key->ptr);
    long double ret = -1;
    if(server.db[0].keymetadata == NULL){
        return -1;
    }
    sds keyforfd = sdsdup(key);
    dictEntry * entryforfd;
    if((entryforfd=dictFind(server.db[0].keymetadata,keyforfd))!=NULL){
        dict* dt =(dict*)entryforfd->v.val;
        sds keyfordist = sdsnew("dist");
        dictEntry * entryfordist;
        if((entryfordist=dictFind(dt,keyfordist))!= NULL){
            getLongDoubleFromObject(entryfordist->v.val,&ret);
        }
        sdsfree(keyfordist);
        sdsfree(keyforfd);
    }else
    {
        sdsfree(keyforfd);
        return -1;
    }
    return ret;
}
long long getFreqFromMetadata(robj* _key){
    sds key = sdsnew(_key->ptr);
    long long ret = -1;
    if(server.db[0].keymetadata == NULL){
        return -1;
    }
    sds keyforfd = sdsdup(key);
    dictEntry * entryforfd;
    if((entryforfd=dictFind(server.db[0].keymetadata,keyforfd))!=NULL){
        dict* dt =(dict*)entryforfd->v.val;
        sds keyfordist = sdsnew("freq");
        dictEntry * entryfordist;
        if((entryfordist=dictFind(dt,keyfordist))!= NULL){
            getLongLongFromObject(entryfordist->v.val,&ret);
        }
        sdsfree(keyfordist);
        sdsfree(keyforfd);
    }else
    {
        sdsfree(keyforfd);
        return -1;
    }
    return ret;
}
robj* valuatePatterns(robj* set){
    
    set = createSetObject();

    struct FILE* in =NULL;
    
    
    char* buf =NULL ;//= (char* )malloc(sizeof(char) * BUF_SIZE);
    robj** sdsbuf = (robj **)malloc(sizeof(robj*) * BUF_SIZE);
    long long* distbuf = (long long* )malloc(sizeof(long long) * BUF_SIZE);
    long long* freqbuf = (long long* )malloc(sizeof(long long) * BUF_SIZE);
    int  counter = 0;
	//char * line = NULL;
	size_t len = 0;
	ssize_t read;
	in = fopen(REDIS_DEFAULT_IB_FIM_FILENAME,"rt");

	while ((read = getline(&buf, &len, in)) != -1){
		int idx = 0;
        char* head = buf;
        double distsum = 0;
        double freqsum = 0;
        counter = 0;
        buf[strlen(buf)-2]=0;
        int slen = strlen(buf);
        for(idx = 0; idx <= slen;idx++){
            int a= idx;
            if(buf[idx]==' '||buf[idx]==0){
                buf[idx] = 0;

                sdsbuf[counter] = createStringObject(head,strlen(head));
                long double dist = getDistFromMetadata(sdsbuf[counter]);
                long double freq = getFreqFromMetadata(sdsbuf[counter]);
                if(dist==-1||freq==-1)
                    goto byebye;
                    
                else{
                    distbuf[counter]= dist;
                    freqbuf[counter] = freq;
                }
                counter++;

                head = &buf[idx]+1;
                //sdsfree(key)
            }
        }
        for(int i = 0; i<counter; i++){
            distsum += distbuf[i];
            freqsum += freqbuf[i];
        }
        double distavg = (double)distsum / counter;
        double freqavg = (double)freqsum / counter;

        double distvarsum = 0.0;
        double freqvarsum = 0.0;
        for(int i = 0; i<counter; i++){

            distvarsum += (distbuf[i] - distavg)*(distbuf[i] - distavg);
            freqvarsum += (distbuf[i] - freqavg)*(distbuf[i] - freqavg);
        
        }
        double distvar = sqrt(distvarsum / counter);
        double freqvar = sqrt(freqvarsum / counter);
        
        double Cs = distvar/(distavg==0?0.01:distavg);
        double Ct = freqvar/(freqavg==0?0.01:freqavg);

        double val =valuate(Cs,Ct);
        if(val <= THRESHOLD_T1){
            for(int i = 0; i<counter;i++) {
               //ziplist->ptr = ziplistPush(ziplist->ptr,sdsbuf[i]->ptr,strlen(sdsbuf[i]->ptr),REDIS_TAIL);
               setTypeAdd(set,sdsbuf[i]);
            }
        }
        for(int i = 0; i<counter;i++) {
            freeStringObject(sdsbuf[i]);
        }
	}
    if(buf)
        free(buf);
    goto out;
byebye:
    for(int i = 0; i<counter;i++) {
            freeStringObject(sdsbuf[i]);
    }
    freeSetObject(set);
    set = NULL;
    return NULL;
out:
    free(freqbuf);
    free(distbuf);
    free(buf);

    fclose(in);

    return set;
}

static robj** createMigrateArgvForSlot(int argc,robj* slot,robj* target){
    
    robj** argv = zmalloc(sizeof(robj*)*argc);
    if(argc==5){
        argv[0] = createStringObject("CLUSTER",strlen("CLUSTER"));
        argv[1] = createStringObject("SETSLOT",strlen("SETSLOT"));
        argv[2] = slot;
        argv[3] = createStringObject("SETSLOT",strlen("SETSLOT"));
        argv[4] = target;
    }else{
        zfree(argv);
        argv=NULL;
    }
    return argv;
}


static robj** createMigrateArgvForKey(int argc,robj* host, robj* port,robj* Key,
                                        robj* dbid,robj* timeout,robj* mode){
    
    robj** argv = zmalloc(sizeof(robj*)*argc);
    if(argc>=6){
        argv[0] = createStringObject("MIGRATE",strlen("MIGRATE"));
        argv[1] = host;
        argv[2] = port;
        argv[3] = Key;
        argv[4] = dbid;
        argv[5] = timeout;
        if(argc > 6)
            argv[6] = mode;
    }else{
        zfree(argv);
        argv=NULL;
    }
    return argv;
}

struct redisClient *createFakeClientForIb(void) {
    struct redisClient *c = zmalloc(sizeof(*c));

    selectDb(c,0);

    c->fd = -1;
    c->name = NULL;
    c->querybuf = sdsempty();
    c->querybuf_peak = 0;
    c->argc = 0;
    c->argv = NULL;
    c->bufpos = 0;
    c->flags = 0;
    c->btype = REDIS_BLOCKED_NONE;
    /* We set the fake client as a slave waiting for the synchronization
     * so that Redis will not try to send replies to this client. 
     *
     * 将客户端设置为正在等待同步的附属节点，这样客户端就不会发送回复了。
     */
    c->replstate = REDIS_REPL_WAIT_BGSAVE_START;
    c->reply = listCreate();
    c->reply_bytes = 0;
    c->obuf_soft_limit_reached_time = 0;
    c->watched_keys = listCreate();
    c->peerid = NULL;
    listSetFreeMethod(c->reply,decrRefCountVoid);
    listSetDupMethod(c->reply,dupClientReplyValue);
    initClientMultiState(c);

    return c;
}

int intelMigrate(char mode,robj* set){
    if(set ==NULL)
        return -1;

    //是键迁移
    if(mode=='k'){
        robj* setitem;
        setTypeIterator* sit = setTypeInitIterator(set);
        while((setitem=setTypeNextObject(sit))!=NULL){
            
            //KEY是在本Node处理
            robj** argv = NULL;
            int argc = 0;
            long long  slot = keyHashSlot(setitem->ptr,strlen(setitem->ptr));
            if(server.cluster->slots[slot] == server.cluster->myself){
                struct redisClient *fakeClient = createFakeClientForIb();

                dictEntry *de;
            retry:
                de = dictGetRandomKey(server.cluster->nodes);
                clusterNode * randmnode = dictGetVal(de);
                if((randmnode->ip)[0] == 0)
                    goto retry;
                robj * host = createStringObject(randmnode->ip,strlen(randmnode->ip));
                sds tempport = sdsfromlonglong(randmnode->port);
                robj * port = createStringObject(tempport,sdslen(tempport));
                robj * key = dupStringObject(setitem);
                robj * timeout = createStringObjectFromLongLong(1000);
                robj * dbid = createStringObjectFromLongLong(0);
                
                
                argv = createMigrateArgvForKey(6,host,port,key,dbid,timeout,NULL);
                if(argv==NULL){
                    freeStringObject(host);
                    freeStringObject(port);
                    freeStringObject(key);
                    freeStringObject(dbid);
                    freeStringObject(timeout);
                    return -1;
                }
                    
                
                struct redisCommand *cmd = lookupCommand(argv[0]->ptr);
                if (!cmd) {
                    redisLog(REDIS_WARNING,"Unknown command '%s' reading the append only file", (char*)argv[0]->ptr);
                    exit(1);
                }
                
                printf("fakeClient->argc = %d\n",fakeClient->argc);
                fakeClient->argc = 6;
                fakeClient->argv = argv;
                cmd->proc(fakeClient);

                for (int j = 0; j < fakeClient->argc; j++)
                    decrRefCount(fakeClient->argv[j]);
                zfree(fakeClient->argv);

            }else
                return -1;
        }
        setTypeReleaseIterator(sit);
    }else if(mode == 's'){//是槽迁移
        robj* setitem;
        setTypeIterator* sit = setTypeInitIterator(set);
        while((setitem=setTypeNextObject(sit))!=NULL){
            //slot是在本Node处理

            

            robj** argv = NULL;
            int argc = 0;
            long long  slot = keyHashSlot(setitem->ptr,strlen(setitem->ptr));
            if(server.cluster->slots[slot] == server.cluster->myself){
                struct redisClient *fakeClient = createFakeClientForIb();

                dictEntry *de;
                de = dictGetRandomKey(server.cluster->nodes);
                sds nodenamesds =dictGetKey(de);
                robj* nodeobj = createStringObject(nodenamesds,sdslen(nodenamesds));
                robj* key = createStringObjectFromLongLong(slot);

                argv = createMigrateArgvForSlot(5,key,nodeobj);

                if(argv==NULL){
                    freeStringObject(nodeobj);
                    freeStringObject(key);
                    return -1;
                }

                struct redisCommand *cmd = lookupCommand(argv[0]->ptr);
                if (!cmd) {
                    redisLog(REDIS_WARNING,"Unknown command '%s' reading the append only file", (char*)argv[0]->ptr);
                    exit(1);
                }

                fakeClient->argc = 5;
                fakeClient->argv = argv;
                cmd->proc(fakeClient);

                for (int j = 0; j < fakeClient->argc; j++)
                    decrRefCount(fakeClient->argv[j]);
                zfree(fakeClient->argv);

            }else{
                return -1;
            }
        }
        setTypeReleaseIterator(sit);
    }else{
        return -1;
    }
    return 0;
}
void ibBackgroundProc(int _fd){

    if(_fd != -1){
        ibRewriteBufferWrite(server.intelbalance_fd);
        close(server.intelbalance_fd);
        server.intelbalance_fd = -1;
    }else{
        //addReplyError(c,"IB fd is closed by accident, exec has been terminated.");
    }
    // 清空 IB 缓冲区
    ibRewriteBufferReset();
    /*********************************/
    /*此处fp-growth处理函数，得到ib.dat
    /*********************************/
    //mining(server.intelbalance_transnum);
    if(-1 == mining(server.intelbalance_transnum)){
        server.intelbalance_state = REDIS_IB_FIN_ERR;
        return ;
    }
    /*******************/
    /*读取ib.dat
    /*******************/
    /*******************/
    /*计算相关性，将满足的加入set
    /*******************/
    robj* set = valuatePatterns(set);
    if(NULL == set){
        server.intelbalance_state = REDIS_IB_FIN_ERR;
        return ;
    }
    printf("size of the set is %d\n",setTypeSize(set));

    if(intelMigrate('k',set)==-1){
        server.intelbalance_state = REDIS_IB_FIN_ERR;
        return ;
    }
    server.intelbalance_state = REDIS_IB_FIN_OK;
    //server.intelbalance_state = REDIS_IB_FIN_ERR;

}
void ibCommand(redisClient *c){
    long period;

    if (server.cluster_enabled == 0) {
        addReplyError(c,"This instance has cluster support disabled");
        return;
    }
    if(c->argc == 0){
        addReplyError(c,"Period parameter is required");
        return;
    }
    if(server.intelbalance_state != REDIS_IB_OFF){
        addReplyError(c,"IB command is running");
        return;
    }
    if(c->argc == 2){
        if (getLongLongFromObject(c->argv[1],&period ) != REDIS_OK) {
            addReplyErrorFormat(c,"Invalid period specified: %s",
                                (char*)c->argv[1]->ptr);
            return;
        }
        server.intelbalance_last_time = mstime();
        server.intelbalance_period = period;
        server.intelbalance_state = REDIS_IB_ON;
        //dictEmpty(server.db[0].keymetadata,NULL);
        /*状态打开成ib模式,在此模式下，所有get 和 set 请求
        都会将请求的Key以sdf为Key加入server.db[0].trans中
        对应sdf的hash表中。同时将此时的时间戳以sdf为Key加入到server.db[0].timestamps。
        */
        server.intelbalance_time_event_id = aeCreateTimeEvent(server.el,REDIS_IB_PERIOD_SEGMENT,ibProc,NULL,NULL);
        /*
        设置时间事件，每1000ms调用一次，处理器为ibProc。
        在时间事件中将 访问时间戳 < ibProc调用时间-1000ms的事务从server.db[0].trans
        中摘下写入并写入dat文件.如果当前时间>=ib指令开始时间+period则开始fp-growth。


        */

    }else{
        addReplyError(c,"Parameter is so mach");
        return;
    }
    addReply(c,shared.queued);
}
