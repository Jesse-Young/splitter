/*************************************************************************
    > File Name: chunk_block.c
    > Author: yzx
    > Mail: yzx1211@163.com 
    > Created Time: 2015年11月03日 星期二 17时32分41秒
 ************************************************************************/

#include <stdio.h>
#include <malloc.h>
#include <string.h>
#include <assert.h>
#include <vector.h>
#include <chunk.h>

#if 1 //for test
char* blk_id_2_ptr(cluster_head_t *pclst, unsigned int id)
{
    int ptrs_bit = pclst->pg_ptr_bits;
    int ptrs = (1 << ptrs_bit);
    u64 direct_pgs = CLST_NDIR_PGS;
    u64 indirect_pgs = 1<<ptrs_bit;
    u64 double_pgs = 1<<(ptrs_bit*2);
    int pg_id = id >> pclst->blk_per_pg_bits;
    int offset;
    char *page, **indir_page, ***dindir_page;

    if(pg_id < direct_pgs)
    {
        page = pclst->pglist[pg_id];
    }
    else if((pg_id -= direct_pgs) < indirect_pgs)
    {
        indir_page = (char **)pclst->pglist[CLST_IND_PG];
        offset = pg_id;
        page = indir_page[offset];
    }
    else if((pg_id -= indirect_pgs) < double_pgs)
    {
        dindir_page = (char ***)pclst->pglist[CLST_DIND_PG];
        offset = pg_id >> ptrs_bit;
        indir_page = dindir_page[offset];
        offset = pg_id & (ptrs-1);
        page = indir_page[offset];
    }
    else
    {
        printf("warning: %s: id is too big\r\n", __func__);
        return 0;
    }
    
    offset = id & (pclst->blk_per_pg-1);
    return    page + (offset << BLK_BITS); 
    
}

char* db_id_2_ptr(cluster_head_t *pclst, unsigned int id)
{
#if 0
    char *pblk;
    unsigned int blk_id;

    blk_id = id/pclst->db_per_blk;
    pblk = blk_id_2_ptr(pclst, blk_id);

    return pblk+id%pclst->db_per_blk * DBLK_SIZE;
#endif
    return blk_id_2_ptr(pclst, id/pclst->db_per_blk) + id%pclst->db_per_blk * DBLK_SIZE;
    
}

char* vec_id_2_ptr(cluster_head_t *pclst, unsigned int id)
{
#if 0
    char *pblk;
    unsigned int blk_id;

    blk_id = id/pclst->db_per_blk;
    pblk = blk_id_2_ptr(pclst, blk_id);

    return pblk+id%pclst->db_per_blk * DBLK_SIZE;
#endif
    return blk_id_2_ptr(pclst, id/pclst->vec_per_blk) + id%pclst->vec_per_blk * VBLK_SIZE;
    
}
#endif


char* cluster_alloc_page()
{
    return malloc(4096);
}

int cluster_add_page(cluster_head_t **ppclst)
{
    char *page, **indir_page, ***dindir_page;
    u32 size, old_head, new_head;
    int i, total, id, pg_id, offset, offset2;
    block_head_t *blk;
    cluster_head_t *pclst = *ppclst;
    int ptrs_bit = pclst->pg_ptr_bits;
    int ptrs = (1 << ptrs_bit);
    u64 direct_pgs = CLST_NDIR_PGS;
    u64 indirect_pgs = 1<<ptrs_bit;
    u64 double_pgs = 1<<(ptrs_bit*2);
    

    pg_id = atomic_add_return(1, (atomic_t *)&pclst->pg_cursor);
    if(pclst->pg_num_max <= pg_id)
    {
        atomic_sub(1, (atomic_t *)&pclst->pg_cursor);
        return CLT_FULL;
    }

    page = cluster_alloc_page();
    if(page == NULL)
        return CLT_NOMEM;
  
//    pclst->pglist[pgid] = page;
    id = pg_id;
    if(id < direct_pgs)
    {
        pclst->pglist[id] = page;
    }
    else if((id -= direct_pgs) < indirect_pgs)
    {
        offset = id;
        if(offset == 0)
        {
            indir_page = (char **)cluster_alloc_page();
            if(indir_page == NULL)
            {
                printf("\r\nOUT OF MEMERY    %d\t%s", __LINE__, __FUNCTION__);
                return CLT_NOMEM;
            }
            pclst->pglist[CLST_IND_PG] = (char *)indir_page;
        }
        else
        {
            while(pclst->pglist[CLST_IND_PG] == 0)
            {
                ;
            }                
        }
        indir_page = (char **)pclst->pglist[CLST_IND_PG];
        indir_page[offset] = page;
    }
    else if((id -= indirect_pgs) < double_pgs)
    {
        offset = id >> ptrs_bit;
        offset2 = id & (ptrs-1);
        if(id == 0)
        {
            dindir_page = (char ***)cluster_alloc_page();
            if(dindir_page == NULL)
            {
                printf("\r\nOUT OF MEMERY    %d\t%s", __LINE__, __FUNCTION__);
                return CLT_NOMEM;
            }
            pclst->pglist[CLST_DIND_PG] = (char *)dindir_page;
        }
        else
        {
            while(pclst->pglist[CLST_DIND_PG] == 0)
            {
                ;
            }

        }
        dindir_page = (char ***)pclst->pglist[CLST_DIND_PG];
        
        if(offset2 == 0)
        {
            indir_page = (char **)cluster_alloc_page();
            if(indir_page == NULL)
            {
                printf("\r\nOUT OF MEMERY    %d\t%s", __LINE__, __FUNCTION__);
                return CLT_NOMEM;
            }
            else
            {

            }
            dindir_page[offset] = indir_page;
        }
        else
        {
            while(dindir_page[offset] == 0)
            {
                ;
            }
        }
        indir_page = dindir_page[offset];        
        indir_page[offset2] = page;
    }
    else
    {
        printf("warning: %s: id is too big", __func__);
        return CLT_ERR;
    }
    
    id = (pg_id<< pclst->blk_per_pg_bits);
//    pclst->pg_cursor++;
    total = pclst->blk_per_pg;
    blk = (block_head_t *)page;

    for(i=1; i<total; i++)
    {
        blk->magic = 0xdeadbeef;
        blk->next = id + i;
        blk = (block_head_t *)(page + (i << BLK_BITS));
    }
    blk->magic = 0xdeadbeef;
    do
    {
        old_head = pclst->blk_free_head;
        blk->next = old_head;
        pclst->blk_free_head = id;
        new_head = atomic_cmpxchg((atomic_t *)&pclst->blk_free_head, old_head, id);
    }while(new_head != id);

    atomic_add(total, (atomic_t *)&pclst->free_blk_cnt);
    return SPT_OK;
}

void cluster_destroy(cluster_head_t *pclst)
{
    printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
}

cluster_head_t * cluster_init()
{
    cluster_head_t *phead;
    int ptr_bits;
    u32 vec;
    spt_vec_t *pvec;

    phead = cluster_alloc_page();
    if(phead == NULL)
        return 0;

    memset(phead, 0, 4096);

    if(sizeof(char *) == 4)
    {
        ptr_bits = 2;
    }
    if(sizeof(char *) == 8)
    {
        ptr_bits = 3;
    }
    
    phead->pg_num_max = CLST_PG_NUM_MAX;
//    phead->pg_num_total = (sizeof(cluster_head_t)>>ptr_bits);
    phead->blk_per_pg_bits = PG_BITS - BLK_BITS;
//    phead->db_per_pg_bits= PG_BITS - DBLK_BITS;
//    phead->vec_per_pg_bits = PG_BITS - VBLK_BITS;
    phead->pg_ptr_bits= PG_BITS - ptr_bits;
    phead->blk_per_pg = PG_SIZE/BLK_SIZE;
    phead->db_per_blk= BLK_SIZE/DBLK_SIZE; /*��������룬ʣ��ռ��˷�*/
    phead->vec_per_blk = BLK_SIZE/VBLK_SIZE;
    phead->vec_free_head = SPT_NULL;
    phead->blk_free_head = SPT_NULL;
    phead->dblk_free_head = SPT_NULL;
    
    phead->vec_buf_head = SPT_NULL;
    phead->dblk_buf_head = SPT_NULL;

    phead->debug = 1;

    vec = vec_alloc(&phead, &pvec);
    if(pvec == 0)
    {
        cluster_destroy(phead);
        return NULL;
    }
    pvec->pos = 0;
    pvec->down = SPT_NULL;
    pvec->rdn = -1;
    spt_set_data_flag(pvec->flag);
    phead->vec_head = vec;

    return phead;
}

int blk_alloc(cluster_head_t **ppcluster, char **blk)
{
    int blk_id, new_head;
    block_head_t *pblk;
    cluster_head_t *pcluster=*ppcluster;
    int try_cnts = 0;

    do
    {
        while((blk_id = pcluster->blk_free_head) == -1 )
        {
            if(SPT_OK != cluster_add_page(ppcluster))
            {
                try_cnts++;
                if(try_cnts != 0)
                {
                    *blk = NULL;
                    return -1;
                }
            }
            pcluster=*ppcluster;
        }
        pblk = (block_head_t *)blk_id_2_ptr(pcluster,blk_id);
        new_head = pblk->next;
    }while(new_head != atomic_cmpxchg((atomic_t *)&pcluster->blk_free_head, blk_id, new_head));
    atomic_sub(1, (atomic_t *)&pcluster->free_blk_cnt);
    *blk = (char *)pblk;

    return blk_id;    
}

int db_add_blk(cluster_head_t **ppcluster)
{
    char *pblk;
    int i, total, id, blkid, new_head, old_head;
    db_head_t *db;
    cluster_head_t *head;

    blkid = blk_alloc(ppcluster, &pblk);
    if(pblk == NULL)
        return CLT_NOMEM;

    head=*ppcluster;
    total = head->db_per_blk;
    id = (blkid*total);
    db = (db_head_t *)pblk;

    for(i=1; i<total; i++)
    {
        db->magic = 0xdeadbeef;
        db->next = id + i;
        db = (db_head_t *)(pblk + (i * DBLK_SIZE));
    }
    db->magic = 0xdeadbeef;

    do
    {
        old_head = head->dblk_free_head;
        db->next = old_head;
        head->blk_free_head = id;
        new_head = atomic_cmpxchg((atomic_t *)&head->dblk_free_head, old_head, id);
    }while(new_head != id);

    atomic_add(total, (atomic_t *)&head->free_dblk_cnt);

    return SPT_OK;
}

int vec_add_blk(cluster_head_t **ppcluster)
{
    char *pblk;
    int i, total, id, blkid, old_head, new_head;
    vec_head_t *vec;
    cluster_head_t *head;

    blkid = blk_alloc(ppcluster, &pblk);
    if(pblk == NULL)
        return CLT_NOMEM;

    head=*ppcluster;
    total = head->vec_per_blk;
    id = (blkid*total);
    vec = (vec_head_t *)pblk;

    for(i=1; i<total; i++)
    {
        vec->magic = 0xdeadbeef;
        vec->next = id + i;
        vec = (vec_head_t *)(pblk + (i << VBLK_BITS));
    }

    vec->magic = 0xdeadbeef;

    do
    {
        old_head = head->free_dblk_cnt;
        vec->next = old_head;
        head->blk_free_head = id;
        new_head = atomic_cmpxchg((atomic_t *)&head->free_dblk_cnt, old_head, id);
    }while(new_head != id);

    atomic_add(total, (atomic_t *)&head->free_vec_cnt);
    
    return SPT_OK;
}


unsigned int db_alloc(cluster_head_t **ppcluster, char **db)
{
    int db_id;
    db_head_t *pdb;
    cluster_head_t *pcluster=*ppcluster;
    u32 try_cnts, new_head, old_head;

    do
    {
        while((db_id = pcluster->dblk_free_head) == -1 )
        {
            if(SPT_OK != db_add_blk(ppcluster))
            {
                try_cnts++;
                if(try_cnts != 0)
                {
                    *db = NULL;
                    return -1;
                }
            }
            pcluster=*ppcluster;
        }
        pdb = (db_head_t *)db_id_2_ptr(pcluster,db_id);
        new_head = pdb->next;
    }while(new_head != atomic_cmpxchg((atomic_t *)&pcluster->dblk_free_head, db_id, new_head));
    atomic_sub(1, (atomic_t *)&pcluster->free_dblk_cnt);
    atomic_add(1, (atomic_t *)&pcluster->used_dblk_cnt);
    *db = (char *)pdb;

    return db_id;
}

void db_free(cluster_head_t *pcluster, int id)
{
    db_head_t *pdb;
    u32 old_head;
    
    pdb = (db_head_t *)db_id_2_ptr(pcluster,id);
    do
    {
        old_head = pcluster->dblk_free_head;
        pdb->next = old_head;
        
    }while(id != atomic_cmpxchg((atomic_t *)&pcluster->dblk_free_head, old_head, id));
    
    atomic_add(1, (atomic_t *)&pcluster->free_dblk_cnt);
    atomic_sub(1, (atomic_t *)&pcluster->used_dblk_cnt);

    return ;
}


unsigned int vec_alloc(cluster_head_t **ppcluster, spt_vec_t **vec)
{
    int vec_id;
    vec_head_t *pvec;
    cluster_head_t *pcluster = *ppcluster;
    u32 try_cnts, new_head, old_head;

    do
    {
        while((vec_id = pcluster->blk_free_head) == -1 )
        {
            if(SPT_OK != vec_add_blk(ppcluster))
            {
                try_cnts++;
                if(try_cnts != 0)
                {
                    *vec = NULL;
                    return -1;
                }
            }
            pcluster=*ppcluster;
        }
        pvec = (vec_head_t *)vec_id_2_ptr(pcluster,vec_id);
        new_head = pvec->next;
    }while(new_head != atomic_cmpxchg((atomic_t *)&pcluster->blk_free_head, vec_id, new_head));
    atomic_sub(1, (atomic_t *)&pcluster->free_blk_cnt);
    *vec = (char *)pvec;
    
    return vec_id;
}

void vec_free(cluster_head_t *pcluster, int id)
{
    vec_head_t *pvec;
    u32 old_head;

    pvec = (vec_head_t *)vec_id_2_ptr(pcluster,id);
    do
    {
        old_head = pcluster->vec_free_head;
        pvec->next = old_head;
        
    }while(id != atomic_cmpxchg((atomic_t *)&pcluster->vec_free_head, old_head, id));
    
    atomic_add(1, (atomic_t *)&pcluster->free_vec_cnt);
    atomic_sub(1, (atomic_t *)&pcluster->used_vec_cnt);

    return ;
}
/*cluster�Ժ��õ�����ǰֻ��һ��cluster*/
void vec_free_to_buf(cluster_head_t *pcluster, int id, int thread_id)
{
    u64 cmd, q_idx;

    cmd = id;
    q_idx = atomic64_add_return(1, (atomic64_t *)&g_thrd_h->free_q_idx)-1;

    lfo_inq(g_thrd_h->pfree_q, thread_id, cmd, q_idx);
    return ;

}

void db_free_to_buf(cluster_head_t *pcluster, int id, int thread_id)
{
    u64 cmd, q_idx;

    cmd = (SPT_PTR_DATA << 32) | id;
    q_idx = atomic64_add_return(1, (atomic64_t *)&g_thrd_h->free_q_idx)-1;

    lfo_inq(g_thrd_h->pfree_q, thread_id, cmd, q_idx);
    return ;
}

void test_p()
{
    printf("haha\r\n");
}

int test_add_page(cluster_head_t **ppclst)
{
    char *page, **indir_page, ***dindir_page;
    u32 size;
    int pg_id, offset;
    cluster_head_t *pclst = *ppclst;
    int ptrs_bit = pclst->pg_ptr_bits;
    int ptrs = (1 << ptrs_bit);
    u64 direct_pgs = CLST_NDIR_PGS;
    u64 indirect_pgs = 1<<ptrs_bit;
    u64 double_pgs = 1<<(ptrs_bit*2);

    if(pclst->pg_num_max == pclst->pg_cursor)
    {
        return CLT_FULL;
    }

    page = cluster_alloc_page();
    if(page == NULL)
        return CLT_NOMEM;

    if(pclst->pg_cursor == pclst->pg_num_total)
    {
        cluster_head_t *new_head;
        size = (sizeof(char *)*pclst->pg_num_total + sizeof(cluster_head_t));
        if(size*2 >= PG_SIZE)
        {
            new_head = (cluster_head_t *)cluster_alloc_page();
            if(new_head == NULL)
            {
                free(page);
                return CLT_NOMEM;
            }
            memcpy((char *)new_head, (char *)pclst, size);
            new_head->pg_num_total = pclst->pg_num_max;
        }
        else
        {
            new_head = malloc(size*2);
            if(new_head == NULL)
            {
                free(page);
                return CLT_NOMEM;
            }    
            memcpy(new_head, pclst, size);
            new_head->pg_num_total = (size*2-sizeof(cluster_head_t))/sizeof(char *);
        }
        free(pclst);
//        new_head->pglist[0] = new_head;
        pclst = new_head;
        *ppclst = pclst;

        
    }
    pg_id = pclst->pg_cursor;
//    pclst->pglist[pgid] = page;
    if(pg_id < direct_pgs)
    {
        pclst->pglist[pg_id] = page;
    }
    else if((pg_id -= direct_pgs) < indirect_pgs)
    {
        indir_page = (char **)pclst->pglist[CLST_IND_PG];
        offset = pg_id;
        indir_page[offset] = page;
    }
    else if((pg_id -= indirect_pgs) < double_pgs)
    {
        dindir_page = (char ***)pclst->pglist[CLST_DIND_PG];
        offset = pg_id >> ptrs_bit;
        indir_page = dindir_page[offset];
        offset = pg_id & (ptrs-1);
        indir_page[offset] = page;
    }
    else
    {
        printf("warning: %s: id is too big", __func__);
        return 0;
    }
    pclst->pg_cursor++;
    return SPT_OK;
}


int test_add_N_page(cluster_head_t **ppclst, int n)
{
    int i;
    for(i=0;i<n;i++)
    {
        if(SPT_OK != cluster_add_page(ppclst))
        {
            printf("\r\n%d\t%s\ti:%d", __LINE__, __FUNCTION__, i);
            return -1;
        }        
    }
    return 0;
}

void test_vec_alloc_n_times(cluster_head_t **ppclst)
{
    int i,vec_a;
    spt_vec_t *pvec_a, *pid_2_ptr;
    cluster_head_t *clst = *ppclst;

    
    for(i=0; ; i++)
    {
        vec_a = vec_alloc(ppclst, &pvec_a);
        if(pvec_a == 0)
        {
            //printf("\r\n%d\t%s\ti:%d", __LINE__, __FUNCTION__, i);
            break;
        }
        clst = *ppclst;
        pid_2_ptr = (spt_vec_t *)vec_id_2_ptr(clst, vec_a);
        if(pid_2_ptr != pvec_a)
        {
            printf("\r\n vec_a:%d pvec_a:%p pid_2_ptr:%p", vec_a,pvec_a,pid_2_ptr);
        }
    }
    printf("\r\n total:%d", i);
    for(;i>0;i--)
    {
        vec_free(clst, i-1);
    }
    printf("\r\n ==============done!");
    return;
}

#if 0
int main()
{
    cluster_head_t *clst;
    int *id;
    char **p;

    clst = cluster_init();
    if(clst == NULL)
    {
        assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return 1;
    }
//    id = malloc(10*sizeof(int));
//    p  = malloc(10*sizeof(char *));
//    test_vec_alloc_n_times(&clst, 1, id, p);
//    test_add_N_page(&clst, CLST_PG_NUM_MAX);
    while(1)
    {
        test_vec_alloc_n_times(&clst);
    }
    test_p();
    return 0;
}
#endif
#if 0
int blk_db_add(CHK_HEAD* pchk, unsigned short blk_id)
{
    char* blk;
    db_head_t *dhead;
    unsigned int db_num;
    unsigned int i;
    unsigned int db_id,db_start;

    if(!pchk)
        return 1;

    db_num = BLK_SIZE/DBLK_SIZE;
    blk = blk_id_2_ptr(pchk, blk_id);
    db_start = db_id = blk_id*db_num;
    

    for(i=0; i<db_num; i++)
    {
        dhead = (db_head_t *)(blk + i*DBLK_SIZE);
        dhead->magic = 0xdeadbeef;
        dhead->next = ++db_id;
    }
    dhead->next = pchk->dblk_free_head;
    pchk->dblk_free_head = db_start;

    return 0;
}

int blk_vec_add(char *pchk, unsigned short blk_id)
{
    char *blk;
    CHK_HEAD *pchkh = (CHK_HAED *)pchk;
    VEC_HEAD *vhead;
    unsigned int vec_num;
    unsigned int i;
    unsigned int vec_id,vec_start;

    if(!pchk)
        return 1;

    vec_num = BLK_SIZE/VBLK_SIZE;
    blk = pchk + blk_id*BLK_SIZE;
    vec_start = vec_id = blk_id*vec_num;
    

    for(i=0; i<vec_num; i++)
    {
        vhead = (VEC_HEAD *)(blk + i*sizeof(VEC_STR));
        vhead->magic = 0xdeadbeef;
        vhead->next = ++vec_id;
    }
    vhead->next = pchkh->vec_free_head;
    pchkh->vec_free_head = vec_start;

    return 0;
}


unsigned short db_alloc(CHK_HEAD *pchk)
{
    unsigned short db_id,db_next, blk_id;
    db_head_t *pdb;

    if(!pchk)
        return 0;

    if(pchk->dblk_free_head != 0)
    {
        db_id = pchk->dblk_free_head;
        pdb = (db_head_t *)db_id_2_ptr(pchk, db_id);
        pchk->dblk_free_head = pdb->next;
        return db_id;
    }

    if((blk_id=blk_alloc(pchk))==0)
    {
        return 0;
    }

    blk_db_add(pchk, blk_id);

    db_id = pchk->dblk_free_head;
    pdb = (db_head_t *)db_id_2_ptr(pchk, db_id);
    pchk->db_free_head = pdb->next;
    return db_id;

}

unsigned short vec_alloc(CHK_HEAD *pchk)
{
    unsigned short vec_id,vec_next, blk_id;
    VEC_HEAD *pvec;

    if(!pchk)
        return 0;

    if(pchk->vec_free_head != 0)
    {
        vec_id = pchk->vec_free_head;
        pvec = (VEC_HEAD *)vec_id_2_ptr(pchk, vec_id);
        pchk->vec_free_head = pvec->next;
        return vec_id;
    }

    if((blk_id=blk_alloc(pchk))==0)
    {
        return 0;
    }

    blk_vec_add(pchk, blk_id);

    vec_id = pchk->vec_free_head;
    pvec = (VEC_HEAD *)vec_id_2_ptr(pchk, vec_id);
    pchk->vec_free_head = pvec->next;
    return vec_id;

}
#endif



