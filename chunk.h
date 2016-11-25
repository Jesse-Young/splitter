/*************************************************************************
    > File Name: chunk.h
    > Author: yzx
    > Mail: yzx1211@163.com 
    > Created Time: 2015年11月03日 星期二 17时10分34秒
 ************************************************************************/
#ifndef _SPLITTER_CLUSTER_H
#define _SPLITTER_CLUSTER_H

#define SPT_NULL         (-1)
#define SPT_INVALID        (-2)

#define SPT_RIGHT        0
#define SPT_DOWN         1

/*Ϊ����Ч�ʣ�blk��СΪ2����������*/
#define CLST_PG_NUM_MAX ((1<<14) ) /*cluster max = 64m*/
//#define CHUNK_SIZE (1<<16)    /*字节*/
#define PG_BITS 12
#define PG_SIZE (1<<PG_BITS)
#define BLK_BITS 5
#define BLK_SIZE (1<<BLK_BITS)

#define CLST_NDIR_PGS          320        //how much

#if 0
#define CLST_IND_PGS_BITS        6
#define CLST_DIND_PGS_BITS        5
#define CLST_TIND_PGS_BITS        4
#endif
//�������Ѱַ����Ѱַ1G��Χ��Զ��64M�Ĵش�С���ơ�
#define CLST_IND_PG            (CLST_NDIR_PGS)    //index
#define CLST_DIND_PG        (CLST_IND_PG+1)    //index
#define CLST_N_PGS            (CLST_DIND_PG+1)//pclst->pglist[] max


//#define CLST_TIND_PGS        (CLST_DIND_PGS+1)

#if 0 //for test
static inline char* blk_id_2_ptr(cluster_head_t *pclst, unsigned int id)
{
    int ptrs_bit = pclst->pg_ptr_bits;
    int ptrs = (1 << ptrs_bit);
    u64 direct_pgs = CLST_N_PGS;
    u64 indirect_pgs = CLST_IND_PGS<<ptrs_bit;
    u64 double_pgs = CLST_DIND_PGS<<(ptrs_bit*2);
    int pg_id = id >> pclst->blk_per_pg_bits;
    int offset;
    char *page;

    if(pg_id < direct_pgs)
    {
    }
    else if((pg_id -= direct_pgs) < indirect_pgs)
    {
        offset = CLST_N_PGS + (pg_id >> ptrs_bit);
        page = (char *)pclst->pglist[offset];
        pg_id = pg_id & (ptrs- 1);
    }
    else if((pg_id -= indirect_pgs) < double_pgs)
    {
        offset = CLST_N_PGS + CLST_IND_PGS + (pg_id >> (ptrs_bit*2));
        page = (char *)pclst->pglist[offset];
        offset = (pg_id >> ptrs_bit) & (ptrs - 1);
        page += offset*sizeof(char *);
//        page = (char *)page[offset];
        pg_id = pg_id & (ptrs- 1);
    }
    else if(((pg_id -= double_pgs)>> (ptrs_bit*3)) < CLST_TIND_PGS)
    {
        offset = CLST_N_PGS + CLST_IND_PGS + CLST_DIND_PGS + (pg_id >> (ptrs_bit*3));
        page = (char *)pclst->pglist[offset];
        offset = (pg_id >> (ptrs_bit*2)) & (ptrs - 1);
        page += offset*sizeof(char *);
        //page = (char *)page[offset];
        offset = (pg_id >> ptrs_bit) & (ptrs - 1);
        page += offset*sizeof(char *);
        //page = (char *)page[offset];
        pg_id = pg_id & (ptrs- 1);
    }
    else
    {
        printf("warning: %s: id is too big", __func__);
        return 0;
    }
    
    offset = id & (pclst->blk_per_pg-1);
    return    pclst->pglist[pg_id] + (offset << DBLK_BITS); 
    
}

static inline char* db_id_2_ptr(cluster_head_t *pclst, unsigned int id)
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

static inline char* vec_id_2_ptr(cluster_head_t *pclst, unsigned int id)
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
#else
extern char* blk_id_2_ptr(cluster_head_t *pclst, unsigned int id);
extern char* db_id_2_ptr(cluster_head_t *pclst, unsigned int id);
extern char* vec_id_2_ptr(cluster_head_t *pclst, unsigned int id);
#endif

#define CLT_FULL 3
#define CLT_NOMEM 2
#define CLT_ERR 1
#define SPT_OK 0
#define SPT_ERR 1
#define SPT_NOMEM 2
#define SPT_DO_AGAIN 3

unsigned int db_alloc(cluster_head_t **ppcluster, char **db);
cluster_head_t * cluster_init();


#endif

