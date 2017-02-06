#define _GNU_SOURCE
#include <stdio.h>
#include <malloc.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>
#include <vector.h>
#include <chunk.h>
#include <unistd.h>

#include <sched.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/sysinfo.h>
#include <sys/types.h>
#include <sys/ipc.h>  
#include <sys/shm.h>  
#include <errno.h>
#include <signal.h>
#include<sys/wait.h>
#include <sys/stat.h> /* For mode constants */
#include <fcntl.h> /* For O_* constants */


__thread u32 g_thrd_id;
cluster_head_t *pgclst;
spt_thrd_t *g_thrd_h;


spt_dbg_info g_dbg_info = {0};
#define DBG_DATA_NUM 10
u64 g_data[DBG_DATA_NUM]={0xee,0xbe,0xba,0xb6,0xae,
                          0xca,0x91,0x8a,0x36,0x06};


u64 find_fs(char *a, u64 start, u64 len);
int diff_identify(char *a, char *b,u64 start, u64 len, vec_cmpret_t *result);

void spt_bit_clear(u8 *p, u64 start, u64 len)
{
    u8 bitstart, bitend;
    s64 lenbyte;
    u8 *acstart;

    bitstart = start%8;
    bitend = (bitstart + len)%8;
    acstart = p + start/8;
    lenbyte =  (bitstart + len)/8;

    if(bitstart != 0)
    {
        *acstart = *acstart >> (8-bitstart) << (8-bitstart);
        acstart++;
    }
    while(lenbyte > 0)
    {
        *acstart = 0;
        acstart++;
        lenbyte--;
    }
    if(bitend != 0)
    {
        *acstart = *acstart << bitend;
        *acstart = *acstart >> bitend;
    }
    return;
}

void spt_bit_cpy(u8 *to, const u8 *from, u64 start, u64 len)
{
    u8 bitstart, bitend;
    s64 lenbyte;
    u8 uca, ucb;
    u8 *acstart;
    u8 *bcstart;

    bitstart = start%8;
    bitend = (bitstart + len)%8;
    acstart = to + start/8;
    bcstart = (u8 *)from + start/8;
    lenbyte =  (bitstart + len)/8;

    if(bitstart != 0)
    {
        uca = *acstart >> (8-bitstart) << (8-bitstart);
        ucb = *bcstart << bitstart;
        ucb = ucb >> bitstart;
        *acstart = uca | ucb;
        acstart++;
        bcstart++;
        lenbyte--;
    }

    while(lenbyte > 0)
    {
        *acstart = *bcstart;
        acstart++;
        bcstart++;
        lenbyte--;
    }

    if(bitend != 0)
    {
        uca = *acstart << bitend;
        uca = uca >> bitend;
        ucb = *bcstart >> (8-bitend) << (8-bitend);
        *acstart = uca | ucb;
        acstart++;
        bcstart++;
    }
    return;
}

void spt_stack_init(spt_stack *p_stack, int size)
{
    p_stack->p_bottom = (void **)malloc(size * sizeof(void *));
   
    if (p_stack->p_bottom == NULL)
    {
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return;
    }
    p_stack->p_top = p_stack->p_bottom;
    p_stack->stack_size = size;

    return;
}

int spt_stack_full(spt_stack *p_stack)
{  
    return p_stack->p_top - p_stack->p_bottom >= p_stack->stack_size-1;
}

int spt_stack_empty(spt_stack *p_stack)
{
    return p_stack->p_top <= p_stack->p_bottom;
}

void spt_stack_push(spt_stack *p_stack, void *value)
{
    spt_stack *p_tmp = (spt_stack *)p_stack->p_bottom;
    
    if (spt_stack_full(p_stack))
    {      
        p_stack->p_bottom = 
        (void **)realloc(p_stack->p_bottom, 2*p_stack->stack_size*sizeof(void *));
        
        if (!p_stack->p_bottom)
        {
            free(p_tmp);
            printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
            return;
        }
        p_stack->stack_size = 2*p_stack->stack_size;
    }
    (p_stack->p_top)++;
    *(p_stack->p_top) = value;
    
    return;
 }

void* spt_stack_pop(spt_stack *p_stack)
{
    void *value = 0;
   
    if (spt_stack_empty(p_stack))
    {
        return (void *)-1;
    }
    value = *(p_stack->p_top--);
   
    return value;
}

void spt_stack_destroy(spt_stack *p_stack)
{
    free(p_stack->p_bottom);
//    free(p_stack);
    p_stack->stack_size = 0;
    p_stack->p_bottom = NULL;
    p_stack->p_top = NULL;
    p_stack = NULL;    
   
    return;
}


u64 ullfind_firt_zero(u64 dword)
{
    return 0;
}

u64 ullfind_firt_set(u64 dword)
{
    int i;
    for(i=63; i>=0; i--)
    {
        if(dword >> i != 0)
            return 63-i;
    }

    return 64;

}
u32 ulfind_firt_zero(u32 word)
{
    return 0;
}
u64 uifind_firt_set(u32 word)
{
    int i;
    for(i=31; i>=0; i--)
    {
        if(word >> i != 0)
            return 31-i;
    }

    return 32;

}
u16 usfind_firt_zero(u16 word)
{
    return 0;
}
u64 usfind_firt_set(u16 word)
{
    int i;
    for(i=15; i>=0; i--)
    {
        if(word >> i != 0)
            return 15-i;
    }

    return 16;

}
u8 ucfind_firt_zero(u8 byte)
{
    return 0;
}
u64 ucfind_firt_set(u8 byte)
{
    int i;
    for(i=7; i>=0; i--)
    {
        if(byte >> i != 0)
            return 7-i;
    }
    return 8;
}

int get_data_id(cluster_head_t *pclst, spt_vec* pvec)
{
    spt_vec *pcur, *pnext, *ppre;
    spt_vec tmp_vec, cur_vec, next_vec;
    //u8 direction;
    u32 vecid;
    int ret;

get_id_start:
    ppre = 0;
    cur_vec.val = pvec->val;
    pcur = pvec;
    if(cur_vec.flag == SPT_VEC_FLAG_RAW)
    {
        smp_mb();
        cur_vec.val = pvec->val;
        if(cur_vec.flag == SPT_VEC_FLAG_RAW)
        {
            return SPT_DO_AGAIN;
        }
    }
    if(cur_vec.valid == SPT_VEC_INVALID)
    {
        return SPT_DO_AGAIN;
    }

    while(1)
    {
        if(cur_vec.flag == SPT_VEC_FLAG_DATA)
        {
            return cur_vec.rd;
        }
        else if(cur_vec.flag == SPT_VEC_FLAG_RIGHT)
        {
            pnext = (spt_vec *)vec_id_2_ptr(pclst,cur_vec.rd);
            next_vec.val = pnext->val;
            if(next_vec.flag == SPT_VEC_FLAG_RAW)
            {
                smp_mb();
                next_vec.val = pnext->val;
                if(next_vec.flag == SPT_VEC_FLAG_RAW)
                {
                    goto get_id_start;
                }
            }           
            if(next_vec.valid == SPT_VEC_INVALID)
            {
                tmp_vec.val = cur_vec.val;
                vecid = cur_vec.rd;
                tmp_vec.rd = next_vec.rd;
                if(next_vec.flag == SPT_VEC_FlAG_SIGNPOST)
                {
                    tmp_vec.flag = next_vec.ext_sys_flg;
                }
                else if(next_vec.flag == SPT_VEC_FLAG_DATA)
                {
                    if(next_vec.down == SPT_NULL)
                    {
                        spt_set_data_flag(tmp_vec);
                    }
                    else
                    {
                        tmp_vec.rd = next_vec.down;
                    }                
                }
                else
                {
                    ;
                }
                //cur_vec.val = atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val);
                //if(cur_vec.val == tmp_vec.val)//delete succ
                if(cur_vec.val == atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val))
                {
                    ret = vec_free_to_buf(pclst, vecid, g_thrd_id);
                    if(ret != SPT_OK)
                        return ret;
                    continue;
                }
                else//delete fail
                {
                    cur_vec.val = pcur->val;
                    if(cur_vec.valid == SPT_VEC_INVALID)
                    {
                        if(ppre == NULL)
                            goto get_id_start;
                        else
                        {
                            cur_vec.val = ppre->val;
                            if(cur_vec.valid == SPT_VEC_INVALID)
                                goto get_id_start;
                            else
                            {
                                pcur = ppre;
                                ppre = NULL;
                                continue;
                            }
                        }
                    }
                    continue;
                }
            }
            
            //对于一个向量的右结点:
                //如果是路标，不能是SPT_VEC_SYS_FLAG_DATA
                //如果是向量，必然有down
            if(next_vec.flag == SPT_VEC_FlAG_SIGNPOST)
            {
                if(next_vec.ext_sys_flg == SPT_VEC_SYS_FLAG_DATA)
                {
                    tmp_vec.val = next_vec.val;
                    tmp_vec.valid = SPT_VEC_INVALID;
                    atomic64_cmpxchg((atomic64_t *)pnext, next_vec.val, tmp_vec.val);
                    //set invalid succ or not, refind from cur
                    cur_vec.val = pcur->val;
                    if((cur_vec.valid == SPT_VEC_INVALID))
                    {
                        if(ppre == NULL)
                            goto get_id_start;
                        else
                        {
                            cur_vec.val = ppre->val;
                            if(cur_vec.valid == SPT_VEC_INVALID)
                                goto get_id_start;
                            else
                            {
                                pcur = ppre;
                                ppre = NULL;
                                continue;
                            }
                        }
                    }
                    continue;
                }
            }
            else
            {
                if(next_vec.down == SPT_NULL)
                {
                    tmp_vec.val = next_vec.val;
                    tmp_vec.valid = SPT_VEC_INVALID;
                    atomic64_cmpxchg((atomic64_t *)pnext, next_vec.val, tmp_vec.val);
                    //set invalid succ or not, refind from cur
                    cur_vec.val = pcur->val;
                    if((cur_vec.valid == SPT_VEC_INVALID))
                    {
                        if(ppre == NULL)
                            goto get_id_start;
                        else
                        {
                            cur_vec.val = ppre->val;
                            if(cur_vec.valid == SPT_VEC_INVALID)
                                goto get_id_start;
                            else
                            {
                                pcur = ppre;
                                ppre = NULL;
                                continue;
                            }
                        }
                    }
                    continue;
                }
            }
        }
        //cur是路标，方向肯定是right,遍历过程中不会对方向是down的路标调用此接口
        else
        {
            if(cur_vec.ext_sys_flg == SPT_VEC_SYS_FLAG_DATA)
            {
                tmp_vec.val = cur_vec.val;
                tmp_vec.valid = SPT_VEC_INVALID;
                cur_vec.val = atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val);
                /*set invalid succ or not, refind from ppre*/;
                if(ppre == NULL)
                    goto get_id_start;
                else
                {
                    cur_vec.val = ppre->val;
                    if(cur_vec.valid == SPT_VEC_INVALID)
                        goto get_id_start;
                    else
                    {
                        pcur = ppre;
                        ppre = NULL;
                        continue;
                    }
                }
            }
            pnext = (spt_vec *)vec_id_2_ptr(pclst,cur_vec.rd);
            next_vec.val = pnext->val;
            if(next_vec.flag == SPT_VEC_FLAG_RAW)
            {
                smp_mb();
                next_vec.val = pnext->val;
                if(next_vec.flag == SPT_VEC_FLAG_RAW)
                {
                    goto get_id_start;
                }
            }
            if(next_vec.valid == SPT_VEC_INVALID)
            {
                tmp_vec.val = cur_vec.val;
                vecid = cur_vec.rd;
                tmp_vec.rd = next_vec.rd;
                if(next_vec.flag == SPT_VEC_FlAG_SIGNPOST)
                {
                    tmp_vec.ext_sys_flg = next_vec.ext_sys_flg;
                }
                else if(next_vec.flag == SPT_VEC_FLAG_DATA)
                {
                    if(next_vec.down == SPT_NULL)
                    {
                        tmp_vec.ext_sys_flg = SPT_VEC_SYS_FLAG_DATA;
                    }
                    else
                    {
                        tmp_vec.rd = next_vec.down;
                    }                
                }
                else
                {
                    ;
                }
                //cur_vec.val = atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val);
                //if(cur_vec.val == tmp_vec.val)//delete succ
                if(cur_vec.val == atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val))//delete succ
                {
                    ret = vec_free_to_buf(pclst, vecid, g_thrd_id);
                    if(ret != SPT_OK)
                        return ret;
                    continue;
                }
                else//delete fail
                {
                    cur_vec.val = pcur->val;
                    if(cur_vec.valid == SPT_VEC_INVALID)
                    {
                        if(ppre == NULL)
                            goto get_id_start;
                        else
                        {
                            cur_vec.val = ppre->val;
                            if(cur_vec.valid == SPT_VEC_INVALID)
                                goto get_id_start;
                            else
                            {
                                pcur = ppre;
                                ppre = NULL;
                                continue;
                            }
                        }
                    }
                    continue;
                }                    
            }
            //对于一个路标结点:
                //next为路标，是不合理结构。
            if(next_vec.flag == SPT_VEC_FlAG_SIGNPOST)
            {
                tmp_vec.val = cur_vec.val;
                tmp_vec.valid = SPT_VEC_INVALID;
                cur_vec.val = atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val);
                /*set invalid succ or not, refind from ppre*/
                if(ppre == NULL)
                    goto get_id_start;
                else
                {
                    cur_vec.val = ppre->val;
                    if(cur_vec.valid == SPT_VEC_INVALID)
                        goto get_id_start;
                    else
                    {
                        pcur = ppre;
                        ppre = NULL;
                        continue;
                    }
                }
            }
            else
            {
                if(next_vec.down == SPT_NULL)
                {
                    tmp_vec.val = next_vec.val;
                    tmp_vec.valid = SPT_VEC_INVALID;
                    atomic64_cmpxchg((atomic64_t *)pnext, next_vec.val, tmp_vec.val);
                    //set invalid succ or not, refind from cur
                    cur_vec.val = pcur->val;
                    if((cur_vec.valid == SPT_VEC_INVALID))
                    {
                        if(ppre == NULL)
                            goto get_id_start;
                        else
                        {
                            cur_vec.val = ppre->val;
                            if(cur_vec.valid == SPT_VEC_INVALID)
                                goto get_id_start;
                            else
                            {
                                pcur = ppre;
                                ppre = NULL;
                                continue;
                            }
                        }
                    }
                    continue;
                }
            }            
        }
        ppre = pcur;
        pcur = pnext;
        cur_vec.val = next_vec.val;
    }

}

int do_insert_dsignpost_right(cluster_head_t *pclst, insert_info_t *pinsert, char *new_data)
{
    spt_vec tmp_vec, *pvec_a;  
    u32 dataid, vecid_a;
    int ret;
    char *pdata;
    spt_dh *pdh;

    dataid = data_alloc_combo(pclst, g_thrd_id, &pdata);
    if(pdata == 0)
    {
        atomic64_add(1,(atomic64_t *)&g_dbg_info.oom_no_db);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return SPT_NOMEM;
    }
    pdh = (spt_dh *)pdata;
    pdh->ref = 1;
    pdata = pdata + sizeof(spt_dh);
    memcpy(pdata, new_data, DATA_SIZE);

    tmp_vec.val = pinsert->key_val;
//    signpost = pinsert->signpost;

    vecid_a = vec_alloc_combo(pclst, g_thrd_id, &pvec_a);
    if(pvec_a == 0)
    {
        /*yzx释放资源， 申请新块，拆分*/
        db_free_to_buf(pclst, dataid, g_thrd_id);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return SPT_NOMEM;
    }
    pvec_a->val = 0;
    pvec_a->flag = SPT_VEC_FLAG_DATA;
    pvec_a->pos = 0;
    pvec_a->rd = dataid;
    pvec_a->down = tmp_vec.rd;

    tmp_vec.rd = vecid_a;
    smp_mb();
    if(pinsert->key_val == atomic64_cmpxchg((atomic64_t *)pinsert->pkey_vec, 
                        pinsert->key_val, tmp_vec.val))
    {
        return SPT_OK;
    }
    else
    {
        db_free_to_buf_simple(pclst, dataid, g_thrd_id);
        vec_free_to_buf_simple(pclst, vecid_a, g_thrd_id);
        ret = fill_in_rsv_list(pclst, 2, g_thrd_id);
        if(ret == SPT_OK)
            return SPT_DO_AGAIN;
        return ret;
    }

}

int do_insert_rsignpost_down(cluster_head_t *pclst, insert_info_t *pinsert, char *new_data)
{
    //spt_vec *ppre = NULL;
    spt_vec tmp_vec, *pvec_a, *pvec_b, *pvec_s;
    //char *pcur_data;//*ppre_data,
    u64 signpost;
    u32 dataid, vecid_a, vecid_b, vecid_s, cnt;
    char *pdata;
    spt_dh *pdh;
    int ret;

    pvec_s = 0;
    dataid = data_alloc_combo(pclst, g_thrd_id, &pdata);
    if(pdata == 0)
    {
        /*申请新块，拆分*/
        //assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return SPT_NOMEM;
    }
    pdh = (spt_dh *)pdata;
    pdh->ref = 1;
    pdata = pdata + sizeof(spt_dh);
    memcpy(pdata, new_data, DATA_SIZE);

    tmp_vec.val = pinsert->key_val;
    signpost = pinsert->signpost;

    vecid_a = vec_alloc_combo(pclst, g_thrd_id, &pvec_a);
    if(pvec_a == 0)
    {
        /*yzx释放资源， 申请新块，拆分*/
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        atomic64_add(1,(atomic64_t *)&g_dbg_info.oom_no_db);
        db_free_to_buf(pclst, dataid, g_thrd_id);
        return SPT_NOMEM;
    }
    pvec_a->val = 0;
//    pvec_a->flag = SPT_VEC_FLAG_DATA;
    pvec_a->pos = 0;
    pvec_a->rd = tmp_vec.rd;
//    pvec_a->down = tmp_vec.rd;

    if(tmp_vec.ext_sys_flg == SPT_VEC_SYS_FLAG_DATA)
    {
        pvec_a->flag = SPT_VEC_FLAG_DATA;
    }
    else
    {
        pvec_a->flag = SPT_VEC_FLAG_RIGHT;
    }
    tmp_vec.rd = vecid_a;
    vecid_b = vec_alloc_combo(pclst, g_thrd_id, &pvec_b);
    if(pvec_b == 0)
    {
        /*yzx释放资源， 申请新块，拆分*/
        atomic64_add(1,(atomic64_t *)&g_dbg_info.oom_no_vec);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        db_free_to_buf(pclst, dataid, g_thrd_id);
        vec_free_to_buf (pclst, vecid_a, g_thrd_id);
        return SPT_NOMEM;
    }
    pvec_b->val = 0;
    pvec_b->flag = SPT_VEC_FLAG_DATA;
    pvec_b->pos = (pinsert->fs-1)&SPT_VEC_SIGNPOST_MASK;
    pvec_b->rd = dataid;
    pvec_b->down = SPT_NULL;
    
    if((pinsert->fs-1)-signpost > SPT_VEC_SIGNPOST_MASK)
    {
        vecid_s = vec_alloc_combo(pclst, g_thrd_id, &pvec_s);
        if(pvec_s == 0)
        {
            /*yzx释放资源， 申请新块，拆分*/
            atomic64_add(1,(atomic64_t *)&g_dbg_info.oom_no_db);
            printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
            db_free_to_buf(pclst, dataid, g_thrd_id);
            vec_free_to_buf(pclst, vecid_a, g_thrd_id);
            vec_free_to_buf(pclst, vecid_b, g_thrd_id);
            return SPT_NOMEM;
        }
        pvec_s->val = 0;        
        pvec_s->idx = (pinsert->fs-1)>>SPT_VEC_SIGNPOST_BIT;
        pvec_s->rd = vecid_b;

        pvec_a->down = vecid_s;
    }
    else
    {
        pvec_a->down = vecid_b;
    }
    smp_mb();
    if(pinsert->key_val == atomic64_cmpxchg((atomic64_t *)pinsert->pkey_vec, 
                        pinsert->key_val, tmp_vec.val))
    {
        return SPT_OK;
    }
    else
    {
        db_free_to_buf_simple(pclst, dataid, g_thrd_id);
        vec_free_to_buf_simple(pclst, vecid_a, g_thrd_id);
        vec_free_to_buf_simple(pclst, vecid_b, g_thrd_id);
        cnt = 3;
        if(pvec_s != 0)
        {
            vec_free_to_buf_simple(pclst, vecid_s, g_thrd_id);
            cnt = 4;
        }
        ret = fill_in_rsv_list(pclst, cnt, g_thrd_id);
        if(ret == SPT_OK)
            return SPT_DO_AGAIN;
        return ret;
    }
}

int do_insert_first_set(cluster_head_t *pclst, insert_info_t *pinsert, char *new_data)
{
    u32 dataid;
    char *pdata;
    spt_vec tmp_vec, *pcur;
    spt_dh *pdh;
    int ret;

    dataid = data_alloc_combo(pclst, g_thrd_id, &pdata);
    if(pdata == 0)
    {
        /*申请新块，拆分*/
        atomic64_add(1,(atomic64_t *)&g_dbg_info.oom_no_db);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return SPT_NOMEM;
    }
    pdh = (spt_dh *)pdata;
    pdh->ref = 1;
    pdata = pdata + sizeof(spt_dh);
    memcpy(pdata, new_data, DATA_SIZE);

    pcur = (spt_vec *)vec_id_2_ptr(pclst,pclst->vec_head);
//    cur_vec.val = pcur->val;
    assert(pcur == pinsert->pkey_vec);
    tmp_vec.val = pinsert->key_val;
//    tmp_vec.val = cur_vec.val;
    tmp_vec.rd = dataid;
    smp_mb();
    if(pinsert->key_val == atomic64_cmpxchg((atomic64_t *)pinsert->pkey_vec, 
                        pinsert->key_val, tmp_vec.val))
    {
        return SPT_OK;
    }
    else
    {
        db_free_to_buf_simple(pclst, dataid, g_thrd_id);      
        ret = fill_in_rsv_list(pclst, 1, g_thrd_id);
        if(ret == SPT_OK)
            return SPT_DO_AGAIN;
        return ret;
    }

}

int do_insert_up_via_r(cluster_head_t *pclst, insert_info_t *pinsert, char *new_data)
{
    spt_vec tmp_vec, *pvec_a, *pvec_b, *pvec_s, *pvec_s2;
    //char *pcur_data;//*ppre_data,
    u64 signpost;
    u32 dataid, vecid_a, vecid_b, vecid_s, vecid_s2, tmp_rd, cnt;
    char *pdata;
    spt_dh *pdh;
    int ret;

    pvec_b = 0;
    pvec_s = 0;
    pvec_s2 = 0;
    dataid = data_alloc_combo(pclst, g_thrd_id, &pdata);
    if(pdata == 0)
    {
        /*申请新块，拆分*/
        atomic64_add(1,(atomic64_t *)&g_dbg_info.oom_no_db);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return SPT_NOMEM;
    }
    pdh = (spt_dh *)pdata;
    pdh->ref = 1;
    pdata = pdata + sizeof(spt_dh);
    memcpy(pdata, new_data, DATA_SIZE);

    tmp_vec.val = pinsert->key_val;
    signpost = pinsert->signpost;
    tmp_rd = tmp_vec.rd;

    vecid_a = vec_alloc_combo(pclst, g_thrd_id, &pvec_a);
    if(pvec_a == 0)
    {
        /*yzx释放资源， 申请新块，拆分*/
        atomic64_add(1,(atomic64_t *)&g_dbg_info.oom_no_vec);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        db_free_to_buf(pclst, dataid, g_thrd_id);
        return SPT_NOMEM;
    }
    pvec_a->val = 0;
    pvec_a->flag = SPT_VEC_FLAG_DATA;
    pvec_a->pos = (pinsert->cmp_pos-1)&SPT_VEC_SIGNPOST_MASK;
    pvec_a->rd = dataid;

    if((pinsert->cmp_pos-1)-signpost > SPT_VEC_SIGNPOST_MASK)
    {
        vecid_s = vec_alloc_combo(pclst, g_thrd_id, &pvec_s);
        if(pvec_s == 0)
        {
            /*yzx释放资源， 申请新块，拆分*/
            atomic64_add(1,(atomic64_t *)&g_dbg_info.oom_no_vec);
            printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
            db_free_to_buf(pclst, dataid, g_thrd_id);
            vec_free_to_buf(pclst, vecid_a, g_thrd_id);
            return SPT_NOMEM;
        }
        pvec_s->val = 0;
        pvec_s->idx = (pinsert->cmp_pos-1)>>SPT_VEC_SIGNPOST_BIT;
        pvec_s->rd = vecid_a;
    
        //pvec_a->down = vecid_b;
    
        tmp_vec.rd = vecid_s;
        signpost = pvec_s->idx << SPT_VEC_SIGNPOST_BIT;
    }
    else
    {
        tmp_vec.rd = vecid_a;
    }

    if(tmp_vec.flag == SPT_VEC_FLAG_DATA || pinsert->endbit > pinsert->fs)
    {
        if(tmp_vec.flag == SPT_VEC_FlAG_SIGNPOST)
        {
            assert(tmp_vec.ext_sys_flg != SPT_VEC_SYS_FLAG_DATA);
        }
        else
        {
            tmp_vec.flag = SPT_VEC_FLAG_RIGHT;
        }    
        vecid_b = vec_alloc_combo(pclst, g_thrd_id, &pvec_b);
        if(pvec_b == 0)
        {
            /*yzx释放资源， 申请新块，拆分*/
            atomic64_add(1,(atomic64_t *)&g_dbg_info.oom_no_vec);
            printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
            db_free_to_buf(pclst, dataid, g_thrd_id);
            vec_free_to_buf(pclst, vecid_a, g_thrd_id);
            if(pvec_s != 0)
            {
                vec_free_to_buf(pclst, vecid_s, g_thrd_id);
            }
            return SPT_NOMEM;
        }
        pvec_b->val = 0;
        pvec_b->flag = SPT_VEC_FLAG_DATA;
        pvec_b->pos = (pinsert->fs-1)&SPT_VEC_SIGNPOST_MASK;
        pvec_b->rd = tmp_rd;
        pvec_b->down = SPT_NULL;
    
        if((pinsert->fs-1)-signpost > SPT_VEC_SIGNPOST_MASK)
        {
            vecid_s2 = vec_alloc_combo(pclst, g_thrd_id, &pvec_s2);
            if(pvec_s2 == 0)
            {
                /*yzx释放资源， 申请新块，拆分*/
                atomic64_add(1,(atomic64_t *)&g_dbg_info.oom_no_vec);
                db_free_to_buf(pclst, dataid, g_thrd_id);
                vec_free_to_buf(pclst, vecid_a, g_thrd_id);
                vec_free_to_buf(pclst, vecid_b, g_thrd_id);
                if(pvec_s != 0)
                {
                    vec_free_to_buf(pclst, vecid_s, g_thrd_id);
                }
                printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
                return SPT_NOMEM;
            }
            pvec_s2->val = 0;        
            pvec_s2->idx = (pinsert->fs-1)>>SPT_VEC_SIGNPOST_BIT;
            pvec_s2->rd = vecid_b;
        
            pvec_a->down = vecid_s2;
        }
        else
        {
            pvec_a->down = vecid_b;
        }
    }
    else
    {
        if(tmp_vec.flag == SPT_VEC_FlAG_SIGNPOST)
        {
            assert(tmp_vec.ext_sys_flg != SPT_VEC_SYS_FLAG_DATA);
        }
        pvec_a->down = tmp_rd;
    }   
    smp_mb();
    if(pinsert->key_val == atomic64_cmpxchg((atomic64_t *)pinsert->pkey_vec, 
                        pinsert->key_val, tmp_vec.val))
    {
        return SPT_OK;
    }
    else
    {
        db_free_to_buf_simple(pclst, dataid, g_thrd_id);
        vec_free_to_buf_simple(pclst, vecid_a, g_thrd_id);
        cnt = 2;
        if(pvec_b != 0)
        {
            vec_free_to_buf_simple(pclst, vecid_b, g_thrd_id);
            cnt++;
        }
        //vec_free_to_buf(*ppclst, vecid_b);
        if(pvec_s != 0)
        {
            vec_free_to_buf_simple(pclst, vecid_s, g_thrd_id);
            cnt++;
        }
        if(pvec_s2 != 0)
        {
            vec_free_to_buf_simple(pclst, vecid_s2, g_thrd_id);
            cnt++;
        }        
        ret = fill_in_rsv_list(pclst, cnt, g_thrd_id);
        if(ret == SPT_OK)
            return SPT_DO_AGAIN;
        return ret;
    }
}

/*比cur->right小时，插到下面*/
int do_insert_down_via_r(cluster_head_t *pclst, insert_info_t *pinsert, char *new_data)
{
    spt_vec tmp_vec, *pvec_a, *pvec_b, *pvec_s;
    //char *pcur_data;//*ppre_data,
    u64 signpost;
    u32 dataid, vecid_a, vecid_b, vecid_s,cnt;
    char *pdata;
    spt_dh *pdh;
    int ret;

    pvec_s = 0;
    dataid = data_alloc_combo(pclst, g_thrd_id, &pdata);
    if(pdata == 0)
    {
        /*申请新块，拆分*/
        //assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        atomic64_add(1,(atomic64_t *)&g_dbg_info.oom_no_db);
        return SPT_NOMEM;
    }
    pdh = (spt_dh *)pdata;
    pdh->ref = 1;
    pdata = pdata + sizeof(spt_dh);
    memcpy(pdata, new_data, DATA_SIZE);

    tmp_vec.val = pinsert->key_val;
    signpost = pinsert->signpost;

    vecid_b = vec_alloc_combo(pclst, g_thrd_id, &pvec_b);
    if(pvec_b == 0)
    {
        /*yzx释放资源， 申请新块，拆分*/
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        atomic64_add(1,(atomic64_t *)&g_dbg_info.oom_no_vec);
        db_free_to_buf(pclst, dataid, g_thrd_id);
        return SPT_NOMEM;
    }
    pvec_b->val = 0;
    pvec_b->flag = SPT_VEC_FLAG_DATA;
    pvec_b->pos = (pinsert->fs-1)&SPT_VEC_SIGNPOST_MASK;
    pvec_b->rd = dataid;
    pvec_b->down = SPT_NULL;

    vecid_a = vec_alloc_combo(pclst, g_thrd_id, &pvec_a);
    if(pvec_a == 0)
    {
        /*yzx释放资源， 申请新块，拆分*/
        atomic64_add(1,(atomic64_t *)&g_dbg_info.oom_no_vec);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        db_free_to_buf(pclst, dataid, g_thrd_id);
        vec_free_to_buf(pclst, vecid_b, g_thrd_id);
        return SPT_NOMEM;
    }
    pvec_a->val = 0;
//    pvec_a->flag = SPT_VEC_FLAG_DATA;
    pvec_a->pos = (pinsert->cmp_pos-1)&SPT_VEC_SIGNPOST_MASK;
//    pvec_a->rd = dataid;

    if(tmp_vec.flag == SPT_VEC_FLAG_DATA)
    {
        pvec_a->flag = SPT_VEC_FLAG_DATA;
        pvec_a->rd = tmp_vec.rd;
        tmp_vec.flag = SPT_VEC_FLAG_RIGHT;
    }
    else if(tmp_vec.flag == SPT_VEC_FlAG_SIGNPOST)
    {
        pvec_a->rd = tmp_vec.rd;
        if(tmp_vec.ext_sys_flg == SPT_VEC_SYS_FLAG_DATA)
        {
            tmp_vec.ext_sys_flg = 0;
            pvec_a->flag = SPT_VEC_FLAG_DATA;
        }
        else
        {
            pvec_a->flag = SPT_VEC_FLAG_RIGHT;
        }
    }
    else
    {
        pvec_a->flag = SPT_VEC_FLAG_RIGHT;
        pvec_a->rd = tmp_vec.rd;
    }

    
    if((pinsert->cmp_pos-1)-signpost > SPT_VEC_SIGNPOST_MASK)
    {
        vecid_s = vec_alloc_combo(pclst, g_thrd_id, &pvec_s);
        if(pvec_s == 0)
        {
            /*yzx释放资源， 申请新块，拆分*/
            atomic64_add(1,(atomic64_t *)&g_dbg_info.oom_no_vec);
            printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
            db_free_to_buf(pclst, dataid, g_thrd_id);
            vec_free_to_buf(pclst, vecid_a, g_thrd_id);
            vec_free_to_buf(pclst, vecid_b, g_thrd_id);
            return SPT_NOMEM;
        }
        pvec_s->val = 0;
        pvec_s->idx = (pinsert->cmp_pos-1)>>SPT_VEC_SIGNPOST_BIT;
        pvec_s->rd = vecid_a;

        pvec_a->down = vecid_b;

        tmp_vec.rd = vecid_s;

    }
    else if((pinsert->fs-1)-signpost > SPT_VEC_SIGNPOST_MASK)
    {
        vecid_s = vec_alloc_combo(pclst, g_thrd_id, &pvec_s);
        if(pvec_s == 0)
        {
            /*yzx释放资源， 申请新块，拆分*/
            atomic64_add(1,(atomic64_t *)&g_dbg_info.oom_no_vec);
            printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
            db_free_to_buf(pclst, dataid, g_thrd_id);
            vec_free_to_buf(pclst, vecid_a, g_thrd_id);
            vec_free_to_buf(pclst, vecid_b, g_thrd_id);
            return SPT_NOMEM;
        }
        pvec_s->val = 0;        
        pvec_s->idx = (pinsert->fs-1)>>SPT_VEC_SIGNPOST_BIT;
        pvec_s->rd = vecid_b;

        pvec_a->down = vecid_s;

        tmp_vec.rd = vecid_a;

    }
    else
    {
        pvec_a->down = vecid_b;

        tmp_vec.rd = vecid_a;        
    }
    smp_mb();
    if(pinsert->key_val == atomic64_cmpxchg((atomic64_t *)pinsert->pkey_vec, 
                        pinsert->key_val, tmp_vec.val))
    {
        return SPT_OK;
    }
    else
    {
        db_free_to_buf_simple(pclst, dataid, g_thrd_id);
        vec_free_to_buf_simple(pclst, vecid_a, g_thrd_id);
        vec_free_to_buf_simple(pclst, vecid_b, g_thrd_id);
        cnt = 3;
        if(pvec_s != 0)
        {
            vec_free_to_buf_simple(pclst, vecid_s, g_thrd_id);
            cnt ++;
        }
        ret = fill_in_rsv_list(pclst, cnt, g_thrd_id);
        if(ret == SPT_OK)
            return SPT_DO_AGAIN;
        return ret;
    }
}
/*cur->down == null, 首位为0 ，直接插到cur->down上*/
//可能是首向量的down
int do_insert_last_down(cluster_head_t *pclst, insert_info_t *pinsert, char *new_data)
{
    spt_vec tmp_vec, *pvec_a, *pvec_s;
    //char *pcur_data;//*ppre_data,
    u64 signpost;
    u32 dataid, vecid_a, vecid_s, cnt;
    char *pdata;
    spt_dh *pdh;
    int ret;

    pvec_s = 0;
    dataid = data_alloc_combo(pclst, g_thrd_id, &pdata);
    if(pdata == 0)
    {
        /*申请新块，拆分*/
        //assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        atomic64_add(1,(atomic64_t *)&g_dbg_info.oom_no_db);
        return SPT_NOMEM;
    }
    pdh = (spt_dh *)pdata;
    pdh->ref = 1;
    pdata = pdata + sizeof(spt_dh);
    memcpy(pdata, new_data, DATA_SIZE);

    tmp_vec.val = pinsert->key_val;
    signpost = pinsert->signpost;

    vecid_a = vec_alloc_combo(pclst, g_thrd_id, &pvec_a);
    if(pvec_a == 0)
    {
        /*yzx释放资源， 申请新块，拆分*/
        atomic64_add(1,(atomic64_t *)&g_dbg_info.oom_no_vec);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        db_free_to_buf(pclst, dataid, g_thrd_id);
        return SPT_NOMEM;
    }
    pvec_a->val = 0;
    pvec_a->flag = SPT_VEC_FLAG_DATA;
    pvec_a->pos = (pinsert->fs-1)&SPT_VEC_SIGNPOST_MASK;
    pvec_a->rd = dataid;
    pvec_a->down = SPT_NULL;

    if((pinsert->fs-1)-signpost > SPT_VEC_SIGNPOST_MASK)
    {
        vecid_s = vec_alloc_combo(pclst, g_thrd_id, &pvec_s);
        if(pvec_s == 0)
        {
            /*yzx释放资源， 申请新块，拆分*/
            atomic64_add(1,(atomic64_t *)&g_dbg_info.oom_no_vec);
            printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
            db_free_to_buf(pclst, dataid, g_thrd_id);
            vec_free_to_buf(pclst, vecid_a, g_thrd_id);
            return SPT_NOMEM;
        }
        pvec_s->val = 0;        
        pvec_s->idx = (pinsert->fs-1)>>SPT_VEC_SIGNPOST_BIT;
        pvec_s->rd = vecid_a;

        tmp_vec.down = vecid_s;
    }
    else
    {
        tmp_vec.down = vecid_a;
    }        
    smp_mb();
    if(pinsert->key_val == atomic64_cmpxchg((atomic64_t *)pinsert->pkey_vec, 
                        pinsert->key_val, tmp_vec.val))
    {
        return SPT_OK;
    }
    else
    {
        db_free_to_buf_simple(pclst, dataid, g_thrd_id);
        vec_free_to_buf_simple(pclst, vecid_a, g_thrd_id);
        cnt = 2;
        if(pvec_s != 0)
        {
            vec_free_to_buf_simple(pclst, vecid_s, g_thrd_id);
            cnt++;
        }
        ret = fill_in_rsv_list(pclst, cnt, g_thrd_id);
        if(ret == SPT_OK)
            return SPT_DO_AGAIN;
        return ret;
    }
}

int do_insert_up_via_d(cluster_head_t *pclst, insert_info_t *pinsert, char *new_data)
{
    spt_vec tmp_vec, *pvec_a, *pvec_s;
    //char *pcur_data;//*ppre_data,
    u64 signpost;
    u32 dataid, vecid_a, vecid_s, cnt;
    char *pdata;
    spt_dh *pdh;
    int ret;

    pvec_s = 0;
    dataid = data_alloc_combo(pclst, g_thrd_id, &pdata);
    if(pdata == 0)
    {
        /*申请新块，拆分*/
        //assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        atomic64_add(1,(atomic64_t *)&g_dbg_info.oom_no_db);
        return SPT_NOMEM;
    }
    pdh = (spt_dh *)pdata;
    pdh->ref = 1;
    pdata = pdata + sizeof(spt_dh);
    memcpy(pdata, new_data, DATA_SIZE);

    tmp_vec.val = pinsert->key_val;
    signpost = pinsert->signpost;

    vecid_a = vec_alloc_combo(pclst, g_thrd_id, &pvec_a);
    if(pvec_a == 0)
    {
        /*yzx释放资源， 申请新块，拆分*/
        atomic64_add(1,(atomic64_t *)&g_dbg_info.oom_no_vec);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        db_free_to_buf(pclst, dataid, g_thrd_id);
        return SPT_NOMEM;
    }
    pvec_a->val = 0;
    pvec_a->flag = SPT_VEC_FLAG_DATA;
    pvec_a->pos = (pinsert->fs-1)&SPT_VEC_SIGNPOST_MASK;
    pvec_a->rd = dataid;

    if((pinsert->fs-1)-signpost > SPT_VEC_SIGNPOST_MASK)
    {
        vecid_s = vec_alloc_combo(pclst, g_thrd_id, &pvec_s);
        if(pvec_s == 0)
        {
            /*yzx释放资源， 申请新块，拆分*/
            atomic64_add(1,(atomic64_t *)&g_dbg_info.oom_no_vec);
            printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
            db_free_to_buf(pclst, dataid, g_thrd_id);
            vec_free_to_buf(pclst, vecid_a, g_thrd_id);
            return SPT_NOMEM;
        }
        pvec_s->val = 0;        
        pvec_s->idx = (pinsert->fs-1)>>SPT_VEC_SIGNPOST_BIT;
        pvec_s->rd = vecid_a;

        if(tmp_vec.flag == SPT_VEC_FlAG_SIGNPOST)
        {
            pvec_a->down = tmp_vec.rd;
            tmp_vec.rd = vecid_s;
        }
        else
        {
            pvec_a->down = tmp_vec.down;
            tmp_vec.down = vecid_s;
        }        
    }
    else
    {
        if(tmp_vec.flag == SPT_VEC_FlAG_SIGNPOST)
        {
            pvec_a->down = tmp_vec.rd;
            tmp_vec.rd = vecid_a;
        }
        else
        {
            pvec_a->down = tmp_vec.down;
            tmp_vec.down = vecid_a;
        }
    }
    smp_mb();
    if(pinsert->key_val == atomic64_cmpxchg((atomic64_t *)pinsert->pkey_vec, 
                        pinsert->key_val, tmp_vec.val))
    {
        return SPT_OK;
    }
    else
    {
        db_free_to_buf_simple(pclst, dataid, g_thrd_id);
        vec_free_to_buf_simple(pclst, vecid_a, g_thrd_id);
        cnt = 2;
        if(pvec_s != 0)
        {
            vec_free_to_buf_simple(pclst, vecid_s, g_thrd_id);
            cnt ++;
        }
        ret = fill_in_rsv_list(pclst, cnt, g_thrd_id);
        if(ret == SPT_OK)
            return SPT_DO_AGAIN;
        return ret;
    }

}

/*ret:1查询不到；0删除成功；-1错误*/
int find_data(cluster_head_t *pclst, query_info_t *pqinfo)
{
    int cur_data, vecid, cmp, op;
    spt_vec *pcur, *pnext, *ppre;
    spt_vec tmp_vec, cur_vec, next_vec;
    char *pcur_data;//*ppre_data,
    u64 startbit, endbit, len, fs_pos, signpost;
    int va_old, va_new;
    u8 direction;
    int ret = SPT_NOT_FOUND;
    vec_cmpret_t cmpres;
    insert_info_t st_insert_info;
    char *pdata;
    spt_dh *pdh;
//    query_info_t *pqinfo = pquery_info;
    op = pqinfo->op;

    atomic64_add(1,(atomic64_t *)&g_dbg_info.find_data);


refind_start:
    atomic64_add(1,(atomic64_t *)&g_dbg_info.refind_start);
    pdata = pqinfo->data;
    signpost = pqinfo->signpost;
    pcur = pqinfo->start_vec;

refind_forward:
    if(pcur == NULL)
        goto refind_start;
    atomic64_add(1,(atomic64_t *)&g_dbg_info.refind_forward);
    ppre = NULL;
    cur_vec.val = pcur->val;
    startbit = signpost + cur_vec.pos + 1;
    endbit = pqinfo->endbit;
    if(cur_vec.flag == SPT_VEC_FLAG_RAW)
    {
        smp_mb();
        cur_vec.val = pcur->val;
        if(cur_vec.flag == SPT_VEC_FLAG_RAW)
        {
            if(pcur == pqinfo->start_vec)
            {
                return SPT_DO_AGAIN;
            }
            else
            {
                goto refind_start;
            }
        }
    }
    if(cur_vec.valid == SPT_VEC_INVALID 
        || cur_vec.flag == SPT_VEC_FlAG_SIGNPOST)
    {
        if(pcur == pqinfo->start_vec)
        {
            return SPT_DO_AGAIN;
        }
        else
        {
            goto refind_start;
        }
    }
    direction = SPT_DIR_START;
    cur_data = SPT_INVALID;

    fs_pos = find_fs(pdata, startbit-1, endbit-1)+1;
    
    while(startbit<endbit)
    {
        /*首位是1，与pcur_vec->right比*/
        if(fs_pos == startbit)
        {
            if(cur_vec.flag == SPT_VEC_FLAG_DATA)
            {
                len = endbit - startbit;
                //ppre = pcur;
                //pre_vec.val = cur_vec.val;
                //pcur = NULL;
            }
            else if(cur_vec.flag == SPT_VEC_FLAG_RIGHT)
            {
                pnext = (spt_vec *)vec_id_2_ptr(pclst,cur_vec.rd);
                next_vec.val = pnext->val;
                if(next_vec.flag == SPT_VEC_FLAG_RAW)
                {
                    smp_mb();
                    next_vec.val = pnext->val;
                    if(next_vec.flag == SPT_VEC_FLAG_RAW)
                    {
                        goto refind_start;
                    }
                }
                if(next_vec.valid == SPT_VEC_INVALID)
                {
                    tmp_vec.val = cur_vec.val;
                    vecid = cur_vec.rd;
                    tmp_vec.rd = next_vec.rd;
                    if(next_vec.flag == SPT_VEC_FlAG_SIGNPOST)
                    {
                        tmp_vec.flag = next_vec.ext_sys_flg;
                    }
                    else if(next_vec.flag == SPT_VEC_FLAG_DATA)
                    {
                        if(next_vec.down == SPT_NULL)
                        {
                            spt_set_data_flag(tmp_vec);
                        }
                        else
                        {
                            tmp_vec.rd = next_vec.down;
                            cur_data = SPT_INVALID;
                        }                
                    }
                    else
                    {
                        ;
                    }
                    //cur_vec.val = atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val);
                    if(cur_vec.val == atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val))//delete succ
                    {
                        ret = vec_free_to_buf(pclst, vecid, g_thrd_id);
                        if(ret != SPT_OK)
                            return ret;
                        ///TODO:取新值后再continue
                    }
                    cur_vec.val = pcur->val;
                    if(cur_vec.valid == SPT_VEC_INVALID)
                    {
                        pcur = ppre;
                        goto refind_forward;
                    }
                    continue;
                }
                
                //对于一个向量的右结点:
                    //如果是路标，不能是SPT_VEC_SYS_FLAG_DATA
                    //如果是向量，必然有down
                if(next_vec.flag == SPT_VEC_FlAG_SIGNPOST)
                {
                    if(next_vec.ext_sys_flg == SPT_VEC_SYS_FLAG_DATA)
                    {
                        tmp_vec.val = next_vec.val;
                        tmp_vec.valid = SPT_VEC_INVALID;
                        atomic64_cmpxchg((atomic64_t *)pnext, next_vec.val, tmp_vec.val);
                        //set invalid succ or not, refind from cur
                        cur_vec.val = pcur->val;
                        if((cur_vec.valid == SPT_VEC_INVALID))
                        {
                            pcur = ppre;
                            goto refind_forward;
                        }
                        continue;
                    }
                    len = (next_vec.idx << SPT_VEC_SIGNPOST_BIT ) - startbit + 1;
                }
                else
                {
                    if(next_vec.down == SPT_NULL)
                    {
                        tmp_vec.val = next_vec.val;
                        tmp_vec.valid = SPT_VEC_INVALID;
                        atomic64_cmpxchg((atomic64_t *)pnext, next_vec.val, tmp_vec.val);
                        //set invalid succ or not, refind from cur
                        cur_vec.val = pcur->val;
                        if((cur_vec.valid == SPT_VEC_INVALID))
                        {
                            pcur = ppre;
                            goto refind_forward;
                        }
                        continue;
                    }
                    len = next_vec.pos + signpost - startbit + 1;
                }
            }
            //cur是路标
            else
            {
                //pnext = (spt_vec *)vec_id_2_ptr(*ppclst,cur_vec.rd);
                //next_vec.val = pnext->val;
                //起始结点不允许是路标，因此方向只有RIGHT和DOWN
                if(direction == SPT_RIGHT)
                {
                    if(cur_vec.ext_sys_flg == SPT_VEC_SYS_FLAG_DATA)
                    {
                        tmp_vec.val = cur_vec.val;
                        tmp_vec.valid = SPT_VEC_INVALID;
                        //cur_vec.val = atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val);
                        atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val);
                        /*set invalid succ or not, refind from ppre*/
                        pcur = ppre;
                        goto refind_forward;
                    }
                    pnext = (spt_vec *)vec_id_2_ptr(pclst,cur_vec.rd);
                    next_vec.val = pnext->val;
                    if(next_vec.flag == SPT_VEC_FLAG_RAW)
                    {
                        smp_mb();
                        next_vec.val = pnext->val;
                        if(next_vec.flag == SPT_VEC_FLAG_RAW)
                        {
                            goto refind_start;
                        }
                    }
                    if(next_vec.valid == SPT_VEC_INVALID)
                    {
                        tmp_vec.val = cur_vec.val;
                        vecid = cur_vec.rd;
                        tmp_vec.rd = next_vec.rd;
                        if(next_vec.flag == SPT_VEC_FlAG_SIGNPOST)
                        {
                            tmp_vec.ext_sys_flg = next_vec.ext_sys_flg;
                        }
                        else if(next_vec.flag == SPT_VEC_FLAG_DATA)
                        {
                            if(next_vec.down == SPT_NULL)
                            {
                                tmp_vec.ext_sys_flg = SPT_VEC_SYS_FLAG_DATA;
                            }
                            else
                            {
                                tmp_vec.rd = next_vec.down;
                                cur_data = SPT_INVALID;
                            }                
                        }
                        else
                        {
                            ;
                        }
                        //cur_vec.val = atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val);
                        if(cur_vec.val == atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val))//delete succ
                        {
                            ret = vec_free_to_buf(pclst, vecid, g_thrd_id);
                            if(ret != SPT_OK)
                                return ret;
                        }
                            cur_vec.val = pcur->val;
                            if(cur_vec.valid == SPT_VEC_INVALID)
                            {
                                pcur = ppre;
                                goto refind_forward;
                            }
                            continue;
                    }
                    //对于一个路标结点:
                        //next为路标，是不合理结构。
                    if(next_vec.flag == SPT_VEC_FlAG_SIGNPOST)
                    {
                        tmp_vec.val = cur_vec.val;
                        tmp_vec.valid = SPT_VEC_INVALID;
                        cur_vec.val = atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val);
                        /*set invalid succ or not, refind from ppre*/
                        pcur = ppre;
                        goto refind_forward;
                    }
                    else
                    {
                        if(next_vec.down == SPT_NULL)
                        {
                            tmp_vec.val = next_vec.val;
                            tmp_vec.valid = SPT_VEC_INVALID;
                            atomic64_cmpxchg((atomic64_t *)pnext, next_vec.val, tmp_vec.val);
                            //set invalid succ or not, refind from cur
                            cur_vec.val = pcur->val;
                            if((cur_vec.valid == SPT_VEC_INVALID))
                            {
                                pcur = ppre;
                                goto refind_forward;
                            }
                            continue;
                        }
                        //signpost赋值以后，不能再从pre重查了。
                        signpost = cur_vec.idx << SPT_VEC_SIGNPOST_BIT;                        
                        len = next_vec.pos + signpost - startbit + 1;
                    }
                }
                else //direction = down
                {
                    if(cur_vec.rd == SPT_NULL)
                    {
                        tmp_vec.val = cur_vec.val;
                        tmp_vec.valid = SPT_VEC_INVALID;
                        cur_vec.val = atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val);
                        /*set invalid succ or not, refind from ppre*/
                        pcur = ppre;
                        goto refind_forward;
                    }
                    pnext = (spt_vec *)vec_id_2_ptr(pclst,cur_vec.rd);
                    next_vec.val = pnext->val;
                    if(next_vec.flag == SPT_VEC_FLAG_RAW)
                    {
                        smp_mb();
                        next_vec.val = pnext->val;
                        if(next_vec.flag == SPT_VEC_FLAG_RAW)
                        {
                            goto refind_start;
                        }
                    }
                    if(next_vec.valid == SPT_VEC_INVALID)
                    {
                        tmp_vec.val = cur_vec.val;
                        vecid = cur_vec.rd;
                        if(next_vec.flag == SPT_VEC_FlAG_SIGNPOST)
                        {
                            tmp_vec.rd = next_vec.rd;
                        }
                        else
                        {
                            tmp_vec.rd = next_vec.down;
                        }
                        //cur_vec.val = atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val);
                        if(cur_vec.val == atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val))//delete succ
                        {
                            ret = vec_free_to_buf(pclst, vecid, g_thrd_id);
                            if(ret != SPT_OK)
                                return ret;
                        }
                        cur_vec.val = pcur->val;
                        if(cur_vec.valid == SPT_VEC_INVALID)
                        {
                            pcur = ppre;
                            goto refind_forward;
                        }
                        continue;
                    }
                    //只处理应该遍历的路径
                    if(next_vec.flag != SPT_VEC_FlAG_SIGNPOST && next_vec.pos == 0)
                    {
                        ppre = pcur;
                        //pre_vec.val = cur_vec.val;
                        pcur = pnext;
                        cur_vec.val = next_vec.val;
                        direction = SPT_DOWN;
                        //signpost赋值以后，不能再从pre重查了。
                        signpost = cur_vec.idx << SPT_VEC_SIGNPOST_BIT;                            
                        continue;
                    }
                    else
                    {
                        switch(op){
                        case SPT_OP_FIND:
                            return ret;
                        case SPT_OP_INSERT:
                            st_insert_info.pkey_vec = pcur;
                            st_insert_info.key_val= cur_vec.val;
                            //st_insert_info.signpost = signpost;
                            ret = do_insert_dsignpost_right(pclst, &st_insert_info, pdata);
                            if(ret == SPT_DO_AGAIN)
                            {
                                goto refind_start;
                            }
                            else
                            {
                                return ret;
                            }
                            break;
                        case SPT_OP_DELETE:
                            return ret;
                        default:
                            break;
                        }
                    }
                }
            }

            if(cur_data == SPT_INVALID)//yzx
            {
                cur_data = get_data_id(pclst, pcur);
                if(cur_data >= 0 && cur_data < SPT_INVALID)
                {
                    pcur_data = db_id_2_ptr(pclst, cur_data) + sizeof(spt_dh);
                    smp_mb();
                }
                else if(cur_data == SPT_DO_AGAIN)
                    goto refind_start;
                else if(cur_data == SPT_NULL)
                {
                    switch(op){
                    case SPT_OP_FIND:
                        return ret;
                    case SPT_OP_INSERT:
                        st_insert_info.pkey_vec= pcur;
                        st_insert_info.key_val= cur_vec.val;
                        ret = do_insert_first_set(pclst, &st_insert_info, pdata);
                        if(ret == SPT_DO_AGAIN)
                        {
                            goto refind_start;
                        }
                        else
                        {
                            return ret;
                        }
                        break;
                    case SPT_OP_DELETE:
                        return ret;
                    default:
                        break;
                    }

                }
                
            }
            
            cmp = diff_identify(pdata, pcur_data, startbit-1, len, &cmpres);
            if(cmp == 0)
            {
                startbit += len;              
                /*find the same record*/
                if(startbit >= endbit)
                {
                    break;
                }
                ppre = pcur;
                pcur = pnext;
                cur_vec.val = next_vec.val;
                direction = SPT_RIGHT;
                ///TODO:startbit already >= DATA_BIT_MAX
                fs_pos = find_fs(pdata, startbit-1, endbit - startbit)+1;
                continue;
            }
            /*insert up*/
            else if(cmp > 0)
            {
                switch(op){
                case SPT_OP_FIND:
                    return ret;
                case SPT_OP_INSERT:
                    st_insert_info.pkey_vec= pcur;
                    st_insert_info.key_val= cur_vec.val;
                    st_insert_info.cmp_pos = cmpres.pos+1;
                    st_insert_info.fs = cmpres.smallfs+1;
                    st_insert_info.signpost = signpost;
                    st_insert_info.endbit = startbit+len;
                    ret = do_insert_up_via_r(pclst, &st_insert_info, pdata);
                    if(ret == SPT_DO_AGAIN)
                    {
                        goto refind_start;
                    }
                    else
                    {
                        return ret;
                    }
                    break;
                case SPT_OP_DELETE:
                    return ret;
                default:
                    break;
                }
            }
            /*insert down*/
            else
            {
                switch(op){
                case SPT_OP_FIND:
                    return ret;
                case SPT_OP_INSERT:
                    startbit +=len;
                    if(cmpres.smallfs+1 == startbit && startbit<endbit)
                    {
                        st_insert_info.fs = find_fs(pdata, startbit-1, endbit-startbit)+1;
                    }
                    else
                    {
                        st_insert_info.fs = cmpres.smallfs+1;
                    }
                    st_insert_info.pkey_vec= pcur;
                    st_insert_info.key_val= cur_vec.val;
                    st_insert_info.cmp_pos = cmpres.pos+1;
                    st_insert_info.signpost = signpost;
                    ret = do_insert_down_via_r(pclst, &st_insert_info, pdata);
                    if(ret == SPT_DO_AGAIN)
                    {
                        goto refind_start;
                    }
                    else
                    {
                        return ret;
                    }
                    break;
                case SPT_OP_DELETE:
                    return ret;
                default:
                    break;
                }
            }        
        }
        else
        {
            cur_data = SPT_INVALID;
            while(fs_pos > startbit)
            {
                //先删除invalid，但cur是路标还是向量，取next的方法不一致，故需要先区分
                if(cur_vec.flag != SPT_VEC_FlAG_SIGNPOST)
                {
                    if(cur_vec.down == SPT_NULL)
                    {
                        if(direction == SPT_RIGHT)
                        {
                            tmp_vec.val = cur_vec.val;
                            tmp_vec.valid = SPT_VEC_INVALID;
                            cur_vec.val = atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val);
                            /*set invalid succ or not, refind from ppre*/
                            pcur = ppre;
                            goto refind_forward;
                        }
                        switch(op){
                        case SPT_OP_FIND:
                            return ret;
                        case SPT_OP_INSERT:
                            st_insert_info.pkey_vec= pcur;
                            st_insert_info.key_val= cur_vec.val;
                            st_insert_info.fs = fs_pos;
                            st_insert_info.signpost = signpost;
                            ret = do_insert_last_down(pclst, &st_insert_info, pdata);
                            if(ret == SPT_DO_AGAIN)
                            {
                                goto refind_start;
                            }
                            else
                            {
                                return ret;
                            }
                            break;
                        case SPT_OP_DELETE:
                            return ret;
                        default:
                            break;
                        }
                    }
                    pnext = (spt_vec *)vec_id_2_ptr(pclst,cur_vec.down);
                    next_vec.val = pnext->val;
                    if(next_vec.flag == SPT_VEC_FLAG_RAW)
                    {
                        smp_mb();
                        next_vec.val = pnext->val;
                        if(next_vec.flag == SPT_VEC_FLAG_RAW)
                        {
                            goto refind_start;
                        }
                    }
                    if(next_vec.valid == SPT_VEC_INVALID)
                    {
                        tmp_vec.val = cur_vec.val;
                        vecid = cur_vec.down;
                        if(next_vec.flag == SPT_VEC_FlAG_SIGNPOST)
                        {
                            tmp_vec.down = next_vec.rd;
                        }
                        else
                        {
                            tmp_vec.down = next_vec.down;
                        }
                        //cur_vec.val = atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val);
                        if(cur_vec.val == atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val))//delete succ
                        {
                            ret = vec_free_to_buf(pclst, vecid, g_thrd_id);
                            if(ret != SPT_OK)
                                return ret;
                            continue;
                        }
                        else//delete fail
                        {
                            cur_vec.val = pcur->val;
                            if(cur_vec.valid == SPT_VEC_INVALID)
                            {
                                pcur = ppre;
                                goto refind_forward;
                            }
                            continue;
                        }
                    }
                    //对于一个向量，下结点为尾路标是不合法的。
                    if(next_vec.flag == SPT_VEC_FlAG_SIGNPOST)
                    {
                        if(next_vec.rd == SPT_NULL)
                        {
                            tmp_vec.val = next_vec.val;
                            tmp_vec.valid = SPT_VEC_INVALID;
                            atomic64_cmpxchg((atomic64_t *)pnext, next_vec.val, tmp_vec.val);
                            //set invalid succ or not, refind from cur
                            cur_vec.val = pcur->val;
                            if((cur_vec.valid == SPT_VEC_INVALID))
                            {
                                pcur = ppre;
                                goto refind_forward;
                            }
                            continue;
                        }
                        len = (next_vec.idx << SPT_VEC_SIGNPOST_BIT ) - startbit + 1;
                    }
                    else
                    {
                        len = next_vec.pos + signpost - startbit + 1;
                    }
                    direction = SPT_DOWN;
                }
                else//SIGN_POST
                {               
                    if(direction == SPT_RIGHT)
                    {
                        if(cur_vec.ext_sys_flg == SPT_VEC_SYS_FLAG_DATA)
                        {
                            tmp_vec.val = cur_vec.val;
                            tmp_vec.valid = SPT_VEC_INVALID;
                            cur_vec.val = atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val);
                            /*set invalid succ or not, refind from ppre*/
                            pcur = ppre;
                            goto refind_forward;
                        }
                        pnext = (spt_vec *)vec_id_2_ptr(pclst,cur_vec.rd);
                        next_vec.val = pnext->val;
                        if(next_vec.flag == SPT_VEC_FLAG_RAW)
                        {
                            smp_mb();
                            next_vec.val = pnext->val;
                            if(next_vec.flag == SPT_VEC_FLAG_RAW)
                            {
                                goto refind_start;
                            }
                        }
                        if(next_vec.valid == SPT_VEC_INVALID)
                        {
                            tmp_vec.val = cur_vec.val;
                            vecid = cur_vec.rd;
                            tmp_vec.rd = next_vec.rd;
                            if(next_vec.flag == SPT_VEC_FlAG_SIGNPOST)
                            {
                                tmp_vec.ext_sys_flg = next_vec.ext_sys_flg;
                            }
                            else if(next_vec.flag == SPT_VEC_FLAG_DATA)
                            {
                                if(next_vec.down == SPT_NULL)
                                {
                                    tmp_vec.ext_sys_flg = SPT_VEC_SYS_FLAG_DATA;
                                }
                                else
                                {
                                    tmp_vec.rd = next_vec.down;
                                    cur_data = SPT_INVALID;
                                }                
                            }
                            else
                            {
                                ;
                            }
                            //cur_vec.val = atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val);
                            if(cur_vec.val == atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val))//delete succ
                            {
                                ret = vec_free_to_buf(pclst, vecid, g_thrd_id);
                                if(ret != SPT_OK)
                                    return ret;
                            }
                                cur_vec.val = pcur->val;
                                if(cur_vec.valid == SPT_VEC_INVALID)
                                {
                                    pcur = ppre;
                                    goto refind_forward;
                                }
                                continue;
                        }

                        //只处理pos为0的右向量
                        if(next_vec.flag != SPT_VEC_FlAG_SIGNPOST 
                            && next_vec.pos == 0)
                        {
                            ppre = pcur;
                            //pre_vec.val = cur_vec.val;
                            pcur = pnext;
                            cur_vec.val = next_vec.val;
                            //direction = SPT_DOWN;
                            //signpost赋值以后，不能再从pre重查了。
                            signpost = cur_vec.idx << SPT_VEC_SIGNPOST_BIT;                            
                            continue;
                        }
                        else
                        {
                            switch(op){
                            case SPT_OP_FIND:
                                return ret;
                            case SPT_OP_INSERT:
                                signpost = cur_vec.idx << SPT_VEC_SIGNPOST_BIT;
                                st_insert_info.pkey_vec = pcur;
                                st_insert_info.key_val= cur_vec.val;
                                st_insert_info.fs = fs_pos;
                                st_insert_info.signpost = signpost;
                                ret = do_insert_rsignpost_down(pclst, &st_insert_info, pdata);
                                if(ret == SPT_DO_AGAIN)
                                {
                                    goto refind_start;
                                }
                                else
                                {
                                    return ret;
                                }
                                break;
                            case SPT_OP_DELETE:
                                return ret;
                            default:
                                break;
                            }
                        }
                    }
                    else//DOWN
                    {
                        if(cur_vec.rd == SPT_NULL)
                        {
                            tmp_vec.val = cur_vec.val;
                            tmp_vec.valid = SPT_VEC_INVALID;
                            cur_vec.val = atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val);
                            /*set invalid succ or not, refind from ppre*/
                            pcur = ppre;
                            goto refind_forward;
                        }
                        pnext = (spt_vec *)vec_id_2_ptr(pclst,cur_vec.rd);
                        next_vec.val = pnext->val;
                        if(next_vec.flag == SPT_VEC_FLAG_RAW)
                        {
                            smp_mb();
                            next_vec.val = pnext->val;
                            if(next_vec.flag == SPT_VEC_FLAG_RAW)
                            {
                                goto refind_start;
                            }
                        }
                        if(next_vec.valid == SPT_VEC_INVALID)
                        {
                            tmp_vec.val = cur_vec.val;
                            vecid = cur_vec.rd;
                            if(next_vec.flag == SPT_VEC_FlAG_SIGNPOST)
                            {
                                tmp_vec.rd = next_vec.rd;
                            }
                            else
                            {
                                tmp_vec.rd = next_vec.down;
                            }
                            //cur_vec.val = atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val);
                            if(cur_vec.val == atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val))//delete succ
                            {
                                ret = vec_free_to_buf(pclst, vecid, g_thrd_id);
                                if(ret != SPT_OK)
                                    return ret;
                            }
                                cur_vec.val = pcur->val;
                                if(cur_vec.valid == SPT_VEC_INVALID)
                                {
                                    pcur = ppre;
                                    goto refind_forward;
                                }
                                continue;
                        }
                        //对于一个路标结点:
                            //next为路标，是不合理结构。
                        if(next_vec.flag == SPT_VEC_FlAG_SIGNPOST)
                        {
                            tmp_vec.val = cur_vec.val;
                            tmp_vec.valid = SPT_VEC_INVALID;
                            cur_vec.val = atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val);
                            /*set invalid succ or not, refind from ppre*/
                            pcur = ppre;
                            goto refind_forward;
                        }
                        else
                        {
                            //signpost赋值以后，不能再从pre重查了。
                            signpost = cur_vec.idx << SPT_VEC_SIGNPOST_BIT;
                            len = next_vec.pos + signpost - startbit + 1;
                        }
                    }
                }
                /*insert*/
                if(fs_pos < startbit + len)
                {
                    switch(op){
                    case SPT_OP_FIND:
                        return ret;
                    case SPT_OP_INSERT:
                        st_insert_info.pkey_vec= pcur;
                        st_insert_info.key_val= cur_vec.val;
                        st_insert_info.fs = fs_pos;
                        st_insert_info.signpost = signpost;
                        ret = do_insert_up_via_d(pclst, &st_insert_info, pdata);
                        if(ret == SPT_DO_AGAIN)
                        {
                            goto refind_start;
                        }
                        else
                        {
                            return ret;
                        }
                        break;
                    case SPT_OP_DELETE:
                        return ret;
                    default:
                        break;
                    }
                }
                startbit += len;
                #if 0
                /*find the same record*/
                if(startbit >= endbit)
                {
                    break;
                }
                #endif
                ppre = pcur;
                pcur = pnext;
                cur_vec.val = next_vec.val;
            }
            assert(fs_pos == startbit);
        }
    }

    ret = SPT_OK;
    
    if(cur_data == SPT_INVALID)//yzx
    {
        cur_data = get_data_id(pclst, pcur);
        if(cur_data == SPT_DO_AGAIN)
            goto refind_start;
        
        pcur_data = db_id_2_ptr(pclst, cur_data) + sizeof(spt_dh);
    }
    pdh = (spt_dh *)(pcur_data - sizeof(spt_dh));

    switch(op){
    case SPT_OP_FIND:
        return ret;
    case SPT_OP_INSERT:
        while(1)
        {
            va_old = pdh->ref;
            if(va_old == 0)
            {
                goto refind_start;
            }
            else if(va_old > 0)
            {
                va_new = va_old+1;
                if(va_old == atomic_cmpxchg((atomic_t *)&pdh->ref, va_old,va_new))
                    break;
            }
            else
            {
                assert(0);
            }
        }
        return ret;
    case SPT_OP_DELETE:
        while(1)
        {
            va_old = pdh->ref;
            if(va_old == 0)
            {
                return SPT_NOT_FOUND;
            }
            else if(va_old > 0)
            {
                va_new = va_old-1;
                if(va_old == atomic_cmpxchg((atomic_t *)&pdh->ref, va_old,va_new))
                    break;
            }
            else
            {
                assert(0);
            }
        }
        if(va_old == 1)
        {
            if(pcur == (spt_vec *)vec_id_2_ptr(pclst,pclst->vec_head))
            {
                tmp_vec.val = cur_vec.val;
                tmp_vec.rd = SPT_NULL;
                if(cur_vec.val == atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val))//invalidate succ
                {
                    db_free_to_buf(pclst,cur_data, g_thrd_id);
                    return SPT_OK;
                }
                else
                {
                    atomic_add_return(1,(atomic_t *)&pdh->ref);
                    goto refind_start;
                }
            }
            tmp_vec.val = cur_vec.val;
            tmp_vec.valid = SPT_VEC_INVALID;
            if(cur_vec.val == atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val))//invalidate succ
            {
                db_free_to_buf(pclst,cur_data, g_thrd_id);
                op = SPT_OP_FIND;
                pcur = ppre;
                goto refind_forward;
            }
            else
            {
                atomic_add_return(1,(atomic_t *)&pdh->ref);
                goto refind_start;
            }
        }
        return ret;        
#if 0
        va_old = atomic_sub_return(1,(atomic_t *)&pdh->ref);
        if(va_old > 0)
            return ret;
        else if(va_old == 0)
        {
            //delete the only first set data
            if(pcur == (spt_vec *)vec_id_2_ptr(*ppclst,(*ppclst)->vec_head))
            {
                tmp_vec.val = cur_vec.val;
                tmp_vec.rd = SPT_NULL;
                //cur_vec.val = atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val);
                if(cur_vec.val == atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val))//invalidate succ
                {
                    //free cur_data
                    db_free_to_buf(*ppclst,cur_data, g_thrd_id);
                    return SPT_OK;
                }
                else
                {
                    ///TODO:并发的线程已经减为负数了怎么办?
                    atomic_add_return(1,(atomic_t *)&pdh->ref);
                    goto refind_start;
                }

            }
            tmp_vec.val = cur_vec.val;
            tmp_vec.valid = SPT_VEC_INVALID;
            //cur_vec.val = atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val);
            if(cur_vec.val == atomic64_cmpxchg((atomic64_t *)pcur, cur_vec.val, tmp_vec.val))//invalidate succ
            {
                //free cur_data
                db_free_to_buf(*ppclst,cur_data, g_thrd_id);
                op = SPT_OP_FIND;
                pcur = ppre;
                goto refind_forward;
            }
            else
            {
                atomic_add_return(1,(atomic_t *)&pdh->ref);
                goto refind_start;
            }
            return ret;
        }
        else
        {
            return SPT_NOT_FOUND;
        }
#endif        
    default:
        break;
    }
    return SPT_ERR;
}

int insert_data(cluster_head_t *pclst, char *pdata)
{
    query_info_t qinfo = {0};
    spt_vec *pvec_start;

    pvec_start = (spt_vec *)vec_id_2_ptr(pclst, pclst->vec_head);
    
    qinfo.op = SPT_OP_INSERT;
    qinfo.signpost = 0;
    qinfo.start_vec = pvec_start;
    qinfo.endbit = DATA_BIT_MAX+1;
    qinfo.data = pdata;

    return find_data(pclst,&qinfo);
}

int delete_data(cluster_head_t *pclst, char *pdata)
{
    query_info_t qinfo = {0};
    spt_vec *pvec_start;

    pvec_start = (spt_vec *)vec_id_2_ptr(pclst, pclst->vec_head);
    
    qinfo.op = SPT_OP_DELETE;
    qinfo.signpost = 0;
    qinfo.start_vec = pvec_start;
    qinfo.endbit = DATA_BIT_MAX+1;
    qinfo.data = pdata;

    return find_data(pclst,&qinfo);
}

#ifdef _BIG_ENDIAN
/*    must be byte aligned
    len:bit len, */
void find_smallfs(u8 *a, s64 len, int align, vec_cmpret_t *result)
{
    u8 uca;
    u64 bitend, ulla, fs;
    s64 lenbyte;
    u32 uia;
    u16 usa;
    
    bitend = len%8; 
    // //要比较的位不包含此位,也可理解为最后一个字节还需要比较多少位
    lenbyte = len/8;

    switch(align)
    {
        case 8:
            while(lenbyte>=8)
            {
                ulla = *(u64 *)a;a += 8;
                if(ulla != 0)
                {
                    fs = ullfind_firt_set(ulla);
                    result->smallfs += fs;
                    result->finish = 1;
                    return ;
                }
                result->smallfs += 64;
                lenbyte -= 8;
            }
        case 4:
            while(lenbyte>=4)
            {
                uia = *(u32 *)a;a += 4;
                if(uia != 0)
                {
                    fs = uifind_firt_set(uia);
                    result->smallfs += fs;
                    result->finish = 1;
                    return ;
                }
                result->smallfs += 32;
                lenbyte -= 4;
            }
        case 2:
            while(lenbyte>=2)
            {
                usa = *(u16 *)a;a += 2;
                if(usa != 0)
                {
                    fs = usfind_firt_set(usa);
                    result->smallfs += fs;
                    result->finish = 1;
                    return ;
                }
                result->smallfs += 16;
                lenbyte -= 2;
            }            
        case 1:
            while(lenbyte>=1)
            {
                uca = *a;a ++;
                if(uca != 0)
                {
                    fs = ucfind_firt_set(uca);
                    result->smallfs += fs;
                    result->finish = 1;
                    return ;
                }
                result->smallfs += 8;
                lenbyte --;
            }            
            break;
        default:
            printf("\n%s\t%d", __FUNCTION__, __LINE__);
            assert(0);
            
    }

    if(bitend)
    {
        uca = *a;
        fs = ucfind_firt_set(uca);
        if(fs >= bitend)
        {
            result->smallfs += bitend;
        }
        else
        {
            result->smallfs += fs;
        }
    }
//    result->finish = 1;
    return;
}

int align_compare(u8 *a, u8 *b, s64 len, int align, vec_cmpret_t *result)
{
    u8 uca, ucb,ucres, bitend;
    u64 ulla, ullb, ullres, fs;
    s64 lenbyte;
    u32 uia, uib, uires;
    u16 usa, usb, usres;
    int ret=0;
    u8 *c;
    
    bitend = len%8;
    lenbyte = len/8;

    switch(align)
    {
        case 8:
            while(lenbyte>=8)
            {
                ulla = *(u64 *)a;a += 8;
                ullb = *(u64 *)b;b += 8;
                
                if(ulla == ullb)
                {
                    result->pos += 64;
                    lenbyte -= 8;
                    if(ulla != 0)
                    {
                        result->flag = 0;
                    }
                    continue;
                }
                else
                {
                    ullres = ulla^ullb;
                    fs = ullfind_firt_set(ullres);
                    result->smallfs = result->pos;
                    result->pos += fs;
                //    result->flag = 0;
                    if(ulla > ullb)
                    {
                        ret = 1;
                        c = b;
                        ulla = ullb;
                    }
                    else
                    {
                        ret = -1;
                        c = a;
                    }
                    if(result->flag == 1 && ulla>>(64-fs) != 0)
                    {
                        result->flag = 0;
                    }
                    ulla = ulla << fs;
                    if(ulla != 0)
                    {
                        fs = ullfind_firt_set(ulla);
                        result->smallfs = result->pos + fs;
                        result->finish = 1;
                    }
                    else
                    {
                        result->smallfs += 64;
                        find_smallfs(c, (lenbyte<<3)+bitend, 8, result);
                        //result->finish = 1;
                    }                
                    return ret;
                }
            }
        case 4:
            while(lenbyte>=4)
            {
                uia = *(u32 *)a;a += 4;
                uib = *(u32 *)b;b += 4;
                
                if(uia == uib)
                {
                    result->pos += 32;
                    lenbyte -= 4;
                    if(uia != 0)
                    {
                        result->flag = 0;
                    }
                    continue;
                }
                else
                {
                    uires = uia^uib;
                    fs = uifind_firt_set(uires);
                    result->smallfs = result->pos;
                    result->pos += fs;

                    if(uia > uib)
                    {
                        ret = 1;
                        c = b;
                        uia = uib;
                    }
                    else
                    {
                        ret = -1;
                        c = a;
                    }
                    if(result->flag == 1 && uia>>(32-fs) != 0)
                    {
                        result->flag = 0;
                    }
                    uia = uia << fs;
                    if(uia != 0)
                    {
                        fs = uifind_firt_set(uia);
                        result->smallfs = result->pos + fs;
                        result->finish = 1;
                    }
                    else
                    {
                        result->smallfs += 32;
                        find_smallfs(c, (lenbyte<<3)+bitend, 4, result);
                        //result->finish = 1;
                    }                
                    return ret;
                }
            }

        case 2:
            while(lenbyte>=2)
            {
                usa = *(u16 *)a;a += 2;
                usb = *(u16 *)b;b += 2;
                
                if(usa == usb)
                {
                    result->pos += 16;
                    lenbyte -= 2;
                    if(usa != 0)
                    {
                        result->flag = 0;
                    }
                    continue;
                }
                else
                {
                    usres = usa^usb;
                    fs = usfind_firt_set(usres);
                    result->smallfs = result->pos;
                    result->pos += fs;

                    if(usa > usb)
                    {
                        ret = 1;
                        c = b;
                        usa = usb;
                    }
                    else
                    {
                        ret = -1;
                        c = a;
                    }
                    if(result->flag == 1 && usa>>(16-fs) != 0)
                    {
                        result->flag = 0;
                    }
                    usa = usa << fs;
                    if(usa != 0)
                    {
                        fs = usfind_firt_set(usa);
                        result->smallfs = result->pos + fs;
                        result->finish = 1;
                    }
                    else
                    {
                        result->smallfs += 16;
                        find_smallfs(c, (lenbyte<<3)+bitend, 2, result);
                        //result->finish = 1;
                    }                
                    return ret;
                }
            }            
        case 1:
            while(lenbyte>=1)
            {
                uca = *(u64 *)a;a ++;
                ucb = *(u64 *)b;b ++;
                
                if(uca == ucb)
                {
                    result->pos += 8;
                    lenbyte --;
                    if(uca != 0)
                    {
                        result->flag = 0;
                    }
                    continue;
                }
                else
                {
                    ucres = uca^ucb;
                    fs = ucfind_firt_set(ucres);
                    result->smallfs = result->pos;
                    result->pos += fs;
            
                    if(uca > ucb)
                    {
                        ret = 1;
                        c = b;
                        uca = ucb;
                    }
                    else
                    {
                        ret = -1;
                        c = a;
                    }
                    if(result->flag == 1 && uca>>(8-fs) != 0)
                    {
                        result->flag = 0;
                    }
                    uca = uca << fs;
                    if(uca != 0)
                    {
                        fs = ucfind_firt_set(uca);
                        result->smallfs = result->pos + fs;
                        result->finish = 1;
                    }
                    else
                    {
                        result->smallfs += 8;
                        find_smallfs(c, (lenbyte<<3)+bitend, 1, result);
                        //result->finish = 1;
                    }                
                    return ret;
                }
            }
            break;
        default:
            printf("\n%s\t%d", __FUNCTION__, __LINE__);
            assert(0);        
    }

    if(bitend)
    {
        uca = *a;
        ucb = *b;
        /*最后一个字节中多余的低位bit清零*/
        uca = uca >> (8-bitend) << (8-bitend);
        ucb = ucb >> (8-bitend) << (8-bitend);
        if(uca == ucb)
        {
            if(uca != 0)
            {
                result->flag = 0;
            }    
            result->pos += bitend;
            result->smallfs = result->pos;
            ret = 0;
        }
        else
        {
            ucres = uca^ucb;
            /*无关位已被抹掉，因为uca!=ucb，所以必然能在有效位中找到set bit*/
            fs = ucfind_firt_set(ucres);
            result->smallfs = result->pos;
            result->pos += fs;
            
            if(uca > ucb)
            {
                ret = 1;
                c = b;
                uca = ucb;
            }
            else
            {
                ret = -1;
                c = a;
            }
            if(result->flag == 1 && uca>>(8-fs) != 0)
            {
                result->flag = 0;
            }
            uca = uca << fs;
            if(uca != 0)
            {
                fs = ucfind_firt_set(uca);
                result->smallfs = result->pos + fs;
                result->finish = 1;
            }
            else
            {
                result->smallfs += bitend;
            //    result->finish = 1;
            }                
            
        }
    }

    return ret;

}

u64 find_fs(char *a, u64 start, u64 len)
{
    s64 align;
    u64 fs, ret, ulla;
    s64 lenbyte;
    u32 uia;
    u16 usa;
    u8 uca, bitstart, bitend;
    u8 *acstart;
    int ret = 0;

    fs = 0;
    bitstart = start%8;
    bitend = (bitstart + len)%8;
    acstart = (u8 *)a + start/8;
    lenbyte =  (bitstart + len)/8;
    ret = start;

    if(lenbyte == 0)
    {
        uca = *acstart;
        uca &= (1<<(8-bitstart))-1;
        fs = ucfind_firt_set(uca); 
        if(fs >= bitend)
        {
            /*指向范围外第一位*/
            return start + len;
        }
        else
        {
            return start + fs - bitstart;
        }
    }
    
    if(bitstart != 0)
    {
        lenbyte--;
        uca = *acstart;acstart++;
        /*第一个字节中多余的bit清零*/
        uca &= (1<<(8-bitstart))-1;
        if(uca == 0)
        {
            ret += 8 - bitstart;
        }
        else
        {
            fs = ucfind_firt_set(uca);
            ret += fs-bitstart;
            return ret;
        }
    }

    if((align=(unsigned long)acstart%8) != 0)
    {
        align = 8-align;
        if(lenbyte < align)
        {
            while(lenbyte>=1)
            {
                uca = *acstart;acstart ++;
                if(uca != 0)
                {
                    fs = ucfind_firt_set(uca);
                    ret += fs;
                    return ret;
                }
                ret += 8;
                lenbyte --;
            }
            return ret;
        }
        else
        {
            while(align>=1)
            {
                uca = *acstart;acstart ++;
                if(uca != 0)
                {
                    fs = ucfind_firt_set(uca);
                    ret += fs;
                    return ret;
                }
                ret += 8;
                align --;
            }
            lenbyte -= align;

        }
    }

    while(lenbyte>=8)
    {
        ulla = *(u64 *)acstart;acstart += 8;
        if(ulla != 0)
        {
            fs = ullfind_firt_set(ulla);
            ret += fs;
            return ret;
        }
        ret += 64;
        lenbyte -= 8;
    }
    while(lenbyte>=4)
    {
        uia = *(u32 *)acstart;acstart += 4;
        if(uia != 0)
        {
            fs = uifind_firt_set(uia);
            ret += fs;
            return ret;
        }
        ret += 32;
        lenbyte -= 4;
    }
    while(lenbyte>=2)
    {
        usa = *(u16 *)acstart;acstart += 2;
        if(usa != 0)
        {
            fs = usfind_firt_set(usa);
            ret += fs;
            return ret;
        }
        ret += 16;
        lenbyte -= 2;
    }            
    while(lenbyte>=1)
    {
        uca = *acstart;acstart ++;
        if(uca != 0)
        {
            fs = ucfind_firt_set(uca);
            ret += fs;
            return ret;
        }
        ret += 8;
        lenbyte --;
    }            
    if(bitend)
    {
        uca = *acstart;
        fs = ucfind_firt_set(uca);
        if(fs >= bitend)
        {
            ret += bitend;
        }
        else
        {
            ret += fs;
        }
    }
    return ret; 
}

#else
void find_smallfs(u8 *a, s64 len, int align, vec_cmpret_t *result)
{
     u8 uca;
     u64 bitend, ulla, fs;
     s64 lenbyte;
     u32 uia;
     u16 usa;
     int i;
     
     bitend = len%8; 
     // //要比较的位不包含此位,也可理解为最后一个字节还需要比较多少位
     lenbyte = len/8;
 
     switch(align)
     {
         case 8:
             while(lenbyte>=8)
             {
                 ulla = *(u64 *)a;
                 if(ulla != 0)
                 {
                     for(i=0;i<8;i++)
                     {
                         uca = *a;
                         if(uca != 0)
                         {
                             fs = ucfind_firt_set(uca);
                             result->smallfs += fs;
                             result->finish = 1;
                             return ;
                         }
                         result->smallfs += 8;
                         a++;
                     }
                 }
                 result->smallfs += 64;
                 lenbyte -= 8;
                 a += 8;
             }
         case 4:
             while(lenbyte>=4)
             {
                 uia = *(u32 *)a;
                 if(uia != 0)
                 {
                     for(i=0;i<4;i++)
                     {
                         uca = *a;
                         if(uca != 0)
                         {
                             fs = ucfind_firt_set(uca);
                             result->smallfs += fs;
                             result->finish = 1;
                             return ;
                         }
                         result->smallfs += 8;
                         a++;
                     }
 
                 }
                 result->smallfs += 32;
                 lenbyte -= 4;
                 a += 4;
             }
         case 2:
             while(lenbyte>=2)
             {
                 usa = *(u16 *)a;
                 if(usa != 0)
                 {
                     for(i=0;i<2;i++)
                     {
                         uca = *a;
                         if(uca != 0)
                         {
                             fs = ucfind_firt_set(uca);
                             result->smallfs += fs;
                             result->finish = 1;
                             return ;
                         }
                         result->smallfs += 8;
                         a++;
                     }
                 }
                 result->smallfs += 16;
                 lenbyte -= 2;
                 a += 2;
             }             
         case 1:
             while(lenbyte>=1)
             {
                 uca = *a;a ++;
                 if(uca != 0)
                 {
                     fs = ucfind_firt_set(uca);
                     result->smallfs += fs;
                     result->finish = 1;
                     return ;
                 }
                 result->smallfs += 8;
                 lenbyte --;
             }             
             break;
         default:
             printf("\n%s\t%d", __FUNCTION__, __LINE__);
             assert(0);
             
     }
 
     if(bitend)
     {
         uca = *a;
         fs = ucfind_firt_set(uca);
         if(fs >= bitend)
         {
             result->smallfs += bitend;
         }
         else
         {
             result->smallfs += fs;
         }
     }
 //  result->finish = 1;
     return;
 
}

int align_compare(u8 *a, u8 *b, s64 len, int align, vec_cmpret_t *result)
{
    u8 uca, ucb,ucres, bitend;
    u64 ulla, ullb, fs;
    s64 lenbyte;
    u32 uia, uib;
    u16 usa, usb;
    int ret = 0;
    u8 *c;
    
    bitend = len%8;
    lenbyte = len/8;

    switch(align)
    {
        case 8:
            while(lenbyte>=8)
            {
                ulla = *(u64 *)a;
                ullb = *(u64 *)b;
                
                if(ulla == ullb)
                {
                    result->pos += 64;
                    lenbyte -= 8;
                    if(ulla != 0)
                    {
                        result->flag = 0;
                    }
                }
                else
                {
                    goto perbyte;
                }
                a += 8;
                b += 8;
            }
        case 4:
            while(lenbyte>=4)
            {
                uia = *(u32 *)a;
                uib = *(u32 *)b;
                
                if(uia == uib)
                {
                    result->pos += 32;
                    lenbyte -= 4;
                    if(uia != 0)
                    {
                        result->flag = 0;
                    }
                }
                else
                {
                    goto perbyte;
                }
                a += 4;
                b += 4;
            }

        case 2:
            while(lenbyte>=2)
            {
                usa = *(u16 *)a;
                usb = *(u16 *)b;
                
                if(usa == usb)
                {
                    result->pos += 16;
                    lenbyte -= 2;
                    if(usa != 0)
                    {
                        result->flag = 0;
                    }
                }
                else
                {
                    goto perbyte;
                }
                a += 2;
                b += 2;
            }            
perbyte:case 1:
            while(lenbyte>=1)
            {
                uca = *(u64 *)a;
                ucb = *(u64 *)b;
                
                if(uca == ucb)
                {
                    result->pos += 8;
                    lenbyte --;
                    if(uca != 0)
                    {
                        result->flag = 0;
                    }
                }
                else
                {
                    ucres = uca^ucb;
                    fs = ucfind_firt_set(ucres);
                    result->smallfs = result->pos;
                    result->pos += fs;
            
                    if(uca > ucb)
                    {
                        ret = 1;
                        c = b;
                        uca = ucb;
                    }
                    else
                    {
                        ret = -1;
                        c = a;
                    }
                    if(result->flag == 1 && uca>>(8-fs) != 0)
                    {
                        result->flag = 0;
                    }
                    uca = uca << fs;
                    if(uca != 0)
                    {
                        fs = ucfind_firt_set(uca);
                        result->smallfs = result->pos + fs;
                        result->finish = 1;
                    }
                    else
                    {
                        result->smallfs += 8;
                        c++;
                        lenbyte--;
                        find_smallfs(c, (lenbyte<<3)+bitend, 1, result);
                        //result->finish = 1;
                    }                
                    return ret;
                }
                a++;
                b++;
            }
            break;
        default:
            printf("\n%s\t%d", __FUNCTION__, __LINE__);
            assert(0);        
    }

    if(bitend)
    {
        uca = *a;
        ucb = *b;
        /*最后一个字节中多余的低位bit清零*/
        uca = uca >> (8-bitend) << (8-bitend);
        ucb = ucb >> (8-bitend) << (8-bitend);
        if(uca == ucb)
        {
            if(uca != 0)
            {
                result->flag = 0;
            }    
            result->pos += bitend;
            result->smallfs = result->pos;
            ret = 0;
        }
        else
        {
            ucres = uca^ucb;
            /*无关位已被抹掉，因为uca!=ucb，所以必然能在有效位中找到set bit*/
            fs = ucfind_firt_set(ucres);
            result->smallfs = result->pos;
            result->pos += fs;
            
            if(uca > ucb)
            {
                ret = 1;
                c = b;
                uca = ucb;
            }
            else
            {
                ret = -1;
                c = a;
            }
            if(result->flag == 1 && uca>>(8-fs) != 0)
            {
                result->flag = 0;
            }
            uca = uca << fs;
            if(uca != 0)
            {
                fs = ucfind_firt_set(uca);
                result->smallfs = result->pos + fs;
                result->finish = 1;
            }
            else
            {
                result->smallfs += bitend;
            //    result->finish = 1;
            }                
            
        }
    }

    return ret;

}

u64 find_fs(char *a, u64 start, u64 len)
{
    s64 align;
    u64 fs, ret, ulla;
    s64 lenbyte;
    u32 uia;
    u16 usa;
    u8 uca, bitstart, bitend;
    u8 *acstart;
    int i;

//    perbyteCnt = perllCnt = perintCnt = pershortCnt = 0;
    fs = 0;
    bitstart = start%8;
    bitend = (bitstart + len)%8;
    acstart = (u8 *)a + start/8;
    lenbyte =  (bitstart + len)/8;
    ret = start;

    if(lenbyte == 0)
    {
        uca = *acstart;
        uca &= (1<<(8-bitstart))-1;
        fs = ucfind_firt_set(uca); 
        if(fs >= bitend)
        {
            /*指向范围外第一位*/
            return start + len;
        }
        else
        {
            return start + fs - bitstart;
        }
    }
    
    if(bitstart != 0)
    {
        lenbyte--;
        uca = *acstart;acstart++;
        /*第一个字节中多余的bit清零*/
        uca &= (1<<(8-bitstart))-1;
        if(uca == 0)
        {
            ret += 8 - bitstart;
        }
        else
        {
            fs = ucfind_firt_set(uca);
            ret += fs-bitstart;
            return ret;
        }
    }

    if((align=(unsigned long)acstart%8) != 0)
    {
        align = 8-align;
        if(lenbyte < align)
        {
            while(lenbyte>=1)
            {
                uca = *acstart;acstart ++;
                if(uca != 0)
                {
                    fs = ucfind_firt_set(uca);
                    ret += fs;
                    return ret;
                }
                ret += 8;
                lenbyte --;
            }
            return ret;
        }
        else
        {
            lenbyte -= align;
            while(align>=1)
            {
                uca = *acstart;acstart ++;
                if(uca != 0)
                {
                    fs = ucfind_firt_set(uca);
                    ret += fs;
                    return ret;
                }
                ret += 8;
                align --;
            }
        }
    }

    while(lenbyte>=8)
    {
        ulla = *(u64 *)acstart;
        if(ulla != 0)
        {
            for(i=0;i<8;i++)
            {
                uca = *acstart;
                if(uca != 0)
                {
                    fs = ucfind_firt_set(uca);
                    ret += fs;
                    return ret;
                }
                ret += 8;
                acstart++;
            }
        }
        ret += 64;
        lenbyte -= 8;
        acstart += 8;
    }
    while(lenbyte>=4)
    {
        uia = *(u32 *)acstart;
        if(uia != 0)
        {
            for(i=0;i<4;i++)
            {
                uca = *acstart;
                if(uca != 0)
                {
                    fs = ucfind_firt_set(uca);
                    ret += fs;
                    return ret;
                }
                ret += 8;
                acstart++;
            }
        }
        ret += 32;
        lenbyte -= 4;
        acstart += 4;
    }
    while(lenbyte>=2)
    {
        usa = *(u16 *)acstart;
        if(usa != 0)
        {
            for(i=0;i<2;i++)
            {
                uca = *acstart;
                if(uca != 0)
                {
                    fs = ucfind_firt_set(uca);
                    ret += fs;
                    return ret;
                }
                ret += 8;
                acstart++;
            }
        }
        ret += 16;
        lenbyte -= 2;
        acstart += 2;
    }            
    while(lenbyte>=1)
    {
        uca = *acstart;
        if(uca != 0)
        {
            fs = ucfind_firt_set(uca);
            ret += fs;
            return ret;
        }
        ret += 8;
        lenbyte --;
        acstart ++;
    }            
    if(bitend)
    {
        uca = *acstart;
        fs = ucfind_firt_set(uca);
        if(fs >= bitend)
        {
            ret += bitend;
        }
        else
        {
            ret += fs;
        }
    }
    return ret; 
}

#endif


#if 1
int is_startbit_set(char *a, u64 start)
{
    return 0;
}
/*bitend: 比较不包含此位*/
int bit_inbyte_cmp(u8 *a, u8 *b,u8 bitstart, u8 bitend, vec_cmpret_t *result)
{
    u8 uca, ucb, cres ,fs;
    int ret;

    uca = *a;
    ucb = *b;

    uca = (u8)(uca << bitstart) >> bitstart;
    ucb = (u8)(ucb << bitstart) >> bitstart;

    uca = (u8)(uca >> (8-bitend)) << (8-bitend);        
    ucb = (u8)(ucb >> (8-bitend)) << (8-bitend);
    
    if(uca == ucb)
    {
        result->pos += bitend - bitstart;
        if(uca != 0)
        {
            result->flag = 0;
        }
        ret = 0;
    }
    else
    {
        cres = uca^ucb;
        fs = ucfind_firt_set(cres);//fs不可能大于bitend;
        /*如果相等，返回的是下一个要比较的位；不等，返回的是哪一位出现的不等*/
        result->pos += fs-bitstart;
        if(uca > ucb)
        {
            uca = ucb;
            ret = 1;
        }
        else
        {
            ret = -1;
        }
        if(result->flag == 1 && uca>>(8-fs) != 0)
        {
            result->flag = 0;
        }
    
        /*计算较小数据相同位后的first set*/
        result->smallfs = result->pos;
        uca = uca << fs;
        bitend = bitend - fs;
        fs = ucfind_firt_set(uca);
        /*bitend后的位已经抹零*/
        if(fs == 8)
        {
            result->smallfs += bitend;
        }
        else
        {
            result->smallfs += fs;
        }
    }

    result->finish = 1;
    return ret;
}

int diff_identify(char *a, char *b,u64 start, u64 len, vec_cmpret_t *result)
{
    s64 align;
    u64 fz, fs;
    s64 lenbyte;
    u8 uca, ucb, cres,bitstart, bitend;
    u8 *acstart;
    u8 *bcstart;
    int ret = 0;

//    perbyteCnt = perllCnt = perintCnt = pershortCnt = 0;
    fs = fz = 0;
    bitstart = start%8;
    bitend = (bitstart + len)%8;
    acstart = (u8 *)a + start/8;
    bcstart = (u8 *)b + start/8;
    lenbyte =  (bitstart + len)/8;
    result->pos = start;
    result->flag = 1;
    result->finish = 0;

    if(lenbyte == 0)
        return bit_inbyte_cmp(acstart, bcstart, bitstart, bitend, result);

    if(bitstart != 0)
    {
        lenbyte--;
        uca = *acstart;acstart++;
        ucb = *bcstart;bcstart++;
        /*第一个字节中多余的bit清零*/
        uca = uca << bitstart;
        uca = uca >> bitstart;
        ucb = ucb << bitstart;
        ucb = ucb >> bitstart;
        
        if(uca == ucb)
        {
            result->pos += 8-bitstart;
            if(uca != 0)
            {
                result->flag = 0;
            }
        }
        else
        {
            cres = uca^ucb;
            fs = ucfind_firt_set(cres);
            /*这样的话，如果相等，返回的是下一个要比较的位；不等，返回的是哪一位出现的不等*/
            result->pos += fs-bitstart;
            if(uca > ucb)
            {
                ret =  1;
                acstart = bcstart;
                uca = ucb;
            }
            else
            {
                ret = -1;
            }
            result->smallfs = result->pos;
            
            if(result->flag == 1 && uca>>(8-fs) != 0)
            {
                result->flag = 0;
            }
            
            uca = uca << fs;
            if(uca == 0)
            {
                result->smallfs += 8 - fs;
            }
            else
            {
                fs = ucfind_firt_set(uca);
                result->smallfs += fs;
                result->finish = 1;
                return ret;
            }
            if((align=(unsigned long)acstart%8) != 0)
            {
                align = 8-align;
                if(lenbyte < align)
                {
                    find_smallfs(acstart, (lenbyte<<3)+bitend, 1, result);
                }
                else
                {
                    find_smallfs(acstart, align<<3, 1, result);
                    /*后一个条件等价于判断 bitend == 0 && lenbyte == align*/
                    if(result->finish != 1 && result->smallfs < start + len )
                    {
                        acstart += align;
                        lenbyte -= align;
                        find_smallfs(acstart, (lenbyte<<3)+bitend, 8, result);
                    }
                }
            }
            else
            {
                find_smallfs(acstart, (lenbyte<<3)+bitend, 8, result);
            }
            result->finish = 1;
            return ret;
        }
        
    }

    if((unsigned long)acstart%8 == (unsigned long)bcstart%8)
    {
        if((align=(unsigned long)acstart%8) != 0)
        {
            align = 8-align;
            if(lenbyte < align)
            {
                ret = align_compare(acstart, bcstart, (lenbyte<<3) + bitend, 1, result);
            }
            else
            {
                ret = align_compare(acstart, bcstart, align<<3, 1, result);
                if(result->finish == 1)
                    return ret;
                else
                {
                    acstart += align;
                    bcstart += align;
                    lenbyte -= align;
                    if(ret == 0)
                    {
                        ret = align_compare(acstart, bcstart, (lenbyte<<3) + bitend, 8, result);
                    }
                    else if(ret == 1)
                    {
                        find_smallfs(bcstart, (lenbyte<<3) + bitend, 8, result);
                    }
                    else
                    {
                        find_smallfs(bcstart, (lenbyte<<3) + bitend, 8, result);
                    }
                }
            }
        }
        else
        {
            ret = align_compare(acstart, bcstart, (lenbyte<<3) + bitend, 8, result);
        }
    }
    else if((unsigned long)acstart%4 == (unsigned long)bcstart%4)
    {
        if((align=(unsigned long)acstart%4) != 0)
        {
            align = 4-align;
            if(lenbyte < align)
            {
                ret = align_compare(acstart, bcstart, (lenbyte<<3) + bitend, 1, result);
            }
            else
            {
                ret = align_compare(acstart, bcstart, align<<3, 1, result);
                if(result->finish == 1)
                    return ret;
                else
                {
                    acstart += align;
                    bcstart += align;
                    lenbyte -= align;
                    if(ret == 0)
                    {
                        ret = align_compare(acstart, bcstart, (lenbyte<<3) + bitend, 8, result);
                    }
                    else if(ret == 1)
                    {
                        find_smallfs(bcstart, (lenbyte<<3) + bitend, 4, result);
                    }
                    else
                    {
                        find_smallfs(bcstart, (lenbyte<<3) + bitend, 4, result);
                    }
                }
            }
        }
        else
        {
            ret = align_compare(acstart, bcstart, (lenbyte<<3) + bitend, 4, result);
        }
    }

    else if((unsigned long)acstart%2 == (unsigned long)bcstart%2)
    {
        if((align=(unsigned long)acstart%2) != 0)
        {
            align = 1;
            if(lenbyte < align)
            {
                ret = align_compare(acstart, bcstart, (lenbyte<<3) + bitend, 1, result);
            }
            else
            {
                ret = align_compare(acstart, bcstart, align<<3, 1, result);
                if(result->finish == 1)
                    return ret;
                else
                {
                    acstart += align;
                    bcstart += align;
                    lenbyte -= align;
                    if(ret == 0)
                    {
                        ret = align_compare(acstart, bcstart, (lenbyte<<3) + bitend, 2, result);
                    }
                    else if(ret == 1)
                    {
                        find_smallfs(bcstart, (lenbyte<<3) + bitend, 2, result);
                    }
                    else
                    {
                        find_smallfs(bcstart, (lenbyte<<3) + bitend, 2, result);
                    }
                }
            }
        }
        else
        {
            ret = align_compare(acstart, bcstart, (lenbyte<<3) + bitend, 2, result);
        }
    }
    else
    {
        ret = align_compare(acstart, bcstart, (lenbyte<<3) + bitend, 1, result);    
    }
    result->finish = 1;
    return ret; 
}
#endif

spt_thrd_t *spt_thread_init(int thread_num)
{
    int i;
    g_thrd_h = malloc(sizeof(spt_thrd_t) + thread_num*sizeof(spt_thrd_data));
    if(g_thrd_h == NULL)
    {
        spt_debug("OOM\r\n");
        return NULL;
    }
    g_thrd_h->thrd_total= thread_num;
    g_thrd_h->black_white_map = SPT_BWMAP_ALL_ONLINE;
    g_thrd_h->online_map = 0;
    g_thrd_h->tick = 0;

    for(i=0;i<thread_num;i++)
    {
        g_thrd_h->thrd_data[i].thrd_id = i;
        g_thrd_h->thrd_data[i].vec_cnt = 0;
        g_thrd_h->thrd_data[i].vec_list_cnt = 0;
        g_thrd_h->thrd_data[i].data_cnt = 0;
        g_thrd_h->thrd_data[i].data_list_cnt = 0;
        g_thrd_h->thrd_data[i].vec_free_in = SPT_NULL;
        g_thrd_h->thrd_data[i].vec_alloc_out = SPT_NULL;
        g_thrd_h->thrd_data[i].data_free_in = SPT_NULL;
        g_thrd_h->thrd_data[i].data_alloc_out = SPT_NULL;
        g_thrd_h->thrd_data[i].rsv_cnt = 0;
        g_thrd_h->thrd_data[i].rsv_list = SPT_NULL;
        fill_in_rsv_list_simple(pgclst, SPT_PER_THRD_RSV_CNT, i);
    }
    
    return g_thrd_h;
}

void spt_atomic64_set_bit(int nr, atomic64_t *v)
{
    u64 tmp,old_val, new_val;

    tmp = 1ull<<nr;
    do
    {
        old_val = atomic64_read(v);
        new_val = tmp|old_val;

    }while(old_val != atomic64_cmpxchg(v, old_val, new_val));

    return;
}
/*返回清除后的值*/
u64 spt_atomic64_clear_bit_return(int nr, atomic64_t *v)
{
    u64 tmp,old_val, new_val;

    tmp = ~(1ull<<nr);
    do
    {
        old_val = atomic64_read(v);
        new_val = tmp&old_val;

    }while(old_val != atomic64_cmpxchg(v, old_val, new_val));

    return new_val;
}


int spt_thread_start(int thread)
{
    int cnt,ret;
    cnt = rsv_list_fill_cnt(pgclst, g_thrd_id);
    if(cnt > 0)
    {
        ret = fill_in_rsv_list(pgclst, cnt, g_thrd_id);
        if(ret != SPT_OK)
            return ret;
    }
    spt_atomic64_set_bit(thread, (atomic64_t *)&g_thrd_h->online_map);
    smp_mb();
    return SPT_OK;
}

void spt_thread_exit(int thread)
{
    u64 olmap, bwmap, new_val;

    smp_mb();
    olmap = spt_atomic64_clear_bit_return(thread,(atomic64_t *)&g_thrd_h->online_map);
    do
    {
        bwmap = atomic64_read((atomic64_t *)&g_thrd_h->black_white_map);
        if(bwmap == 0)
            return;
        new_val = bwmap & olmap;
    }while(bwmap != atomic64_cmpxchg((atomic64_t *)&g_thrd_h->black_white_map, bwmap, new_val));
    if(new_val == 0)
    {
        atomic_add(1, (atomic_t *)&g_thrd_h->tick);
        if(0 != atomic64_cmpxchg((atomic64_t *)&g_thrd_h->black_white_map, 
                                    0, SPT_BWMAP_ALL_ONLINE))
        {
            spt_debug("@@@@@@@@@@@@@@@@@@Err\r\n");
            assert(0);
        }
    }
    return;
}


void debug_print_2(u8 *p, u32 size)
{
    u8 data, k;
    int i;
    for(k = 0; k < size; k++)
    {
        data = *(p+k);
        for (i = 7; i >= 0; i--)
        {
            if(data & (1 << i))
                printf("1");
            else
                printf("0");
        }
    }
    printf("\r\n");
    return;
}

void debug_get_final_vec(cluster_head_t *pclst, spt_vec* pvec, spt_vec_f *pvec_f, 
                            u64 sp, u32 data, int direction)
{
    //下路标没有dataid，所以对于路标来说，dataid不重要
    if(pvec->flag == SPT_VEC_FlAG_SIGNPOST)
    {
        if(pvec->ext_sys_flg == SPT_VEC_SYS_FLAG_DATA)
        {
            spt_debug("found SPT_VEC_SYS_FLAG_DATA vec\r\n");
            while(1);
        }
        pvec_f->data = data;
        pvec_f->pos = pvec->idx << SPT_VEC_SIGNPOST_BIT;
        if(direction == SPT_DOWN)
        {
            pvec_f->down = pvec->rd;
            pvec_f->right = SPT_INVALID;
        }
        else
        {
            pvec_f->right = pvec->rd;
            pvec_f->down = SPT_INVALID;
        }
        return;
    }

    pvec_f->down = pvec->down;;
    pvec_f->pos = pvec->pos + sp;
    if(pvec->flag == SPT_VEC_FLAG_DATA)
    {
        pvec_f->data = pvec->rd;
        pvec_f->right = SPT_NULL;
        return;
    }
    pvec_f->right = pvec->rd;
    if(data != SPT_INVALID)
    {
        pvec_f->data = data;
    }
    else
    {
        pvec_f->data = get_data_id(pclst, pvec);
        if(pvec_f->data == SPT_DO_AGAIN)
        {
            spt_debug("invalid found\r\n");
            while(1);
        }
    }
    return;
}

void debug_vec_print(spt_vec_f *pvec_f, int vec_id)
{
    printf("{down:%d, right:%d, pos:%lld, data:%d}[%d]\r\n",pvec_f->down, pvec_f->right, pvec_f->pos, pvec_f->data, vec_id);
    return;
}

void debug_id_vec_print(cluster_head_t *pclst, int id)
{
    return;
}

void debug_data_print(char *pdata)
{
    int i;
    u8 *p = (u8 *)pdata;
    for(i = 0; i < DATA_SIZE; i++,p++)
    {
        if(i%8 == 0)
        {
            printf("\r\n");
        }
        printf("%02x ", *p);
    }
    printf("\r\n");
    return;
}
travl_info *debug_travl_stack_pop(spt_stack *p_stack)
{
    return (travl_info *)spt_stack_pop(p_stack);
}


void debug_travl_stack_destroy(spt_stack *p_stack)
{
    travl_info *node;
    while(!spt_stack_empty(p_stack))
    {
        node = debug_travl_stack_pop(p_stack);
        free(node);
    }
    spt_stack_destroy(p_stack);
    return;
}


void debug_travl_stack_push(spt_stack *p_stack, spt_vec_f *pvec_f, long long signpost)
{
    travl_info *node;
    node = (travl_info *)malloc(sizeof(travl_info));
    if(node == NULL)
    {
        printf("\r\nOUT OF MEM        %d\t%s", __LINE__, __FUNCTION__);
        debug_travl_stack_destroy(p_stack);
        return;
    }
    node->signpost = signpost;
    node->vec_f = *pvec_f;
    spt_stack_push(p_stack,node);
    return;
}


void debug_cluster_travl(cluster_head_t *pclst)
{
    spt_stack stack = {0};
    spt_stack *pstack = &stack;
    //u8 data[DATA_SIZE] = {0};
    int cur_data, cur_vecid;
    spt_vec *pcur;
    spt_vec_f st_vec_f;
    spt_dh *pdh;
    char *pcur_data = NULL;
    u64 signpost;
    travl_info *pnode;
    u16 rank;

    signpost = 0;
    spt_stack_init(pstack, 100);
    cur_data = SPT_INVALID;

    cur_vecid = pclst->vec_head;
    pcur = (spt_vec *)vec_id_2_ptr(pclst, pclst->vec_head);

    debug_get_final_vec(pclst, pcur, &st_vec_f, signpost, cur_data, SPT_RIGHT);
    if(pcur->down == SPT_NULL && pcur->rd == SPT_NULL)
    {
        printf("cluster is null\r\n");
        debug_travl_stack_destroy(pstack);
        return;
    }
    rank = pclst->used_dblk_cnt;
    cur_data = st_vec_f.data;
    if(cur_data != SPT_INVALID)
    {
        pcur_data = db_id_2_ptr(pclst, cur_data) + sizeof(spt_dh);
    }
    debug_vec_print(&st_vec_f, cur_vecid);
#if 0
    if(st_vec_r.pos == 0)
    {
        printf("only one vec in this cluster\r\n");
        debug_vec_print(&st_vec_r, cur_vec);
        debug_data_print(pcur_data);
        return;        
    }
#endif
    debug_travl_stack_push(pstack,&st_vec_f, signpost);

    while (1)
    {
        if(pcur->flag != SPT_VEC_FLAG_DATA)
        {
            cur_vecid = st_vec_f.right;
            pcur = (spt_vec *)vec_id_2_ptr(pclst, cur_vecid);
            debug_get_final_vec(pclst, pcur, &st_vec_f, signpost, cur_data, SPT_RIGHT);
            debug_vec_print(&st_vec_f, cur_vecid);
            if(pcur->flag == SPT_VEC_FlAG_SIGNPOST)
            {
                signpost = st_vec_f.pos;
                
                cur_vecid = st_vec_f.right;
                pcur = (spt_vec *)vec_id_2_ptr(pclst, cur_vecid);
                debug_get_final_vec(pclst, pcur, &st_vec_f, signpost, cur_data, SPT_RIGHT);
                debug_vec_print(&st_vec_f, cur_vecid);                
                if(pcur->flag == SPT_VEC_FlAG_SIGNPOST)
                {
                    spt_debug("double signpost vec found!!\r\n");
                    while(1);
                }
            }
            debug_travl_stack_push(pstack,&st_vec_f, signpost);
            #if 0
            pos = st_vec_f.pos;
            if(pos != 0)
                spt_bit_cpy(data, pcur_data, bit, pos);
            else
                spt_bit_cpy(data, pcur_data, bit, DATA_BIT_MAX - bit);
            bit += st_vec_r.pos;
            #endif
        }
        else
        {            
            //debug_vec_print(&st_vec_r, cur_vec);
            //单个数据遍历完
            //spt_bit_cpy(data, pcur_data, bit, DATA_BIT_MAX - bit);
            //此时data应该等于插入的某个数
            //debug_vec_print(&st_vec_f, cur_vecid);
            if(pcur_data != NULL)//head->right为空时，pcur_data等于空
            {
                #if 0
                if(memcmp(data, pcur_data, DATA_SIZE)!=0)
                {
                    printf("\r\nErr    %d\t%s", __LINE__, __FUNCTION__);
                }
                #endif
                pdh = (spt_dh *)(pcur_data-sizeof(spt_dh));
                pdh->rank = rank;
                rank--;
                debug_data_print(pcur_data);
            }
            
            if(spt_stack_empty(pstack))
            {
                break;
            }
            while(1)
            {
                pnode = debug_travl_stack_pop(pstack);
                if(pnode == (travl_info *)-1)
                {
                    printf("\r\n");
                    debug_travl_stack_destroy(pstack);
                    return;
                }
                signpost = pnode->signpost;
                if(pnode->vec_f.down != SPT_NULL)
                {
                    cur_vecid = pnode->vec_f.down;
                    pcur = (spt_vec *)vec_id_2_ptr(pclst, cur_vecid);
                    debug_get_final_vec(pclst, pcur, &st_vec_f, signpost, SPT_INVALID, SPT_DOWN);
                    //debug_vec_print(&st_vec_f, cur_vecid);
                    
                    if(pcur->flag == SPT_VEC_FlAG_SIGNPOST)
                    {
                        signpost = st_vec_f.pos;
                        cur_vecid = pnode->vec_f.down;
                        pcur = (spt_vec *)vec_id_2_ptr(pclst, cur_vecid);
                        debug_get_final_vec(pclst, pcur, &st_vec_f, signpost, SPT_INVALID, SPT_DOWN);
                        if(pcur->flag == SPT_VEC_FlAG_SIGNPOST)
                        {
                            spt_debug("double signpost vec found!!\r\n");
                            while(1);
                        }
                    }
                    cur_data = st_vec_f.data;
                    pcur_data = (char *)db_id_2_ptr(pclst, cur_data) + sizeof(spt_dh);
                    printf("\r\n@@data[%p],bit:%lld\r\n", pcur_data, st_vec_f.pos);
                    debug_vec_print(&st_vec_f, cur_vecid);
                    debug_travl_stack_push(pstack,&st_vec_f, signpost);
                    free(pnode);
                    break;
                }
                free(pnode);
            }
        }
    }
    debug_travl_stack_destroy(pstack);
    return;
}


#if 0
void debug_vec_print(spt_vec_r *pvec_r, int vec_id)
{
    printf("{down:%d, right:%d, pos:%lld, data:%d}[%d]\r\n",pvec_r->down, pvec_r->right, pvec_r->pos, pvec_r->data, vec_id);
    return;
}

void debug_id_vec_print(cluster_head_t *pclst, int id)
{
    return;
}

void debug_data_print(u8 *pdata)
{
    int i;
    u8 *p = pdata;
    for(i = 0; i < DATA_SIZE; i++,p++)
    {
        if(i%8 == 0)
        {
            printf("\r\n");
        }
        printf("%02x ", *p);
    }
    printf("\r\n");
    return;
}
void debug_id_data_print(cluster_head_t *pclst, int id)
{
    u8 *pdata;

    pdata = (u8 *)db_id_2_ptr(pclst, id);
    debug_data_print(pdata);
    return;
}

travl_info *debug_travl_stack_pop(spt_stack *p_stack)
{
    return (travl_info *)spt_stack_pop(p_stack);
}

void debug_travl_stack_destroy(spt_stack *p_stack)
{
    travl_info *node;
    while(!spt_stack_empty(p_stack))
    {
        node = debug_travl_stack_pop(p_stack);
        free(node);
    }
    spt_stack_destroy(p_stack);
    return;
}

void debug_travl_stack_push(spt_stack *p_stack, spt_vec *pvec_r, int 
direct)
{
    travl_info *node;
    node = (travl_info *)malloc(sizeof(travl_info));
    if(node == NULL)
    {
        printf("\r\nOUT OF MEM        %d\t%s", __LINE__, __FUNCTION__);
        debug_travl_stack_destroy(p_stack);
        return;
    }
    node->direction = direct;
    node->vec_r = *pvec_r;
    spt_stack_push(p_stack,(int)node);
    return;
}


void debug_cluster_travl(cluster_head_t *pclst)
{
    spt_stack stack = {0};
    spt_stack *pstack = &stack;
    u8 data[DATA_SIZE] = {0};
    int cur_vec, cur_data;
    spt_vec *pcur_vec;
    u8 *pcur_data = NULL;
    u64 bit=0, pos;
    spt_vec st_vec_r;
    travl_info *pnode;
    u16 rank, *prsv;
 //   int count=0;

    spt_stack_init(pstack, 100);

    cur_vec = pclst->vec_head;
    pcur_vec = (spt_vec *)vec_id_2_ptr(pclst, cur_vec);
    get_final_vec(pclst, pcur_vec, &st_vec_r, -1);    
    
    if(st_vec_r.down == SPT_NULL && st_vec_r.right == SPT_NULL)
    {
        printf("cluster is null\r\n");
        return;
    }
    rank = pclst->used_dblk_cnt;
    cur_data = st_vec_r.data;//没有首位为1的data时，cur_data等于-1，bug
    /*此时right肯定为空*/
    if(cur_data != -1)
    {
        pcur_data = (u8 *)db_id_2_ptr(pclst, cur_data);    
    }
#if 0
    if(st_vec_r.pos == 0)
    {
        printf("only one vec in this cluster\r\n");
        debug_vec_print(&st_vec_r, cur_vec);
        debug_data_print(pcur_data);
        return;        
    }
#endif
    debug_travl_stack_push(pstack,&st_vec_r, SPT_RIGHT);

    while (1)
    {
        if(st_vec_r.right != SPT_NULL)
        {
            cur_vec = st_vec_r.right;
            pcur_vec = (spt_vec *)vec_id_2_ptr(pclst, cur_vec);
            get_final_vec(pclst, pcur_vec, &st_vec_r, cur_data);
            debug_vec_print(&st_vec_r, cur_vec);
            debug_travl_stack_push(pstack,&st_vec_r, SPT_RIGHT);
            pos = st_vec_r.pos;
            if(pos != 0)
                spt_bit_cpy(data, pcur_data, bit, pos);
            else
                spt_bit_cpy(data, pcur_data, bit, DATA_BIT_MAX - bit);
            bit += st_vec_r.pos;
        }
        else
        {            
            //debug_vec_print(&st_vec_r, cur_vec);
            //单个数据遍历完
            //spt_bit_cpy(data, pcur_data, bit, DATA_BIT_MAX - bit);
            //此时data应该等于插入的某个数
            if(pcur_data != NULL)//head->right为空时，pcur_data等于空
            {
                if(memcmp(data, pcur_data, DATA_SIZE)!=0)
                {
                    printf("\r\nErr    %d\t%s", __LINE__, __FUNCTION__);
                }
                prsv = (u16 *)(pcur_data + DATA_SIZE);
                *prsv = rank;
                rank--;
                debug_data_print(data);

                if(st_vec_r.down != SPT_NULL)
                {
                    printf("\r\nErr    %d\t%s", __LINE__, __FUNCTION__);
                }
            }
            
            if(spt_stack_empty(pstack))
            {
                break;
            }
            while(1)
            {
                pnode = debug_travl_stack_pop(pstack);
                if(pnode == (travl_info *)-1)
                {
                    printf("\r\n");
                    return;
                }
                if(pnode->direction == SPT_RIGHT && pnode->vec_r.down != SPT_NULL)
                {
                    pnode->direction = SPT_DOWN;
                    //不用申请内存，直接入栈
                    spt_stack_push(pstack,(int)pnode);
                    //spt_bit_clear(data, bit - pnode->vec_r.pos, pnode->vec_r.pos);
                    cur_vec = pnode->vec_r.down;
                    pcur_vec = (spt_vec *)vec_id_2_ptr(pclst, cur_vec);
                    get_final_vec(pclst, pcur_vec, &st_vec_r, -1);
                    cur_data = st_vec_r.data;
                    pcur_data = (u8 *)db_id_2_ptr(pclst, cur_data);
                    printf("\r\n@@data[%p],bit:%lld\r\n", pcur_data, bit);
                    debug_vec_print(&st_vec_r, cur_vec);
                    debug_travl_stack_push(pstack,&st_vec_r, SPT_RIGHT);                    
                    pos = st_vec_r.pos;
                    if(pos != 0)
                        spt_bit_cpy(data, pcur_data, bit, pos);
                    else
                        spt_bit_cpy(data, pcur_data, bit, DATA_BIT_MAX - bit);
                    bit += st_vec_r.pos;
                    
                    break;
                }
                else
                {
                    bit -= pnode->vec_r.pos;
                    free(pnode);
                }
            }
        }
    }
    
}

void debug_outer_q_in(travl_q *q, travl_outer_info *pnode)
{
    if(q->tail == NULL)
    {
        q->head = q->tail = pnode;
        return;
    }
    q->tail->next = pnode;
    q->tail = pnode;
    return;
}

travl_outer_info *debug_outer_q_de(travl_q *q)
{
    travl_outer_info *p;
    if(q->head == NULL)
    {
        return NULL;
    }
    p = q->head;
    if(q->head == q->tail)
    {
        q->head = q->tail = NULL;
    }
    else
    {
        q->head = p->next;
    }
    return p;
}
void debug_outer_q_destroy(travl_q *q)
{
    return;
}

void debug_cluster_outer_prior_travl(cluster_head_t * pclst)
{
    int cur_vec, cur_data;
    spt_vec *pcur_vec;
    u8 *pcur_data;
    u64 bit=0;
    spt_vec st_vec_r;
    travl_outer_info *pnode;
    u16 rank;
    travl_q tq = {0};
    FILE *fp;
    char buf[1024]={0};
    int j = 0;

    cur_vec = pclst->vec_head;
    pcur_vec = (spt_vec *)vec_id_2_ptr(pclst, cur_vec);
    get_final_vec(pclst, pcur_vec, &st_vec_r, -1);    

    if(st_vec_r.down == SPT_NULL && st_vec_r.right == SPT_NULL)
    {
        printf("cluster is null\r\n");
        return;
    }
    
    fp = fopen("file.txt","w+");
    cur_data = st_vec_r.data;
    /*没有首位是1的data*/
    if(cur_data == -1)
    {
        cur_vec = st_vec_r.down;
    }

    while(cur_vec != SPT_NULL)
    {
        pcur_vec = (spt_vec *)vec_id_2_ptr(pclst, cur_vec);
        get_final_vec(pclst, pcur_vec, &st_vec_r, -1);
        cur_data = st_vec_r.data;
        pcur_data = (u8 *)db_id_2_ptr(pclst, cur_data);
        rank = *(u16 *)(pcur_data + DATA_SIZE);
        
        if(st_vec_r.right != SPT_NULL)
        {
            bit += st_vec_r.pos;
            pnode = (travl_outer_info *)malloc(sizeof(travl_outer_info));
            if(pnode == NULL)
            {
                printf("\r\nOUT OF MEM        %d\t%s", __LINE__, __FUNCTION__);
                debug_outer_q_destroy(&tq);
                fclose(fp);
                return;
            }
            pnode->next = NULL;
            pnode->pre = cur_vec;
            pnode->cur = st_vec_r.right;
            pnode->data = cur_data;
            pnode->xy_pre.x = rank;
            pnode->xy_pre.y = bit;
            debug_outer_q_in(&tq, pnode);
        }
        else
        {
            bit = DATA_BIT_MAX;
        }       
        j += sprintf(buf+j, "%lld\t%d\r\n", bit, rank);
        
        cur_vec = st_vec_r.down;
    }
    j += sprintf(buf+j, "\r\n");
    fwrite(buf, j, 1, fp);

    while(NULL != (pnode = debug_outer_q_de(&tq)))
    {
        j = 0;
        rank = pnode->xy_pre.x;
        bit = pnode->xy_pre.y;
        j += sprintf(buf+j, "%lld\t%d\r\n", bit, rank);
        cur_vec = pnode->cur;
        cur_data = pnode->data;
        free(pnode);
        while(cur_vec != SPT_NULL)
        {
            pcur_vec = (spt_vec *)vec_id_2_ptr(pclst, cur_vec);
            get_final_vec(pclst, pcur_vec, &st_vec_r, -1);
            cur_data = st_vec_r.data;
            pcur_data = (u8 *)db_id_2_ptr(pclst, cur_data);
            rank = *(u16 *)(pcur_data + DATA_SIZE);
            
            if(st_vec_r.right != SPT_NULL)
            {
                bit += st_vec_r.pos;
                pnode = (travl_outer_info *)malloc(sizeof(travl_outer_info));
                if(pnode == NULL)
                {
                    printf("\r\nOUT OF MEM        %d\t%s", __LINE__, __FUNCTION__);
                    debug_outer_q_destroy(&tq);
                    fclose(fp);
                    return;
                }
                pnode->next = NULL;
                pnode->pre = cur_vec;
                pnode->cur = st_vec_r.right;
                pnode->data = cur_data;
                pnode->xy_pre.x = rank;
                pnode->xy_pre.y = bit;
                debug_outer_q_in(&tq, pnode);
            }
            else
            {
                bit = DATA_BIT_MAX;
            }       
            j += sprintf(buf+j, "%lld\t%d\r\n", bit, rank);
            
            cur_vec = st_vec_r.down;
        }
        
        j += sprintf(buf+j, "\r\n");
        fwrite(buf, j, 1, fp);
        
    }
    fclose(fp);
    return;
}

char *test_read_test_case(char *file_name)
{
    FILE *fp;
    char *data_buf;
    u64 flen;

    fp=fopen(file_name,"r");
    if(NULL==fp)
    {
        return -1;
    }
    fseek(fp,0L,SEEK_END);
    flen=ftell(fp); 

    data_buf = (char *)malloc(flen);
    if(data_buf == NULL)
    {
        fclose(fp);
        return 0;
    }
    fread(data_buf, flen, 1, fp);
    fclose(fp);
    return data_buf;
}
#endif

char *acmp;
char *bbcmp;
vec_cmpret_t *rcmp;

int test_init()
{
    int i=0;

    *(acmp+i)=0x11;i++;
    *(acmp+i)=0x12;i++;
    *(acmp+i)=0x13;i++;
    *(acmp+i)=0x14;i++;
    *(acmp+i)=0x15;i++;
    *(acmp+i)=0x16;i++;
    *(acmp+i)=0x17;i++;
    *(acmp+i)=0x18;i++;

    *(acmp+i)=0xf0;i++;
    *(acmp+i)=0xf1;i++;
    *(acmp+i)=0xf2;i++;
    *(acmp+i)=0xf3;i++;
    *(acmp+i)=0xf4;i++;
    *(acmp+i)=0xf5;i++;
    *(acmp+i)=0xf6;i++;
    *(acmp+i)=0xf7;i++;

    memcpy(bbcmp, acmp, 16);

    *(bbcmp+8)=0x0;
    *(bbcmp+9)=0x0;
    *(bbcmp+10)=0x0;
    *(bbcmp+11)=0x0;
    *(bbcmp+12)=0x6;

    return 0;
}

#if 0
int main()
{
    //    int flag = 1;
    //    spt_vec_t_r tmp_vec_r={5,10,4,65535};
    //    spt_vec_t *pvec_a;0x123406789abcedf; 0x12345678abcedf
    //    int vec_a;
        unsigned long long indata = 0x123406789abcedf;
    //    unsigned long long deldata;

#if 1
    acmp = (char *)malloc(4096);
    bbcmp = (char *)malloc(4096);
    rcmp = (vec_cmpret_t *)malloc(sizeof(vec_cmpret_t));
    if(acmp == NULL || bbcmp == NULL || rcmp == NULL)
    {
        printf("\r\n@@@@@@@@@@");
        return -1;
    }

    test_init();
#endif


    pgclst = cluster_init();
    if(pgclst == NULL)
    {
        assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return 1;
    }

    while(1)
    {
      //  sleep(2);
        insert_data(&pgclst,(char *)&indata);
        debug_cluster_travl(pgclst);        
    }

    return 0;
}
#else
#include <stdio.h>

u64 gdata;
query_info_t g_qinfo = {0};
void *spt_thread(void *arg)
{
    cpu_set_t mask;
    int i,j, ret;
//    u64 order_id;
//    spt_vec *pvec;
    //spt_vec *pvec, *pid_2_ptr;
    //spt_dh *pdb, *pdbid_2_ptr;
    
//    u64 start, end;
    i = (long)arg;

    g_thrd_id = i;
    CPU_ZERO(&mask); 
    CPU_SET(i,&mask);

    if (sched_setaffinity(0, sizeof(mask), &mask) == -1)
    {
        printf("warning: could not set CPU affinity, continuing...\n");
    }
#if   0  
    gdata = 0xee;
    pvec = (spt_vec *)vec_id_2_ptr(pgclst, pgclst->vec_head);
    g_qinfo.op = SPT_OP_INSERT;
    g_qinfo.signpost = 0;
    g_qinfo.start_vec = pvec;
    g_qinfo.endbit = DATA_BIT_MAX+1;
    g_qinfo.data = (char *)&gdata;
#endif
    printf("writefn%d,start\n",i);
//    spt_thread_start();
#if 0
    buf = (int *)malloc(4*1000000);
    if(buf == NULL)
        spt_debug("OOM\r\n");

    for(order_id=0;order_id<100;order_id ++)
    {
        #if 0
        for(i=0;i<10000;i++)
        {
            vec = vec_alloc_combo(&pgclst, &pvec);
            if(pvec != NULL)
            {
                pid_2_ptr = (spt_vec *)vec_id_2_ptr(pgclst, vec);
                if(pid_2_ptr != pvec)
                {
                    spt_debug("vec:%d pvec:%p pid_2_ptr:%p\r\n", vec,pvec,pid_2_ptr);
                }
                buf[i] = vec;
            }

        }
        for(i=0;i<10000;i++)
        {
            vec = buf[i];
            vec_free_to_buf(pgclst, vec, g_thrd_id);
        }
        #else
        for(i=0;i<100000;i++)
        {
            db = db_alloc(&pgclst, (char **)&pdb);
            if(pdb != NULL)
            {
                pdbid_2_ptr = (spt_dh *)db_id_2_ptr(pgclst, db);
                if(pdbid_2_ptr != pdb)
                {
                    spt_debug("db:%d pdb:%p pdbid_2_ptr:%p\r\n", db,pdb,pdbid_2_ptr);
                }
                buf[i] = db;
            }

        }
        for(i=0;i<100000;i++)
        {
            db = buf[i];
            db_free_to_buf(pgclst, db);
        }

        #endif
    }
#endif

    for(i=0;i<1000000;i++)
    {
        while(SPT_OK != spt_thread_start(g_thrd_id))
        {
            sleep(1);
        }
        for(j=0;j<DBG_DATA_NUM;j++)
        {
            if(rsv_list_fill_cnt(pgclst, g_thrd_id) > 0)
            {
                spt_debug("@@@@@@@@@@@\r\n");
                while(1);
            }
            ret = insert_data(pgclst, (char *)&g_data[j]);
            if(ret == SPT_OK)
            {
                atomic64_add(1,(atomic64_t *)&g_dbg_info.insert_ok);
            }
            else
            {
                atomic64_add(1,(atomic64_t *)&g_dbg_info.insert_fail);
                if(ret == SPT_WAIT_AMT)
                {
                    atomic64_add(1,(atomic64_t *)&g_dbg_info.ret_wait);
                    goto thread_exit;
                }
                if(ret == SPT_NOMEM)
                {
                    atomic64_add(1,(atomic64_t *)&g_dbg_info.ret_nomem);
                    goto thread_exit;
                }                
            }
        }
        #if 1
        for(j=0;j<DBG_DATA_NUM;j++)
        {
            if(rsv_list_fill_cnt(pgclst, g_thrd_id) > 0)
            {
                spt_debug("@@@@@@@@@@@\r\n");
                while(1);
            }        
            ret = delete_data(pgclst, (char *)&g_data[j]);
            if(ret == SPT_OK)
            {
                atomic64_add(1,(atomic64_t *)&g_dbg_info.delete_ok);
            }
            else
            {
                atomic64_add(1,(atomic64_t *)&g_dbg_info.delete_fail);
                if(ret == SPT_WAIT_AMT)
                {
                    atomic64_add(1,(atomic64_t *)&g_dbg_info.ret_wait);
                    goto thread_exit;
                }
                if(ret == SPT_NOMEM)
                {
                    atomic64_add(1,(atomic64_t *)&g_dbg_info.ret_nomem);
                    goto thread_exit;
                }                
            }
        }
        #endif
thread_exit:
        spt_thread_exit(g_thrd_id);
    }
//    spt_thread_exit(g_thrd_id);
    spt_debug("test%d done\r\n", g_thrd_id);
    while(1)
    {

    }
    return 0;    
}


int main()
{
    long num, thread_num;
    int err;
    pthread_t ntid;
    cpu_set_t mask;

    CPU_ZERO(&mask); 
    thread_num = 4;

    pgclst = cluster_init(thread_num);
    if(pgclst == NULL)
    {
        spt_debug("cluster_init err\r\n");
        return 1;
    }

    g_thrd_h = spt_thread_init(thread_num);
    if(g_thrd_h == NULL)
    {
        spt_debug("spt_thread_init err\r\n");
        return 1;
    }
    //test_vec_alloc_n_times(&pgclst);

#if 1
    for(num=0; num <thread_num; num++)
    {
        err = pthread_create(&ntid, NULL, spt_thread, (void *)num);
        if (err != 0)
            printf("can't create thread: %s\n", strerror(err));
    }
#endif

    while(1)
    {
        sleep(1);
    }
    
    return 0;
}

#endif



