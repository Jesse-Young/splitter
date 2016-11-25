#include <stdio.h>
#include <malloc.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>
#include <vector.h>
#include <chunk.h>
#include <unistd.h>

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
    p_stack->p_bottom = (int *)malloc(size * sizeof(int));
   
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

void spt_stack_push(spt_stack *p_stack, int value)
{
    spt_stack *p_tmp = (spt_stack *)p_stack->p_bottom;
    
    if (spt_stack_full(p_stack))
    {      
        p_stack->p_bottom = 
        (int *)realloc(p_stack->p_bottom, 2*p_stack->stack_size*sizeof(int));
        
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

int spt_stack_pop(spt_stack *p_stack)
{
    int value = 0;
   
    if (spt_stack_empty(p_stack))
    {
        return -1;
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
/*��ȡdataid��Ҫ���ұ�������������������invalid��㼴���д�����
    ����ʱ����invalid����preҲ��Ϊinvalid����Ҫ������ǰ���ݣ�ֱ�ӷ���ʧ��*/
int get_data_id(cluster_head_t *pclst, spt_vec_t* pvec)
{
    spt_vec_t *ppre = NULL;
    spt_vec_t tmp_vec, cur_vec, pre_vec;
    u64 ret;
    u32 vecid_cur;
    
    cur_vec.val = pvec->val;

    while(1)
    {
        if(cur_vec.valid == SPT_VEC_INVALID)
        {
            if(ppre == NULL)
            {
                return SPT_INVALID;
            }
            tmp_vec.val = pre_vec.val;
            /*һ���Ǵ�right����������*/
            if(cur_vec.flag == SPT_VEC_FlAG_SIGNPOST)
            {
                if(cur_vec.ext_sys_flg == SPT_VEC_SYS_FLAG_DATA)
                {
                    spt_set_data_flag(tmp_vec);
                }
                tmp_vec.rd = cur_vec.rd;
            }
            else if(cur_vec.flag == SPT_VEC_FLAG_DATA)
            {
                if(cur_vec.down == SPT_NULL)
                {
                    spt_set_data_flag(tmp_vec);
                    tmp_vec.rd = cur_vec.rd;
                }
                else
                {
                    tmp_vec.rd = cur_vec.down;
                }                
            }
            else
            {
                tmp_vec.rd = cur_vec.rd;
            }
            vecid_cur = pre_vec.rd;
            pre_vec.val = atomic64_cmpxchg((atomic64_t *)ppre, pre_vec.val, tmp_vec.val);
            if(pre_vec.val == tmp_vec.val)//delete succ
            {
                vec_free_to_buf(pclst, vecid_cur);
            }          
            /*���۽����ɹ����ֱ�����µ�pre_vec�����жϺʹ���*/
            if(pre_vec.valid == SPT_VEC_INVALID)
            {
                return SPT_INVALID;
            }
            if(pre_vec.flag == SPT_VEC_FLAG_DATA)
            {
                return pre_vec.rd;
            }
            else if(pre_vec.flag == SPT_VEC_FLAG_RIGHT)
            {
                pvec = (spt_vec_t *)vec_id_2_ptr(pclst,pre_vec.rd);
                cur_vec = *pvec;
            }                
            else if(pre_vec.flag == SPT_VEC_FlAG_SIGNPOST)
            {
                if(pre_vec.ext_sys_flg == SPT_VEC_SYS_FLAG_DATA)
                {
                    return pre_vec.rd;
                }
                pvec = (spt_vec_t *)vec_id_2_ptr(pclst,pre_vec.rd);
                cur_vec = *pvec;
            }
            else
            {
                assert(0);
                return SPT_INVALID;
            }
        }
        else
        {
            if(cur_vec.flag == SPT_VEC_FLAG_DATA)
            {
                return cur_vec.rd;
            }
            else if(cur_vec.flag == SPT_VEC_FLAG_RIGHT)
            {
                ppre = pvec;
                pre_vec = cur_vec;
                pvec = (spt_vec_t *)vec_id_2_ptr(pclst,cur_vec.rd);
                cur_vec = *pvec;                
            }            
            else if(cur_vec.flag == SPT_VEC_FlAG_SIGNPOST)
            {
                if(cur_vec.ext_sys_flg == SPT_VEC_SYS_FLAG_DATA)
                {
                    return cur_vec.rd;
                }
                ppre = pvec;
                pre_vec = cur_vec;
                pvec = (spt_vec_t *)vec_id_2_ptr(pclst,cur_vec.rd);
                cur_vec = *pvec;                
            }
            else
            {
                assert(0);
                return SPT_INVALID;
            }            
        }
    }
}

/*data=-1������Ҫ����������*/
 void get_final_vec(cluster_head_t *pclst, spt_vec_t* pvec, spt_vec_t_r *pvec_r, u32 data)
{
    pvec_r->pos = 0;
    while(pvec->flag&SPT_VEC_FLAG_NLAST_MASK)
    {
        pvec_r->pos += spt_get_pos_multi(pvec->pos)*POW256(spt_get_pos_index(pvec->pos));
        pvec = (spt_vec_t *)vec_id_2_ptr(pclst,pvec->rdn);
    }
    
    pvec_r->pos += spt_get_pos_multi(pvec->pos)*POW256(spt_get_pos_index(pvec->pos));

    pvec_r->down = pvec->down;

    if(pvec->flag&SPT_VEC_FLAG_RIGHT_MASK)
    {
        pvec_r->right = pvec->rdn;
        
        if(data != -1)
        {
            pvec_r->data = data;
        }
        else
        {
            do
            {
                pvec = (spt_vec_t *)vec_id_2_ptr(pclst,pvec->rdn);
                if(pvec == NULL)
                {
                    assert(0);
                }                
            }while(pvec->flag&SPT_VEC_FLAG_RIGHT_MASK);

            pvec_r->data = pvec->rdn;
        }
    }
    else
    {
        pvec_r->right = SPT_NULL;
        pvec_r->data = pvec->rdn;
    }
    return;
}
/*�п��������ұ�����һ������(rdn��¼d��ɼ�¼r);Ҳ�п��ܷ����Ҳ�����ͨ���ϲ�������Ҳ����������Ҹı�dataid*/
/**/
/*�������˵��:
    1.pos��Ϊ0ʱ�����ͬʱҲ�޸�dataid�������dataid���޸�
    2.a.right�޸�Ϊ�� b.pos�޸�Ϊ0 c.pvec_r->data��ΪSPT_INVALID,
      Ҫôabcͬʱ���֣�Ҫôc��������ʱ��ab�������ǿպ�0.
      
    3.
    */
int modify_vec(cluster_head_t **ppclst, spt_vec_t *pvec, spt_vec_t_r *pvec_r)
{
    u64 pos;
    int multi;
    int i=0;
    u32 tmpid=0;
    u32 free_head;
    spt_vec_t *ptmp, *pprev, *ptail, *pnew_tail;
    unsigned int down;
    unsigned int rdn;
    unsigned int flag;

    /*��������������
        1.ԭ������������ֻ�޸�rdn��down(��һ���߶��޸�)
        2.ԭ��������������pos����ֵС��256��
    */
    if(!spt_nlst_is_set(pvec->flag))
    {
        if(pvec_r->pos != SPT_INVALID)
        {
            if(pvec_r->pos <= SPT_VEC_POS_MULTI_MASK)
            {
                pvec->pos = pvec_r->pos;
                if(pvec->pos == 0)
                {
                    assert(pvec_r->right == SPT_NULL);
                    assert(pvec_r->data != SPT_INVALID);
                    pvec->rdn = pvec_r->data;
                    spt_set_data_flag(pvec->flag);                    
                }
                else
                {
                    if(pvec_r->right != SPT_INVALID)
                    {
                        assert(pvec_r->right != SPT_NULL);
                        pvec->rdn = pvec_r->right;
                        spt_set_right_flag(pvec->flag);
                    }
                    /*���Զ�dataid���޸�*/
                }
                if(pvec_r->down != SPT_INVALID)
                {
                    pvec->down = pvec_r->down;
                }
                return SPT_OK;
            }
        }
        else/*���޸�pos*/
        {
            if(pvec_r->down != SPT_INVALID)
            {
                pvec->down = pvec_r->down;
            }
            if(pvec_r->right != SPT_INVALID)
            {
                /*����޸�rightΪ�գ���Ȼ��Ҫ�޸�posΪ0�����pos������Ϊ0��right��Ϊ0������Ҫ�޸���Ϊ��*/
                assert(pvec_r->right != SPT_NULL);
                pvec->rdn = pvec_r->right;
                spt_set_right_flag(pvec->flag);
            }
            /**/
            if(pvec_r->data != SPT_INVALID && pvec->pos == 0)
            {
                pvec->rdn = pvec_r->data;
                spt_set_data_flag(pvec->flag);
            }
            return SPT_OK;
        }
        #if 0
        if(pvec_r->pos != SPT_INVALID)
        {
            if(pvec_r->pos <= SPT_VEC_POS_MULTI_MASK)
            {
                pvec->pos = pvec_r->pos;
                if(pvec_r->down != SPT_INVALID)
                {
                    pvec->down = pvec_r->down;
                }
                
                if(pvec_r->right != SPT_INVALID)
                {    
                    if(pvec_r->right == SPT_NULL)
                    {
                        assert(pvec->pos == 0);
                        assert(pvec_r->data != SPT_INVALID);

                        pvec->rdn = pvec_r->data;
                        spt_set_data_flag(pvec->flag);
                    }
                    else
                    {
                        pvec->rdn = pvec_r->right;
                        spt_set_right_flag(pvec->flag);
                    }
                }
                else if(pvec_r->data != SPT_INVALID)
                {
                    pvec->rdn = pvec_r->data;
                    assert(pvec->flag == 0);
                }
                return SPT_OK;
            }
        }
        else
        {
            if(pvec_r->down != SPT_INVALID)
            {
                pvec->down = pvec_r->down;
            }
            if(pvec_r->right != SPT_INVALID)
            {
                pvec->rdn = pvec_r->right;
                spt_set_right_flag(pvec->flag);
            }
            if(pvec_r->data != SPT_INVALID)
            {
                if(pvec->flag != 0)
                {
                    assert(0);
                    printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
                    return SPT_ERR;
                }
                pvec->rdn = pvec_r->data;
            }            
            return SPT_OK;
        }
        #endif
    }
    /*������pvec�϶�����*/
    ptmp = pvec;
    pos = pvec_r->pos;
    pprev = 0;
    ptail = 0;
    
    if(pos != SPT_INVALID)
    {
        while(pos != 0)
        {
            multi = pos & SPT_VEC_POS_MULTI_MASK;
            if(multi != 0)
            {
                if(ptail != 0)
                {
                    tmpid = vec_alloc(ppclst, &ptmp);
                    if(ptmp == 0)
                    {
                        /*yzx�ͷ���Դ�� �����¿飬���*/
                        return CLT_NOMEM;
                    }
                    ptmp->flag = 0;
                    spt_set_pos(ptmp->pos, i, multi);
                    pprev->rdn = tmpid;
                    spt_set_vec_nlst(pprev->flag);
                    pprev = ptmp;
                }
                else
                {    
                    spt_set_pos(ptmp->pos, i, multi);

                    if(!spt_nlst_is_set(ptmp->flag))
                    {
                        ptail = ptmp;
                        down = ptail->down;
                        rdn = ptail->rdn;
                        flag = ptail->flag;
                    }
                    else
                    {
                        pprev = ptmp;
                        tmpid = ptmp->rdn;
                        ptmp = (spt_vec_t *)vec_id_2_ptr(*ppclst,tmpid);                
                    }
                }
            }    
            i++;
            pos = pos >> SPT_VEC_POS_INDEX_BIT;
        }

        /*ԭʼ��û�е�β�����ҵ�β��¼down��rdnֵ����free pvecʣ��Ĵ�*/
        if(ptail == 0)
        {
            if(pvec_r->pos == 0)
            {
                pvec->pos = 0;
                pnew_tail = pvec;
                free_head = tmpid = ptmp->rdn;
                ptmp = (spt_vec_t *)vec_id_2_ptr(*ppclst,tmpid);    

            }
            else
            {
                free_head = tmpid;
                pnew_tail = pprev;
            }
            while(spt_nlst_is_set(ptmp->flag))
            {
                tmpid = ptmp->rdn;
                ptmp = (spt_vec_t *)vec_id_2_ptr(*ppclst,tmpid);
            }
            down = ptmp->down;
            rdn = ptmp->rdn;
            flag = ptmp->flag;
            vec_list_free(*ppclst, free_head);
        }
        else
        {
            pnew_tail = ptmp;
        }    
        pnew_tail->down = (pvec_r->down == SPT_INVALID) ?  down:pvec_r->down;

        if(pvec_r->pos == 0)
        {
            assert(pvec_r->right == SPT_NULL);
            assert(pvec_r->data != SPT_INVALID);
            pnew_tail->rdn = pvec_r->data;
            spt_set_data_flag(pnew_tail->flag);                    
        }
        else
        {
            if(pvec_r->right != SPT_INVALID)
            {
                assert(pvec_r->right != SPT_NULL);
                pnew_tail->rdn = pvec_r->right;
                spt_set_right_flag(pnew_tail->flag);
            }
            else
            {
                pnew_tail->rdn = rdn;
                pnew_tail->flag = flag;
            }
            /*���Զ�dataid���޸�*/
        }            
        #if 0
        /*�п������޸�right��Ҳ�п������Ҳ�������һ��������rdn�ɼ�¼d����˼�¼r*/
        if(pvec_r->right != SPT_INVALID)
        {
            pnew_tail->rdn = pvec_r->right;
            spt_set_right_flag(pnew_tail->flag);
        }
        else
        {
            pnew_tail->rdn = rdn;
            pnew_tail->flag = flag;
        }
        if(pvec_r->data != SPT_INVALID)
        {
            if(pnew_tail->flag != 0)
            {
                assert(0);
                printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
                return SPT_ERR;
            }
            pnew_tail->rdn = pvec_r->data;
        }
        #endif
        return SPT_OK;
    }
    /*������pos�϶���Ϊ0*/
    else/*�������޸�������ʲô�����Ҫ���ҵ�tail*/
    {
        while(spt_nlst_is_set(ptmp->flag))
        {
            tmpid = ptmp->rdn;
            ptmp = (spt_vec_t *)vec_id_2_ptr(*ppclst,tmpid);
        }
        down = ptmp->down;
        rdn = ptmp->rdn;
        flag = ptmp->flag;
        pnew_tail = ptmp;
    }
    if(pvec_r->down != SPT_INVALID)
    {
        pnew_tail->down = pvec_r->down;
    }
    if(pvec_r->right != SPT_INVALID)
    {
        assert(pvec_r->right != SPT_NULL);
        pnew_tail->rdn = pvec_r->right;
        spt_set_right_flag(pnew_tail->flag);
    }
    /*pos�϶���Ϊ0, ���Զ�dataid���޸�*/
    #if 0
    else
    {
        pnew_tail->rdn = rdn;
        pnew_tail->flag = flag;
    }
    #endif
    return SPT_OK;    
}

int construct_vec_new(cluster_head_t **ppclst, spt_vec_t_r *pvec_r, spt_vec_t **pret_vec)
{
    u64 pos;
    int multi;
    int i=0;
    u32 vec, vec_h;
    spt_vec_t *pvec_p, *pvec;

    pos = pvec_r->pos;
    if(pos == 0)
    {
        vec = vec_alloc(ppclst, &pvec);
        if(pvec == 0)
        {
            /*yzx�ͷ���Դ�� �����¿飬���*/
            *pret_vec = NULL;
            return SPT_NULL;
        }
        pvec->pos = 0;
        *pret_vec = pvec;
        vec_h = vec;
    }
    else
    {
        pvec_p = 0;    
        do
        {
            multi = pos & SPT_VEC_POS_MULTI_MASK;
            if(multi != 0)
            {
                vec = vec_alloc(ppclst, &pvec);
                if(pvec == 0)
                {
                    /*yzx�ͷ���Դ�� �����¿飬���*/
                    *pret_vec = NULL;
                    return SPT_NULL;
                }
                if(pvec_p == 0)
                {
                    *pret_vec = pvec;
                    vec_h = vec;
                }
                else
                {
                    pvec_p->rdn = vec;
                    spt_set_vec_nlst(pvec_p->flag);
                }
                spt_set_pos(pvec->pos, i, multi);
                pvec_p = pvec;

            }
            i++;
        }while((pos = pos >> SPT_VEC_POS_INDEX_BIT) != 0);
    }

    pvec->down = pvec_r->down;
    if(pvec_r->right != SPT_NULL)
    {
        pvec->rdn = pvec_r->right;
        spt_set_right_flag(pvec->flag);
    }
    else
    {
        assert(pvec_r->pos == 0);
        pvec->rdn = pvec_r->data;
        spt_set_data_flag(pvec->flag);
    }

    return vec_h;
}

int get_modify_auth()
{
    return 1;
}

void do_modify(cluster_head_t **ppclst, spt_md_entirety *pent)
{
    spt_md_vec_t *ptmp_md_vec, *pcur_md_vec;
    spt_free_vec_node *pcur_free_node, *ptmp_free_node;
    spt_del_data_node *ptmp_del_data, *pcur_del_data;
    spt_vec_t *pcur_vec;

    /*1.����ʱ����������������ǰ����ã�ɾ��ʱ�����ͷŵ�������>=�޸�pos����������������*/
    /*2.���ͷ�������ͬһʱ��ֻ��һ���߳����޸Ĺ������ṹ����ֻ���޸ĵ�ʱ��������ͷ�����*/
    /*�ۺ�1��2���޸ıض��ɹ�*/
    pcur_free_node = pent->free_head;
    while(pcur_free_node != NULL)
    {
        ptmp_free_node = pcur_free_node;
        vec_list_free(*ppclst, pcur_free_node->id);
        pcur_free_node = pcur_free_node->next;
        free(ptmp_free_node);
    }

    pcur_md_vec = pent->md_head;
    while(pcur_md_vec != NULL)
    {
        ptmp_md_vec = pcur_md_vec;
        if(pcur_md_vec->vec_id == SPT_NULL)
        {
            (*ppclst)->vec_head = pcur_md_vec->value.down;
        }
        else
        {
            pcur_vec = (spt_vec_t *)vec_id_2_ptr(*ppclst, pcur_md_vec->vec_id);
            modify_vec(ppclst, pcur_vec, &pcur_md_vec->value);
        }
        pcur_md_vec = pcur_md_vec->next;
        free(ptmp_md_vec);    
    }
    
    pcur_del_data = pent->del_head;
    while(pcur_del_data != NULL)
    {
        ptmp_del_data = pcur_del_data;
        db_free(*ppclst, pcur_del_data->id);
        pcur_del_data = pcur_del_data->next;
        free(ptmp_del_data);
    }

}

#if 0
/*startbit ��aָ����ֽڷ�Χ��*/
 u64 stfind_first_set(char *a,  u8 startbit, char *end)
{
    int perbyteCnt,perllCnt;
    u8 bitstart, uca;
    u64 ulla, zCnt, fs;

    zCnt = 0;
    if(startbit != 0)
    {
        uca = *a << startbit >> startbit;a++;
        if((zCnt = ucfind_firt_set(uca)) < 8)
        {
            return zCnt;
        }
    }

    if(a%8 != 0)
    {
        perbyteCnt = 8-a%8;
        while(perbyteCnt > 0)
        {
            uca = *a; a++;
            if((fs = ucfind_firt_set(uca)) < 8)
            {
                return zCnt+fs;
            }
            
            zCnt += 8;
        }
    }
    
    perllCnt = (end - a)/8;

    while (perllCnt > 0)
    {
        ulla = *(u64 *)a;a += 8;
        if((fs = ullfind_firt_set(ulla)) < 64)
        {
            return zCnt+fs;
        }
        zCnt += 64;
        perllCnt--;
    }
    while(a < end)
    {
        uca = *a; a++;
        if((fs = ucfind_firt_set(uca)) < 8)
        {
            return zCnt+fs;
        }
        zCnt += 8;
    }

    return zCnt;
}
#endif

int do_insert_signpost_right(cluster_head_t **ppclst, insert_info_t *pinsert, char *new_data)
{
    spt_vec_t *ppre = NULL;
    spt_vec_t tmp_vec, cur_vec, pre_vec, *pcur, *pvec_a;
    char *pcur_data;//*ppre_data,
    u64 startbit, endbit, len, fs_pos, signpost;
    u32 dataid, vecid_a;
    char *pdata;

    dataid = db_alloc(ppclst, &pdata);
    if(pdata == 0)
    {
        /*�����¿飬���*/
        assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return SPT_NOMEM;
    }
    memcpy(pdata, new_data, DATA_SIZE);

    tmp_vec.val = pinsert->key_val;
    signpost = pinsert->signpost;

    vecid_a = vec_alloc(ppclst, &pvec_a);
    if(pvec_a == 0)
    {
        /*yzx�ͷ���Դ�� �����¿飬���*/
        db_free(*ppclst, dataid);
        return SPT_NOMEM;
    }
    pvec_a->val = 0;
    pvec_a->flag = SPT_VEC_FLAG_DATA;
    pvec_a->pos = 0;
    pvec_a->rd = dataid;
    pvec_a->down = tmp_vec.rd;

    tmp_vec.rd = vecid_a;
    if(tmp_vec.val == atomic64_cmpxchg((atomic64_t *)pinsert->pkey_vec, 
                        pinsert->key_val, tmp_vec.val))
    {
        return SPT_OK;
    }
    else
    {
        db_free(*ppclst, dataid);
        vec_free(*ppclst, vecid_a);
        return SPT_DO_AGAIN;
    }

}

int do_insert_up_via_r(cluster_head_t **ppclst, insert_info_t *pinsert, char *new_data)
{
    spt_vec_t tmp_vec, cur_vec, pre_vec, *pcur, *pvec_a, *pvec_b, *pvec_s;
    char *pcur_data;//*ppre_data,
    u64 fs_pos, signpost;
    u32 dataid, vecid_a, vecid_b, vecid_s;
    char *pdata;

    pvec_s = 0;
    dataid = db_alloc(ppclst, &pdata);
    if(pdata == 0)
    {
        /*�����¿飬���*/
        assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return SPT_NOMEM;
    }
    memcpy(pdata, new_data, DATA_SIZE);

    tmp_vec.val = pinsert->key_val;
    signpost = pinsert->signpost;

    vecid_b = vec_alloc(ppclst, &pvec_b);
    if(pvec_b == 0)
    {
        /*yzx�ͷ���Դ�� �����¿飬���*/
        db_free(*ppclst, dataid);
        return SPT_NOMEM;
    }
    pvec_b->val = 0;
//    pvec_b->flag = tmp_vec.flag;
    pvec_b->pos = (pinsert->fs-1)&SPT_VEC_SIGNPOST_MASK;
//    pvec_b->rd = tmp_vec.rd;
    pvec_b->down = SPT_NULL;

    vecid_a = vec_alloc(ppclst, &pvec_a);
    if(pvec_a == 0)
    {
        /*yzx�ͷ���Դ�� �����¿飬���*/
        db_free(*ppclst, dataid);
        return SPT_NOMEM;
    }
    pvec_a->val = 0;
    pvec_a->flag = SPT_VEC_FLAG_DATA;
    pvec_a->pos = (pinsert->cmp_pos-1)&SPT_VEC_SIGNPOST_MASK;
    pvec_a->rd = dataid;

    if(tmp_vec.flag == SPT_VEC_FLAG_DATA)
    {
        pvec_b->flag = SPT_VEC_FLAG_DATA;
        pvec_b->rd = tmp_vec.rd;
        tmp_vec.flag = SPT_VEC_FLAG_RIGHT;
    }
    else if(tmp_vec.flag == SPT_VEC_FlAG_SIGNPOST)
    {
        pvec_b->rd = tmp_vec.rd;
        if(tmp_vec.ext_sys_flg == SPT_VEC_SYS_FLAG_DATA)
        {
            tmp_vec.ext_sys_flg = 0;
            pvec_b->flag = SPT_VEC_FLAG_DATA;
        }
        else
        {
            pvec_b->flag = SPT_VEC_FLAG_RIGHT;
        }
    }
    else
    {
        pvec_b->flag = SPT_VEC_FLAG_RIGHT;
        pvec_b->rd = tmp_vec.rd;
    }

    
    if((pinsert->cmp_pos-1)-signpost > SPT_VEC_SIGNPOST_MASK)
    {
        vecid_s = vec_alloc(ppclst, &pvec_s);
        if(pvec_s == 0)
        {
            /*yzx�ͷ���Դ�� �����¿飬���*/
            db_free(*ppclst, dataid);
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
        vecid_s = vec_alloc(ppclst, &pvec_s);
        if(pvec_s == 0)
        {
            /*yzx�ͷ���Դ�� �����¿飬���*/
            db_free(*ppclst, dataid);
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
    if(tmp_vec.val == atomic64_cmpxchg((atomic64_t *)pinsert->pkey_vec, 
                        pinsert->key_val, tmp_vec.val))
    {
        return SPT_OK;
    }
    else
    {
        db_free(*ppclst, dataid);
        vec_free(*ppclst, vecid_a);
        vec_free(*ppclst, vecid_b);
        if(pvec_s != 0)
        {
            vec_free(*ppclst, vecid_s);
        }
        return SPT_DO_AGAIN;
    }
}

/*��cur->rightСʱ���嵽����*/
int do_insert_down_via_r(cluster_head_t **ppclst, insert_info_t *pinsert, char *new_data)
{
    spt_vec_t tmp_vec, cur_vec, pre_vec, *pcur, *pvec_a, *pvec_b, *pvec_s;
    char *pcur_data;//*ppre_data,
    u64 fs_pos, signpost;
    u32 dataid, vecid_a, vecid_b, vecid_s;
    char *pdata;

    pvec_s = 0;
    dataid = db_alloc(ppclst, &pdata);
    if(pdata == 0)
    {
        /*�����¿飬���*/
        assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return SPT_NOMEM;
    }
    memcpy(pdata, new_data, DATA_SIZE);

    tmp_vec.val = pinsert->key_val;
    signpost = pinsert->signpost;

    vecid_b = vec_alloc(ppclst, &pvec_b);
    if(pvec_b == 0)
    {
        /*yzx�ͷ���Դ�� �����¿飬���*/
        db_free(*ppclst, dataid);
        return SPT_NOMEM;
    }
    pvec_b->val = 0;
    pvec_b->flag = SPT_VEC_FLAG_DATA;
    pvec_b->pos = (pinsert->fs-1)&SPT_VEC_SIGNPOST_MASK;
    pvec_b->rd = tmp_vec.rd;
    pvec_b->down = SPT_NULL;

    vecid_a = vec_alloc(ppclst, &pvec_a);
    if(pvec_a == 0)
    {
        /*yzx�ͷ���Դ�� �����¿飬���*/
        db_free(*ppclst, dataid);
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
        vecid_s = vec_alloc(ppclst, &pvec_s);
        if(pvec_s == 0)
        {
            /*yzx�ͷ���Դ�� �����¿飬���*/
            db_free(*ppclst, dataid);
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
        vecid_s = vec_alloc(ppclst, &pvec_s);
        if(pvec_s == 0)
        {
            /*yzx�ͷ���Դ�� �����¿飬���*/
            db_free(*ppclst, dataid);
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
    if(tmp_vec.val == atomic64_cmpxchg((atomic64_t *)pinsert->pkey_vec, 
                        pinsert->key_val, tmp_vec.val))
    {
        return SPT_OK;
    }
    else
    {
        db_free(*ppclst, dataid);
        vec_free(*ppclst, vecid_a);
        vec_free(*ppclst, vecid_b);
        if(pvec_s != 0)
        {
            vec_free(*ppclst, vecid_s);
        }
        return SPT_DO_AGAIN;
    }
}
/*cur->down == null, ��λΪ0 ��ֱ�Ӳ嵽cur->down��*/
int do_insert_last_down(cluster_head_t **ppclst, insert_info_t *pinsert, char *new_data)
{
    spt_vec_t tmp_vec, cur_vec, pre_vec, *pcur, *pvec_a, *pvec_b, *pvec_s;
    char *pcur_data;//*ppre_data,
    u64 fs_pos, signpost;
    u32 dataid, vecid_a, vecid_b, vecid_s;
    char *pdata;

    pvec_s = 0;
    dataid = db_alloc(ppclst, &pdata);
    if(pdata == 0)
    {
        /*�����¿飬���*/
        assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return SPT_NOMEM;
    }
    memcpy(pdata, new_data, DATA_SIZE);

    tmp_vec.val = pinsert->key_val;
    signpost = tmp_vec.idx << SPT_VEC_SIGNPOST_BIT;

    vecid_a = vec_alloc(ppclst, &pvec_a);
    if(pvec_a == 0)
    {
        /*yzx�ͷ���Դ�� �����¿飬���*/
        db_free(*ppclst, dataid);
        return SPT_NOMEM;
    }
    pvec_a->val = 0;
    pvec_a->flag = SPT_VEC_FLAG_DATA;
    pvec_a->pos = (pinsert->fs-1)&SPT_VEC_SIGNPOST_MASK;
    pvec_a->rd = dataid;
    pvec_a->down = SPT_NULL;

    if(tmp_vec.flag == SPT_VEC_FlAG_SIGNPOST)
    {
        vecid_b = vec_alloc(ppclst, &pvec_b);
        if(pvec_b == 0)
        {
            /*yzx�ͷ���Դ�� �����¿飬���*/
            db_free(*ppclst, dataid);
            return SPT_NOMEM;
        }
        pvec_b->val = 0;
//        pvec_b->flag = SPT_VEC_FLAG_DATA;
        pvec_b->pos = 0;
        pvec_b->rd = tmp_vec.rd;
//        pvec_b->down = SPT_NULL;
        if(tmp_vec.ext_sys_flg == SPT_VEC_SYS_FLAG_DATA)
        {
            tmp_vec.ext_sys_flg = 0;
            pvec_b->flag = SPT_VEC_FLAG_DATA;
        }
        else
        {
            pvec_b->flag = SPT_VEC_FLAG_RIGHT;
        }
        tmp_vec.rd = vecid_b;
        if((pinsert->fs-1)-signpost > SPT_VEC_SIGNPOST_MASK)
        {
            vecid_s = vec_alloc(ppclst, &pvec_s);
            if(pvec_s == 0)
            {
                /*yzx�ͷ���Դ�� �����¿飬���*/
                db_free(*ppclst, dataid);
                return SPT_NOMEM;
            }
            pvec_s->val = 0;        
            pvec_s->idx = (pinsert->fs-1)>>SPT_VEC_SIGNPOST_BIT;
            pvec_s->rd = vecid_a;

            pvec_b->down = vecid_s;
        }
        else
        {
            pvec_b->down = vecid_a;
        }
    }
    else
    {
        if((pinsert->fs-1)-signpost > SPT_VEC_SIGNPOST_MASK)
        {
            vecid_s = vec_alloc(ppclst, &pvec_s);
            if(pvec_s == 0)
            {
                /*yzx�ͷ���Դ�� �����¿飬���*/
                db_free(*ppclst, dataid);
                return SPT_NOMEM;
            }
            pvec_s->val = 0;        
            pvec_s->idx = (pinsert->fs-1)>>SPT_VEC_SIGNPOST_BIT;
            pvec_s->rd = vecid_a;

            tmp_vec->down = vecid_s;
        }
        else
        {
            tmp_vec->down = vecid_a;
        }        
    }

    if(tmp_vec.val == atomic64_cmpxchg((atomic64_t *)pinsert->pkey_vec, 
                        pinsert->key_val, tmp_vec.val))
    {
        return SPT_OK;
    }
    else
    {
        db_free(*ppclst, dataid);
        vec_free(*ppclst, vecid_a);
        if(pvec_b != 0)
        {
            vec_free(*ppclst, vecid_b);
        }
        if(pvec_s != 0)
        {
            vec_free(*ppclst, vecid_s);
        }
        return SPT_DO_AGAIN;
    }
}

int do_insert_first_set(cluster_head_t **ppclst, u32 cur_vec, char *new_data)
{
    u32 dataid, vec_a;
    char *pdata;
    spt_md_entirety *pmdf_ent;
    spt_md_vec_t *pmdf_vec;
    spt_vec_t_r tmp_vec_r;
    spt_vec_t *pvec_a;

    dataid = db_alloc(ppclst, &pdata);
    if(pdata == 0)
    {
        /*�����¿飬���*/
        assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return 1;
    }
    memcpy(pdata, new_data, DATA_SIZE);

    pmdf_ent = malloc(sizeof(spt_md_entirety));
    if(pmdf_ent == NULL)
    {
        assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return 1;
    }
    pmdf_ent->md_head = NULL;
    pmdf_ent->free_head = NULL;
    pmdf_ent->del_head = NULL;

    tmp_vec_r.data = dataid;
    tmp_vec_r.pos = 0;
    tmp_vec_r.down = SPT_NULL;
    tmp_vec_r.right = SPT_NULL;
    vec_a = construct_vec_new(ppclst, &tmp_vec_r, &pvec_a);
    if(pvec_a == NULL)
    {
        assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return 1;
    }
    
    pmdf_vec = malloc(sizeof(spt_md_vec_t));
    if(pmdf_vec == NULL)
    {
        assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return 1;
    }
    pmdf_vec->vec_id = cur_vec;
    pmdf_vec->value.data = SPT_INVALID;
    pmdf_vec->value.down = SPT_INVALID;
    pmdf_vec->value.right = vec_a;
    pmdf_vec->value.pos = SPT_INVALID;
    
    pmdf_vec->next = pmdf_ent->md_head;
    pmdf_ent->md_head = pmdf_vec;

    do_modify(ppclst, pmdf_ent);
    return 0;
}



int do_insert_up_via_d(cluster_head_t **ppclst, insert_info_t *pinsert, char *new_data)
{
    u32 dataid, vec_a, vec_b;
    char *pdata;
    spt_md_entirety *pmdf_ent;
    spt_md_vec_t *pmdf_vec;
    spt_vec_t_r tmp_vec_r, *pdown_r, *pcur_r;
    spt_vec_t *pvec_a, *pvec_b;
    u64 startbit, fs;

    dataid = db_alloc(ppclst, &pdata);
    if(pdata == 0)
    {
        /*�����¿飬���*/
        assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return 1;
    }
    memcpy(pdata, new_data, DATA_SIZE);

    pmdf_ent = malloc(sizeof(spt_md_entirety));
    if(pmdf_ent == NULL)
    {
        assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return 1;
    }
    pmdf_ent->md_head = NULL;
    pmdf_ent->free_head = NULL;
    pmdf_ent->del_head = NULL;

    pdown_r = pinsert->pn_vec_r;
    pcur_r = pinsert->pcur_vec_r;
    startbit =  pinsert->startbit;
    fs = pinsert->fs;

    if(pdown_r->pos != 0)
    {
        pmdf_vec = malloc(sizeof(spt_md_vec_t));
        if(pmdf_vec == NULL)
        {
            assert(0);
            printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
            return 1;
        }
        pmdf_vec->vec_id = pcur_r->down;
        pmdf_vec->value.data = SPT_INVALID;
        pmdf_vec->value.down = SPT_INVALID;
        pmdf_vec->value.right = SPT_INVALID;
        pmdf_vec->value.pos = pdown_r->pos + startbit - fs;
        
        pmdf_vec->next = pmdf_ent->md_head;
        pmdf_ent->md_head = pmdf_vec;
    }

    tmp_vec_r.data = dataid;
    tmp_vec_r.pos = 0;//���ұ�һ���pos�򻯼�¼Ϊ0
    tmp_vec_r.down = SPT_NULL;
    tmp_vec_r.right = SPT_NULL;
    vec_b = construct_vec_new(ppclst, &tmp_vec_r, &pvec_b);
    if(pvec_b == NULL)
    {
        assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return 1;
    } 

    tmp_vec_r.data = dataid;
    tmp_vec_r.pos = fs-startbit;
    tmp_vec_r.down = pcur_r->down;
    tmp_vec_r.right = vec_b;
    vec_a = construct_vec_new(ppclst, &tmp_vec_r, &pvec_a);
    if(pvec_a == NULL)
    {
        assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return 1;
    }

    pmdf_vec = malloc(sizeof(spt_md_vec_t));
    if(pmdf_vec == NULL)
    {
        assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return 1;
    }    
    pmdf_vec->vec_id = pinsert->cur_vec;
    pmdf_vec->value.data = SPT_INVALID;
    pmdf_vec->value.down = vec_a;
    pmdf_vec->value.right = SPT_INVALID;
    pmdf_vec->value.pos = SPT_INVALID;
    
    pmdf_vec->next = pmdf_ent->md_head;
    pmdf_ent->md_head = pmdf_vec;

    do_modify(ppclst, pmdf_ent);
    return 0;
}

int do_del_isolate(cluster_head_t **ppclst, u32 up_2_del, int direction, u32 dataid)
{
    u32 cur_vec;
    spt_md_entirety *pmdf_ent;
    spt_md_vec_t *pmdf_vec;
    spt_vec_t_r st_vec_r, st_u2dr_r;
    spt_vec_t *pvec;
    spt_del_data_node *pdel_data;
    spt_free_vec_node *pfree_vec;

    pmdf_ent = malloc(sizeof(spt_md_entirety));
    if(pmdf_ent == NULL)
    {
        assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return 1;
    }
    pmdf_ent->md_head = NULL;
    pmdf_ent->free_head = NULL;
    pmdf_ent->del_head = NULL;
    
    pmdf_vec = malloc(sizeof(spt_md_vec_t));
    if(pmdf_vec == NULL)
    {
        assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return 1;
    }
    if(up_2_del == SPT_NULL)
    {
        pvec = (spt_vec_t *)vec_id_2_ptr(*ppclst, (*ppclst)->vec_head);
        get_vec_rd(*ppclst, pvec, &st_vec_r);
        cur_vec = st_vec_r.right;

        pmdf_vec->vec_id = (*ppclst)->vec_head;
        pmdf_vec->value.data = -1;
        pmdf_vec->value.down = SPT_INVALID;
        pmdf_vec->value.right = SPT_NULL;
        pmdf_vec->value.pos = 0;
    }
    else
    {
        pvec = (spt_vec_t *)vec_id_2_ptr(*ppclst, up_2_del);
        get_final_vec(*ppclst, pvec, &st_vec_r, -1);
        cur_vec = st_vec_r.down;
        /*need merge up_2_del->right*/
        if(direction == SPT_RIGHT)
        {
            pvec = (spt_vec_t *)vec_id_2_ptr(*ppclst, st_vec_r.right);
            get_final_vec(*ppclst, pvec, &st_u2dr_r, st_vec_r.data);
            /*merge up_2_del->right*/
            if(st_u2dr_r.pos != 0)
            {
                pmdf_vec->value.pos = st_vec_r.pos + st_u2dr_r.pos;
                pmdf_vec->value.data = SPT_INVALID;
            }
            else
            {
                pmdf_vec->value.pos = 0;
                pmdf_vec->value.data = st_vec_r.data;            
            }
            
            pmdf_vec->vec_id = up_2_del;
            pmdf_vec->value.down = st_u2dr_r.down;
            pmdf_vec->value.right = st_u2dr_r.right;

            pfree_vec = malloc(sizeof(spt_free_vec_node));
            if(pfree_vec == NULL)
            {
                assert(0);
                printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
                return 1;
            }
            pfree_vec->id = st_vec_r.right;
            pfree_vec->next = pmdf_ent->free_head;
            pmdf_ent->free_head = pfree_vec;            

        }
        else
        {
            pmdf_vec->vec_id = up_2_del;
            pmdf_vec->value.pos = SPT_INVALID;
            pmdf_vec->value.down = SPT_NULL;
            pmdf_vec->value.right = SPT_INVALID;
            pmdf_vec->value.data = SPT_INVALID;
        }
    #if 0
        pvec = (spt_vec_t *)vec_id_2_ptr(*ppclst, up_2_del);
        get_final_vec(*ppclst, pvec, &st_vec_r, -1);
        cur_vec = st_vec_r.down;

        pvec = (spt_vec_t *)vec_id_2_ptr(*ppclst, st_vec_r.right);
        get_final_vec(*ppclst, pvec, &st_u2dr_r, st_vec_r.data);
        /*merge up_2_del->right*/
        if(st_u2dr_r.pos != 0 && direction == SPT_RIGHT)
        {
            pmdf_vec->value.pos = st_vec_r.pos + st_u2dr_r.pos;
            pmdf_vec->value.data = SPT_INVALID;
        }
        else
        {
            pmdf_vec->value.pos = 0;
            pmdf_vec->value.data = st_vec_r.data;            
        }
        
        pmdf_vec->vec_id = up_2_del;
        pmdf_vec->value.down = SPT_NULL;
        pmdf_vec->value.right = st_u2dr_r.right;
    #endif
    }
    pmdf_vec->next = pmdf_ent->md_head;
    pmdf_ent->md_head = pmdf_vec;
    
    while(cur_vec != SPT_NULL)
    {
        pfree_vec = malloc(sizeof(spt_free_vec_node));
        if(pfree_vec == NULL)
        {
            assert(0);
            printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
            return 1;
        }
        pfree_vec->id = cur_vec;
        pfree_vec->next = pmdf_ent->free_head;
        pmdf_ent->free_head = pfree_vec;
        
        pvec = (spt_vec_t *)vec_id_2_ptr(*ppclst, cur_vec);
        get_vec_rd(*ppclst, pvec, &st_vec_r);
        cur_vec = st_vec_r.right;
    }

    pdel_data = malloc(sizeof(spt_del_data_node));
    if(pdel_data == NULL)
    {
        assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return 1;
    }
    pdel_data->id = dataid;
    pdel_data->next = pmdf_ent->del_head;
    pmdf_ent->del_head = pdel_data;

    do_modify(ppclst, pmdf_ent);
    return 0;
}

int do_del_related_down(cluster_head_t **ppclst, spt_path seek_path, u32 dataid)
{
    u32 cur_vec, del_2_down, down;
    int direction;
    spt_md_entirety *pmdf_ent;
    spt_md_vec_t *pmdf_vec;
    spt_vec_t_r st_d2d_r, st_d_r, st_dr_r;
    spt_vec_t *pvec_d, *pvec_d2d, *pvec_dr;
    spt_del_data_node *pdel_data;
    spt_free_vec_node *pfree_vec;

    pmdf_ent = malloc(sizeof(spt_md_entirety));
    if(pmdf_ent == NULL)
    {
        assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return 1;
    }
    pmdf_ent->md_head = NULL;
    pmdf_ent->free_head = NULL;
    pmdf_ent->del_head = NULL;

    pmdf_vec = malloc(sizeof(spt_md_vec_t));
    if(pmdf_vec == NULL)
    {
        assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return 1;
    }

    del_2_down = seek_path.del_2_down;
    down = seek_path.down;
    direction = seek_path.direction;

    pvec_d = (spt_vec_t *)vec_id_2_ptr(*ppclst, down);
    get_final_vec(*ppclst, pvec_d, &st_d_r, -1);

    /*pos==0,�϶�û��down*/
    if(st_d_r.pos == 0)
    {
        pmdf_vec->vec_id = del_2_down;
        pmdf_vec->value.data = st_d_r.data;
        pmdf_vec->value.down = st_d_r.down;
        pmdf_vec->value.right = st_d_r.right;
        pmdf_vec->value.pos = 0;        
    }
    else
    {
        pvec_d2d = (spt_vec_t *)vec_id_2_ptr(*ppclst, del_2_down);
        get_final_vec(*ppclst, pvec_d2d, &st_d2d_r, -1);
        /*��������Ǵ�down������del_2_down������merge down->right*/
        /*del_2_down merge down and down->right*/
        if(direction == SPT_RIGHT && st_d_r.down == SPT_NULL)
        {
            pvec_dr = (spt_vec_t *)vec_id_2_ptr(*ppclst, st_d_r.right);
            get_final_vec(*ppclst, pvec_dr, &st_dr_r, st_d_r.data);
            pmdf_vec->vec_id = del_2_down;
            pmdf_vec->value.data = st_d_r.data;
            pmdf_vec->value.down = st_dr_r.down;
            pmdf_vec->value.right = st_dr_r.right;
            if(st_dr_r.pos == 0)
            {
                pmdf_vec->value.pos = 0;
            }
            else
            {
                pmdf_vec->value.pos = st_d2d_r.pos + st_d_r.pos + st_dr_r.pos;
            }
            /*free down->right*/
            pfree_vec = malloc(sizeof(spt_free_vec_node));
            if(pfree_vec == NULL)
            {
                assert(0);
                printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
                return 1;
            }
            pfree_vec->id = st_d_r.right;
            pfree_vec->next = pmdf_ent->free_head;
            pmdf_ent->free_head = pfree_vec;            
        }
        /*only merge down*/
        else
        {
            pmdf_vec->vec_id = del_2_down;
            pmdf_vec->value.data = st_d_r.data;
            pmdf_vec->value.down = st_d_r.down;
            pmdf_vec->value.right = st_d_r.right;
            pmdf_vec->value.pos = st_d2d_r.pos + st_d_r.pos;
        }
       
    }
    pmdf_vec->next = pmdf_ent->md_head;
    pmdf_ent->md_head = pmdf_vec;    

    pfree_vec = malloc(sizeof(spt_free_vec_node));
    if(pfree_vec == NULL)
    {
        assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return 1;
    }
    pfree_vec->id = st_d2d_r.right;
    pfree_vec->next = pmdf_ent->free_head;
    pmdf_ent->free_head = pfree_vec;

    pfree_vec = malloc(sizeof(spt_free_vec_node));
    if(pfree_vec == NULL)
    {
        assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return 1;
    }
    pfree_vec->id = down;
    pfree_vec->next = pmdf_ent->free_head;
    pmdf_ent->free_head = pfree_vec;

    if((*ppclst)->debug)
    {
        cur_vec = st_d2d_r.right;
        pvec_d = (spt_vec_t *)vec_id_2_ptr(*ppclst, cur_vec);
        get_final_vec(*ppclst, pvec_d, &st_d_r, -1);
        if(st_d_r.right != SPT_NULL)
        {
            printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
            return 1;
        }        
    }

    pdel_data = malloc(sizeof(spt_del_data_node));
    if(pdel_data == NULL)
    {
        assert(0);
        printf("\r\n%d\t%s", __LINE__, __FUNCTION__);
        return 1;
    }
    pdel_data->id = dataid;
    pdel_data->next = pmdf_ent->del_head;
    pmdf_ent->del_head = pdel_data;        

    do_modify(ppclst, pmdf_ent);
    return 0;
}



/*ret:1��ѯ������0ɾ���ɹ���-1����*/
int delete_data(cluster_head_t **ppclst, char *pdata)
{
    u32 dataid;
    u32 cur_vec, down_vec, right_vec, cur_data;
    spt_vec_t *pcur_vec, *pdown_vec, *pright_vec;
    char *pcur_data;//*ppre_data,
    u64 startbit, endbit, len, fs_pos;
    int direction, ret;
    vec_cmpret_t cmpres;
    spt_vec_t_r st_vec_r, st_right_r, st_down_r;
    spt_path seek_path = {-1,-1,-1,-1};

    startbit = endbit = 0;

    cur_vec = (*ppclst)->vec_head;
    cur_data = -1;

    pcur_vec = (spt_vec_t *)vec_id_2_ptr(*ppclst, cur_vec);
    get_final_vec(*ppclst, pcur_vec, &st_vec_r, cur_data);
    fs_pos = find_fs(pdata, startbit, DATA_BIT_MAX - startbit);

    if(fs_pos == 0 && st_vec_r.right == SPT_NULL)
    {
        return -1;
    }

    while(startbit<DATA_BIT_MAX)
    {
        /*��λ��1����pcur_vec->right��*/
        if(fs_pos == startbit)
        {
            if(cur_data == -1)//yzx
            {
                cur_data = st_vec_r.data;
                pcur_data = db_id_2_ptr(*ppclst, cur_data);
            }
            right_vec = st_vec_r.right;
            assert(right_vec != SPT_NULL);

            pright_vec = (spt_vec_t *)vec_id_2_ptr(*ppclst, right_vec);
            get_final_vec(*ppclst, pright_vec, &st_right_r, cur_data);
            len = (st_right_r.pos == 0)?(DATA_BIT_MAX - startbit):st_right_r.pos;         

            ret = diff_identify(pdata, pcur_data, startbit, len, &cmpres);
            if(ret == 0)
            {
                cur_vec = right_vec;
                st_vec_r = st_right_r;
                startbit += len;
                direction = SPT_RIGHT;
                if(st_vec_r.down != SPT_NULL)
                {
                    seek_path.del_2_down = cur_vec;
                    seek_path.down = st_vec_r.down;
                    /*����ǰһ���Ǵ��ҵ���del_2_down��*/
                    seek_path.direction = direction;
                }
                fs_pos = find_fs(pdata, startbit, DATA_BIT_MAX - startbit);
                continue;
            }
            else if(ret > 0)
            {
                return -1;
            }
            else
            {
                return -1;
            }
        }
        /*�϶���startbit����pcur->down��pos���ȱ�*/
        else
        {
            seek_path.del_2_down = SPT_NULL;
            seek_path.down = SPT_NULL;            
            cur_data = -1;
            while(fs_pos > startbit)
            {
                seek_path.up_2_del = cur_vec;
                seek_path.direction = direction;
                down_vec = st_vec_r.down;
                if(down_vec == SPT_NULL)
                {
                    return -1;
                }
                pdown_vec = (spt_vec_t *)vec_id_2_ptr(*ppclst, down_vec);
                get_final_vec(*ppclst, pdown_vec, &st_down_r, cur_data);
                len = (st_down_r.pos == 0)?(DATA_BIT_MAX - startbit):st_down_r.pos;
                /*insert*/
                if(fs_pos < startbit + len)
                {
                    return -1;
                }
                cur_vec = down_vec;
                st_vec_r = st_down_r;
                direction = SPT_DOWN;
                startbit += len;
            }
            assert(fs_pos == startbit);
            if(st_vec_r.down != SPT_NULL)
            {
                seek_path.del_2_down = cur_vec;
                seek_path.down = st_vec_r.down;
                seek_path.direction = direction;
            }            
        }
    }
    assert(startbit == DATA_BIT_MAX);
    dataid = st_vec_r.data;
    if(seek_path.down == SPT_NULL)
    {
        return do_del_isolate(ppclst,seek_path.up_2_del, seek_path.direction, dataid);
    }
    else
    {
        return do_del_related_down(ppclst, seek_path, dataid);
    }
}

int insert_data(cluster_head_t **ppclst, char *new_data)
{
    u32 cur_data, vecid_cur;
    spt_vec_t *ppre = NULL;
    spt_vec_t tmp_vec, cur_vec, pre_vec, *pcur;
    char *pcur_data;//*ppre_data,
    u64 startbit, endbit, len, fs_pos, signpost;
    
    u8 direction;
    u64 ret;
    vec_cmpret_t cmpres;
    insert_info_t st_insert_info;

insert_start:
    startbit = endbit = 0;
    signpost = 0;
    cur_data = SPT_INVALID;
    pcur = (spt_vec_t *)vec_id_2_ptr(*ppclst, (*ppclst)->vec_head);
    
    fs_pos = find_fs(new_data, startbit, DATA_BIT_MAX);
    
    while(startbit<DATA_BIT_MAX)
    {
        /*��λ��1����pcur_vec->right��*/
        if(fs_pos == startbit)
        {
            if(cur_data == -1)//yzx
            {
                cur_data = get_data_id(*ppclst, pcur);
                pcur_data = db_id_2_ptr(*ppclst, cur_data);
            }
            if(cur_vec.flag == SPT_VEC_FlAG_SIGNPOST)
            {
                signpost = cur_vec.idx << SPT_VEC_SIGNPOST_BIT;
                if(direction == SPT_DOWN)
                {
                    ///TODO: insert
                    st_insert_info.pkey_vec = pcur;
                    st_insert_info.key_val= cur_vec.val;
                    st_insert_info.signpost = signpost;
                    return do_insert_signpost_right(ppclst, &st_insert_info, new_data);
                }
            }
            if(cur_vec.flag == SPT_VEC_FLAG_DATA 
                    || (cur_vec.flag == SPT_VEC_FlAG_SIGNPOST 
                        && cur_vec.ext_sys_flg == SPT_VEC_SYS_FLAG_DATA))
            {
                len = DATA_BIT_MAX - startbit;
                ppre = pcur;
                pre_vec.val = cur_vec.val;            
                pcur = NULL;
                cur_vec = SPT_NULL;
                direction = SPT_RIGHT;                
            }
            else
            {
                ppre = pcur;
                pre_vec.val = cur_vec.val;            
                pcur = (spt_vec_t *)vec_id_2_ptr(*ppclst,pre_vec.rd);
                cur_vec = pcur->val;
                direction = SPT_RIGHT;
                while(1)
                {
                    if(cur_vec.valid == SPT_VEC_INVALID)
                    {
                        tmp_vec.val = pre_vec.val;
                        /*һ���Ǵ�right����������*/
                        if(cur_vec.flag == SPT_VEC_FlAG_SIGNPOST)
                        {
                            if(cur_vec.ext_sys_flg == SPT_VEC_SYS_FLAG_DATA)
                            {
                                spt_set_data_flag(tmp_vec);
                            }
                            tmp_vec.rd = cur_vec.rd;
                        }
                        else if(cur_vec.flag == SPT_VEC_FLAG_DATA)
                        {
                            if(cur_vec.down == SPT_NULL)
                            {
                                spt_set_data_flag(tmp_vec);
                                tmp_vec.rd = cur_vec.rd;
                            }
                            else
                            {
                                tmp_vec.rd = cur_vec.down;
                                cur_data = -1;
                            }                
                        }
                        else
                        {
                            tmp_vec.rd = cur_vec.rd;
                        }
                        vecid_cur = pre_vec.rd;
                        pre_vec.val = atomic64_cmpxchg((atomic64_t *)ppre, pre_vec.val, tmp_vec.val);
                        if(pre_vec.val == tmp_vec.val)//delete succ
                        {
                            vec_free_to_buf(*ppclst, vecid_cur);
                        }
                        /*���۽����ɹ����ֱ�����µ�pre_vec�����жϺʹ���*/
                        if(pre_vec.valid == SPT_VEC_INVALID)
                        {
                            /*��ͷ����*/
                            return SPT_DO_AGAIN;
                        }
                        if(pre_vec.flag == SPT_VEC_FLAG_DATA)
                        {
                            len = DATA_BIT_MAX - startbit;
                            break;
                        }
                        else if(pre_vec.flag == SPT_VEC_FLAG_RIGHT)
                        {
                            pcur = (spt_vec_t *)vec_id_2_ptr(*ppclst,pre_vec.rd);
                            cur_vec = *pcur;
                        }                
                        else if(pre_vec.flag == SPT_VEC_FlAG_SIGNPOST)
                        {
                            signpost = pre_vec.idx << SPT_VEC_SIGNPOST_BIT;
                            if(pre_vec.ext_sys_flg == SPT_VEC_SYS_FLAG_DATA)
                            {
                                len = DATA_BIT_MAX - startbit;
                                break;
                            }
                            pcur = (spt_vec_t *)vec_id_2_ptr(*ppclst,pre_vec.rd);
                            cur_vec = *pcur;
                        }
                        else
                        {
                            assert(0);
                            return SPT_ERR;
                        }
                    }
                    else
                    {
                        if(cur_vec.flag == SPT_VEC_FlAG_SIGNPOST)
                        {
                            len = (cur_vec.idx << SPT_VEC_SIGNPOST_BIT ) - startbit + 1;
                        }
                        else
                        {
                            len = cur_vec.pos + signpost - startbit + 1;
                        }
                        break;
                    }
                }                

            }
            ret = diff_identify(new_data, pcur_data, startbit, len, &cmpres);
            if(ret == 0)
            {
                startbit += len;
                /*find the same record*/
                if(startbit >= DATA_BIT_MAX)
                {
                    break;
                }
                ///TODO:startbit already >= DATA_BIT_MAX
                fs_pos = find_fs(new_data, startbit, DATA_BIT_MAX - startbit);
                continue;
            }
            /*insert up*/
            else if(ret > 0)
            {
                st_insert_info.pkey_vec= ppre;
                st_insert_info.key_val= pre_vec.val;
                st_insert_info.cmp_pos = cmpres.pos;
                st_insert_info.fs = cmpres.smallfs;
                st_insert_info.signpost = signpost;
                return do_insert_up_via_r(ppclst, &st_insert_info, new_data);
            }
            /*insert down*/
            else
            {
                st_insert_info.pkey_vec= ppre;
                st_insert_info.key_val= pre_vec.val;
                st_insert_info.cmp_pos = cmpres.pos;
                st_insert_info.fs = cmpres.smallfs;
                st_insert_info.signpost = signpost;
                return do_insert_down_via_r(ppclst, &st_insert_info, new_data);
            }        
        }
        else
        {
            cur_data = -1;
            
            while(fs_pos > startbit)
            {
                ppre = pcur;
                pre_vec.val = cur_vec.val;
                if(pre_vec.flag == SPT_VEC_FlAG_SIGNPOST)
                {
                    signpost = cur_vec.idx << SPT_VEC_SIGNPOST_BIT;
                    if(pre_vec.rd == SPT_NULL)
                    {
                        st_insert_info.pkey_vec= pcur;
                        st_insert_info.key_val= cur_vec.val;
                        st_insert_info.cmp_pos = cmpres.pos;
                        st_insert_info.fs = fs_pos;
                        st_insert_info.signpost = signpost;
                        return do_insert_last_down(ppclst, &st_insert_info, new_data);
                    }
                    else
                    {
                        pcur = (spt_vec_t *)vec_id_2_ptr(*ppclst,pre_vec.rd);
                    }
                }
                else
                {
                    if(pre_vec.down == SPT_NULL)
                    {
                        st_insert_info.pkey_vec= pcur;
                        st_insert_info.key_val= cur_vec.val;
                        st_insert_info.cmp_pos = cmpres.pos;
                        st_insert_info.fs = fs_pos;
                        st_insert_info.signpost = signpost;
                        return do_insert_last_down(ppclst, &st_insert_info, new_data);
                    }
                    else
                    {
                        pcur = (spt_vec_t *)vec_id_2_ptr(*ppclst,pre_vec.down);
                    }
                }
                cur_vec = pcur->val;
                direction = SPT_DOWN;
                while(cur_vec.valid == SPT_VEC_INVALID)
                {
                    tmp_vec.val = pre_vec.val;
                    /*һ���Ǵ�down����������*/
                    if(pre_vec.flag == SPT_VEC_FlAG_SIGNPOST)
                    {
                        if(cur_vec.flag == SPT_VEC_FlAG_SIGNPOST)
                        {
                            tmp_vec.rd = cur_vec.rd;
                        }
                        else
                        {
                            tmp_vec.rd = cur_vec.down;
                        }
                        vecid_cur = pre_vec.rd;
                    }
                    else
                    {
                        if(cur_vec.flag == SPT_VEC_FlAG_SIGNPOST)
                        {
                            tmp_vec.down = cur_vec.rd;
                        }
                        else
                        {
                            tmp_vec.down = cur_vec.down;
                        }
                        vecid_cur = pre_vec.down;
                    }
                    pre_vec.val = atomic64_cmpxchg((atomic64_t *)ppre, pre_vec.val, tmp_vec.val);
                    if(pre_vec.val == tmp_vec.val)//delete succ
                    {
                        vec_free_to_buf(*ppclst, vecid_cur);
                    }                    
                    /*���۽����ɹ����ֱ�����µ�pre_vec�����жϺʹ���*/
                    if(pre_vec.valid == SPT_VEC_INVALID)
                    {
                        /*��ͷ����*/
                        return insert_data(ppclst, new_data);
                    }
                    /*insert last down*/
                    if(pre_vec.flag == SPT_VEC_FlAG_SIGNPOST)
                    {
                        if(pre_vec.rd == SPT_NULL)
                        {
                            return //do_insert_last_down(ppclst, cur_vec, startbit, fs_pos, new_data);
                        }
                    }
                    else
                    {
                        if(pre_vec.down == SPT_NULL)
                        {
                            return //do_insert_last_down(ppclst, cur_vec, startbit, fs_pos, new_data);
                        }
                    }
                    pcur = (spt_vec_t *)vec_id_2_ptr(*ppclst,pre_vec.down);
                    cur_vec = pcur->val;
                }
                if(cur_vec.flag == SPT_VEC_FlAG_SIGNPOST)
                {
                    len = (cur_vec.idx << SPT_VEC_SIGNPOST_BIT ) - startbit + 1;
                }
                else
                {
                    len = cur_vec.pos + signpost - startbit + 1;
                }
                /*insert*/
                if(fs_pos < startbit + len)
                {
                    st_insert_info.cur_vec = cur_vec;
                    st_insert_info.startbit = startbit;
                    st_insert_info.pcur_vec_r = &st_vec_r;
                    st_insert_info.pn_vec_r = &st_down_r;
                    st_insert_info.len = len;
                    st_insert_info.fs = fs_pos;
                    return do_insert_up_via_d(ppclst, &st_insert_info,new_data);
                }
                startbit += len;
            }
            assert(fs_pos == startbit);
        }
    }

}

int insert_data(cluster_head_t **ppclst, char *new_data)
{
    u32 cur_vec, down_vec, right_vec, cur_data;
    spt_vec_t *pcur_vec, *pdown_vec, *pright_vec;
    char *pcur_data;//*ppre_data,
    u64 startbit, endbit, len, fs_pos;
    int ret;
    vec_cmpret_t cmpres;
    spt_vec_t_r st_vec_r, st_right_r, st_down_r;
    insert_info_t st_insert_info;

    startbit = endbit = 0;

    cur_vec = (*ppclst)->vec_head;
    cur_data = -1;

    pcur_vec = (spt_vec_t *)vec_id_2_ptr(*ppclst, cur_vec);
    get_final_vec(*ppclst, pcur_vec, &st_vec_r, cur_data);
    fs_pos = find_fs(new_data, startbit, DATA_BIT_MAX);
    /*�����׸���ʼλΪ1������*/
    /*û����ʼλΪ1������ʱ����������data = -1*/
    if(fs_pos == 0 && st_vec_r.data == -1)
    {
        do_insert_first_set(ppclst, cur_vec, new_data);
        return 0;
    }
    #if 0
    /*�����׸���ʼλΪ0������*/
    if(fs_pos != 0 && st_vec_r.down == SPT_NULL)
    {
        do_insert_last_down(ppclst, cur_vec, 0, fs_pos, new_data);
        return 0;
    }
    #endif
    while(startbit<DATA_BIT_MAX)
    {
        /*��λ��1����pcur_vec->right��*/
        if(fs_pos == startbit)
        {
            if(cur_data == -1)//yzx
            {
                cur_data = st_vec_r.data;
                pcur_data = db_id_2_ptr(*ppclst, cur_data);
            }
            right_vec = st_vec_r.right;
            assert(right_vec != SPT_NULL);

            pright_vec = (spt_vec_t *)vec_id_2_ptr(*ppclst, right_vec);
            get_final_vec(*ppclst, pright_vec, &st_right_r, cur_data);
            len = (st_right_r.pos == 0)?(DATA_BIT_MAX - startbit):st_right_r.pos;         

            ret = diff_identify(new_data, pcur_data, startbit, len, &cmpres);
            if(ret == 0)
            {
                cur_vec = right_vec;
                st_vec_r = st_right_r;
                startbit += len;
                ///TODO:startbit already >= DATA_BIT_MAX
                fs_pos = find_fs(new_data, startbit, DATA_BIT_MAX - startbit);
                continue;
            }
            else if(ret > 0)
            {
                st_insert_info.pcur_vec_r = &st_vec_r;
                st_insert_info.pn_vec_r = &st_right_r;
                st_insert_info.cur_vec = cur_vec;
                st_insert_info.startbit = startbit;
                st_insert_info.fs = cmpres.smallfs;
                st_insert_info.len = len;
                st_insert_info.cmp_pos = cmpres.pos;
                return do_insert_up_via_r(ppclst, &st_insert_info, new_data);
            }
            else
            {
                st_insert_info.pcur_vec_r = &st_vec_r;
                st_insert_info.pn_vec_r = &st_right_r;
                st_insert_info.cur_vec = cur_vec;
                st_insert_info.startbit = startbit;
                st_insert_info.fs = cmpres.smallfs;
                st_insert_info.len = len;
                st_insert_info.cmp_pos = cmpres.pos;                
                return do_insert_down_via_r(ppclst, &st_insert_info, new_data);
            }
        }
        /*�϶���startbit����pcur->down��pos���ȱ�*/
        else
        {
            cur_data = -1;
            while(fs_pos > startbit)
            {
                down_vec = st_vec_r.down;
                if(down_vec == SPT_NULL)
                {
                    return do_insert_last_down(ppclst, cur_vec, startbit, fs_pos, new_data);
                }
                pdown_vec = (spt_vec_t *)vec_id_2_ptr(*ppclst, down_vec);
                get_final_vec(*ppclst, pdown_vec, &st_down_r, cur_data);
                len = (st_down_r.pos == 0)?(DATA_BIT_MAX - startbit):st_down_r.pos;
                /*insert*/
                if(fs_pos < startbit + len)
                {
                    st_insert_info.cur_vec = cur_vec;
                    st_insert_info.startbit = startbit;
                    st_insert_info.pcur_vec_r = &st_vec_r;
                    st_insert_info.pn_vec_r = &st_down_r;
                    st_insert_info.len = len;
                    st_insert_info.fs = fs_pos;
                    return do_insert_up_via_d(ppclst, &st_insert_info,new_data);
                }
                cur_vec = down_vec;
                st_vec_r = st_down_r;
                startbit += len;
            }
            assert(fs_pos == startbit);
        }
    }
    assert(startbit == DATA_BIT_MAX);
    /*�ҵ�һ����ͬ��������st_vec_r��Ӧdata�����ü���+1*/
    printf("find the same data\r\n");
    return 0;     
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
    // //Ҫ�Ƚϵ�λ��������λ,Ҳ������Ϊ���һ���ֽڻ���Ҫ�Ƚ϶���λ
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
        /*���һ���ֽ��ж���ĵ�λbit����*/
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
            /*�޹�λ�ѱ�Ĩ������Ϊuca!=ucb�����Ա�Ȼ������Чλ���ҵ�set bit*/
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
            /*ָ��Χ���һλ*/
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
        /*��һ���ֽ��ж����bit����*/
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
     // //Ҫ�Ƚϵ�λ��������λ,Ҳ������Ϊ���һ���ֽڻ���Ҫ�Ƚ϶���λ
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
        /*���һ���ֽ��ж���ĵ�λbit����*/
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
            /*�޹�λ�ѱ�Ĩ������Ϊuca!=ucb�����Ա�Ȼ������Чλ���ҵ�set bit*/
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
            /*ָ��Χ���һλ*/
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
        /*��һ���ֽ��ж����bit����*/
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
/*bitend: �Ƚϲ�������λ*/
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
        fs = ucfind_firt_set(cres);//fs�����ܴ���bitend;
        /*�����ȣ����ص�����һ��Ҫ�Ƚϵ�λ�����ȣ����ص�����һλ���ֵĲ���*/
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
    
        /*�����С������ͬλ���first set*/
        result->smallfs = result->pos;
        uca = uca << fs;
        bitend = bitend - fs;
        fs = ucfind_firt_set(uca);
        /*bitend���λ�Ѿ�Ĩ��*/
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
        /*��һ���ֽ��ж����bit����*/
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
            /*�����Ļ��������ȣ����ص�����һ��Ҫ�Ƚϵ�λ�����ȣ����ص�����һλ���ֵĲ���*/
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
                    /*��һ�������ȼ����ж� bitend == 0 && lenbyte == align*/
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



void debug_vec_print(spt_vec_t_r *pvec_r, int vec_id)
{
    printf("{down:%d, right:%d, pos:%lld, data:%d}[%d]\r\n",pvec_r->down, pvec_r->right, pvec_r->pos, pvec_r->data, vec_id);
    return;
}

void debug_id_vec_print(cluster_head_t *pclst, int id)
{
    spt_vec_t *pcur_vec;
    spt_vec_t_r st_vec_r;
    
    pcur_vec = (spt_vec_t *)vec_id_2_ptr(pclst, id);
    get_final_vec(pclst, pcur_vec, &st_vec_r, -1);
    debug_vec_print(&st_vec_r, id);
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

void debug_travl_stack_push(spt_stack *p_stack, spt_vec_t_r *pvec_r, int 
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
    spt_vec_t *pcur_vec;
    u8 *pcur_data = NULL;
    u64 bit=0, pos;
    spt_vec_t_r st_vec_r;
    travl_info *pnode;
    u16 rank, *prsv;
 //   int count=0;

    spt_stack_init(pstack, 100);

    cur_vec = pclst->vec_head;
    pcur_vec = (spt_vec_t *)vec_id_2_ptr(pclst, cur_vec);
    get_final_vec(pclst, pcur_vec, &st_vec_r, -1);    
    
    if(st_vec_r.down == SPT_NULL && st_vec_r.right == SPT_NULL)
    {
        printf("cluster is null\r\n");
        return;
    }
    rank = pclst->used_dblk_cnt;
    cur_data = st_vec_r.data;//û����λΪ1��dataʱ��cur_data����-1��bug
    /*��ʱright�϶�Ϊ��*/
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
            pcur_vec = (spt_vec_t *)vec_id_2_ptr(pclst, cur_vec);
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
            //�������ݱ�����
            //spt_bit_cpy(data, pcur_data, bit, DATA_BIT_MAX - bit);
            //��ʱdataӦ�õ��ڲ����ĳ����
            if(pcur_data != NULL)//head->rightΪ��ʱ��pcur_data���ڿ�
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
                    //���������ڴ棬ֱ����ջ
                    spt_stack_push(pstack,(int)pnode);
                    //spt_bit_clear(data, bit - pnode->vec_r.pos, pnode->vec_r.pos);
                    cur_vec = pnode->vec_r.down;
                    pcur_vec = (spt_vec_t *)vec_id_2_ptr(pclst, cur_vec);
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
    spt_vec_t *pcur_vec;
    u8 *pcur_data;
    u64 bit=0;
    spt_vec_t_r st_vec_r;
    travl_outer_info *pnode;
    u16 rank;
    travl_q tq = {0};
    FILE *fp;
    char buf[1024]={0};
    int j = 0;

    cur_vec = pclst->vec_head;
    pcur_vec = (spt_vec_t *)vec_id_2_ptr(pclst, cur_vec);
    get_final_vec(pclst, pcur_vec, &st_vec_r, -1);    

    if(st_vec_r.down == SPT_NULL && st_vec_r.right == SPT_NULL)
    {
        printf("cluster is null\r\n");
        return;
    }
    
    fp = fopen("file.txt","w+");
    cur_data = st_vec_r.data;
    /*û����λ��1��data*/
    if(cur_data == -1)
    {
        cur_vec = st_vec_r.down;
    }

    while(cur_vec != SPT_NULL)
    {
        pcur_vec = (spt_vec_t *)vec_id_2_ptr(pclst, cur_vec);
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
            pcur_vec = (spt_vec_t *)vec_id_2_ptr(pclst, cur_vec);
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

int test_insert(cluster_head_t **ppclst, char *test_case)
{

}
char *acmp;
char *bbcmp;
vec_cmpret_t *rcmp;
cluster_head_t *pgclst;

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

typedef union spt_vec1
{
    struct normal_vec
    {
        unsigned long long flag:     4;
        unsigned long long pos:      14; /*至少4字节才够right or dataid or next*/
        unsigned long long rd:       23;    /*8��ʾ��һ����������rdn��ʾnext��1��ʾrdn��right��0����rdn��dataid*/
        unsigned long long down:     23;
    }normal;
    struct sign_vec
    {
        unsigned long long flag:        4;
        unsigned long long ext_sys_flg:           6;
        unsigned long long ext_usr_flg:           6;
        unsigned long long dr:                  23;
        unsigned long long pos:                 25;
    }sign;
    unsigned long long data;
}spt_vec_t1;


int main()
{
    spt_vec_t1 vec;

    vec.data = 0x123456789abcdef0;

    while(1)
    {

    }

#if 0

    FILE *fp;
    char buf[1024]={0};
    int j;

    memcpy(buf, "hahahahahahahahahaha", 20);
    printf("%s\r\n", buf);

    j = sprintf(buf, "%s\n", "hello");
    j = sprintf(buf+j, "%s\n", "world");
    printf("%s [j:%d]\r\n", buf, j);

    fp = fopen("file.txt","w+");
    j = sprintf(buf, "%d\t%d\r\n", 100,222);
    j += sprintf(buf+j, "%d\t%d\r\n", 500,312);
    j += sprintf(buf+j, "%d\t%d\r\n", 66,767);
    j += sprintf(buf+j, "\r\n");
    
    printf("%s [j:%d]", buf, j);

    fwrite(buf, j, 1, fp);

    j = sprintf(buf, "%d\t%d\r\n", 5,6);
    j += sprintf(buf+j, "%d\t%d\r\n", 32,78);
    j += sprintf(buf+j, "%d\t%d\r\n", 52,897);
    j += sprintf(buf+j, "\r\n");
    
    fwrite(buf, j, 1, fp);
    fclose(fp);
#endif
    return 0;
}
#endif


