#define _GNU_SOURCE
#include <sched.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <pthread.h>
#include <sys/sysinfo.h>
#include <sys/types.h>
#include <sys/ipc.h>  
#include <sys/shm.h>  
#include <errno.h>
#include <signal.h>
#include<sys/wait.h>
#include <sys/stat.h> /* For mode constants */
#include <fcntl.h> /* For O_* constants */



#include <atomic_user.h>
#include <lf_order.h>
orderq_h_t *goqh;
__thread u64 token_at_id;
__thread u64 *token_at_entry;
u64 send_total = 0;


static inline void *alloc_page()
{
    void *p;
    p = malloc(4096);
    if(p != 0)
    {
        memset(p, 0, 4096);
        smp_mb();
    }
    return p;
}

static inline int pg_test_zero(u64 *p)
{
    int i;
    for(i=0;i < 512;i++)
    {
        if(*p != 0)
            return -1;
        p++;
    }
    return 0;
}

static inline void free_page(void *page)
{
    free(page);
    return;
}
//static
int __lxd_alloc(u64 *lxud, orderq_h_t *oq)
{
    u64 *new_pg, *pg;
    new_pg = (u64 *)alloc_page();
    if(new_pg == NULL)
    {
        lforder_debug("OOM\n");
        return LO_OOM;
    }
    pg = (u64 *)atomic64_cmpxchg((atomic64_t *)lxud, 0, (long)new_pg);
    if(pg != 0)
    {
        free_page(new_pg);
    }
    else
    {
        atomic_add(1, (atomic_t *)&oq->pg_num);
    }
    return LO_OK;
}

static inline u64 *l0d_offset(u64 *lxud, u64 oid)
{
	return (u64 *)(*lxud) + l0_index(oid);
}
static inline u64 *l1d_offset(u64 *lxud, u64 oid)
{
	return (u64 *)(*lxud) + l1_index(oid);
}
static inline u64 *l2d_offset(u64 *lxud, u64 oid)
{
	return (u64 *)(*lxud) + l2_index(oid);
}
static inline u64 *l3d_offset(u64 *lxud, u64 oid)
{
	return (u64 *)(*lxud) + l3_index(oid);
}
static inline u64 *l4d_offset(u64 *lxud, u64 oid)
{
	return (u64 *)(*lxud) + l4_index(oid);
}
static inline u64 *l5d_offset(u64 *lxud, u64 oid)
{
	return (u64 *)(*lxud) + l5_index(oid);
}
static inline u64 *l6d_offset(u64 *lxud, u64 oid)
{
	return (u64 *)(*lxud) + l6_index(oid);
}
static inline u64 *l7d_offset(u64 *lxud, u64 oid)
{
	return (u64 *)(*lxud) + l7_index(oid);
}

static inline u64 *l0d_alloc(u64 *lxud, u64 oid, orderq_h_t *oq)
{
    smp_mb();
	return (unlikely(lxd_none(*lxud)) && __lxd_alloc(lxud, oq))?
		NULL: l0d_offset(lxud, oid);
}
static inline u64 *l1d_alloc(u64 *lxud, u64 oid, orderq_h_t *oq)
{
    smp_mb();
	return (unlikely(lxd_none(*lxud)) && __lxd_alloc(lxud, oq))?
		NULL: l1d_offset(lxud, oid);
}
static inline u64 *l2d_alloc(u64 *lxud, u64 oid, orderq_h_t *oq)
{
    smp_mb();
	return (unlikely(lxd_none(*lxud)) && __lxd_alloc(lxud, oq))?
		NULL: l2d_offset(lxud, oid);
}
static inline u64 *l3d_alloc(u64 *lxud, u64 oid, orderq_h_t *oq)
{
    smp_mb();
	return (unlikely(lxd_none(*lxud)) && __lxd_alloc(lxud, oq))?
		NULL: l3d_offset(lxud, oid);
}
static inline u64 *l4d_alloc(u64 *lxud, u64 oid, orderq_h_t *oq)
{
    smp_mb();
	return (unlikely(lxd_none(*lxud)) && __lxd_alloc(lxud, oq))?
		NULL: l4d_offset(lxud, oid);
}
static inline u64 *l5d_alloc(u64 *lxud, u64 oid, orderq_h_t *oq)
{
    smp_mb();
	return (unlikely(lxd_none(*lxud)) && __lxd_alloc(lxud, oq))?
		NULL: l5d_offset(lxud, oid);
}
static inline u64 *l6d_alloc(u64 *lxud, u64 oid, orderq_h_t *oq)
{
    smp_mb();
	return (unlikely(lxd_none(*lxud)) && __lxd_alloc(lxud, oq))?
		NULL: l6d_offset(lxud, oid);
}
static inline u64 *l7d_alloc(u64 *lxud, u64 oid, orderq_h_t *oq)
{
    smp_mb();
	return (unlikely(lxd_none(*lxud)) && __lxd_alloc(lxud, oq))?
		NULL: l7d_offset(lxud, oid);
}



orderq_h_t *lfo_q_init(int thread_num)
{
    orderq_h_t *oq = NULL;
    int i;

    oq = (orderq_h_t *)malloc(sizeof(orderq_h_t) + sizeof(u64)*thread_num + sizeof(u64 *)*thread_num);
    if(oq == NULL)
    {
        lforder_debug("OOM\n");
        return NULL;
    }
    memset((void *)oq, 0, sizeof(orderq_h_t));

    oq->thd_num = thread_num;

    oq->l0_fd = (u64 *)alloc_page();
    if(oq->l0_fd == NULL)
    {
        free(oq);
        lforder_debug("OOM\n");
        return NULL;
    }
    memset((void *)oq->l0_fd, 0, 4096);
    oq->pg_num = 1;
    oq->l0_fd[0] = ORDER_TOKEN;

    oq->local_oid = (u64 *)((u64)oq + sizeof(orderq_h_t));
    memset((void *)oq->local_oid, 0, sizeof(u64)*thread_num);

    oq->local_l0_pg = (u64 **)((u64)oq->local_oid + sizeof(u64)*thread_num);
    for(i=0; i<thread_num; i++)
    {
        oq->local_l0_pg[i] = oq->l0_fd;
    }
    oq->pg_max = 100;
    return oq;
}

u64 *dq_get_l0_pg(orderq_h_t *oq, int thread, u64 oid)
{
    u64 *lv1d,*lv2d,*lv3d,*lv4d,*lv5d,*lv6d, *lv7d;
    u64 last_oid;
    last_oid = oq->local_oid[thread];
    oq->local_oid[thread] = oid;
    if(oids_in_same_page(oid, last_oid))
    {
        oq->local_oid[thread] = oid;//可以不赋值，等到换页时再赋值
        return oq->local_l0_pg[thread];
    }

    if(oid < l0_PTRS_PER_PG)
    {
        oq->local_l0_pg[thread] = oq->l0_fd;
        oq->local_oid[thread] = oid;
        return oq->local_l0_pg[thread];
    }
    else if(oid < l1_PTRS_PER_PG)
    {
        lv2d = (u64 *)&oq->l1_fd;
        goto get_lv1d;
    }
    else if(oid < l2_PTRS_PER_PG)
    {
        lv3d = (u64 *)&oq->l2_fd;
        goto get_lv2d;
    }        
    else if(oid < l3_PTRS_PER_PG)
    {
        lv4d = (u64 *)&oq->l3_fd;
        goto get_lv3d;
    }        
    else if(oid < l4_PTRS_PER_PG)
    {
        lv5d = (u64 *)&oq->l4_fd;
        goto get_lv4d;
    }        
    else if(oid < l5_PTRS_PER_PG)
    {
        lv6d = (u64 *)&oq->l5_fd;
        goto get_lv5d;
    }
    else if(oid < l6_PTRS_PER_PG) 
    {
        lv7d = (u64 *)&oq->l6_fd;
        goto get_lv6d;
    }
    else
    {
        goto get_lv7d;
    }

get_lv7d:    
    lv7d = l7d_alloc((u64 *)&oq->l7_fd, oid, oq);
    if(lv7d == NULL)
    {
        lforder_debug("OOM\n");
        return NULL;
    }
get_lv6d:        
    lv6d = l6d_alloc(lv7d, oid, oq);
    if(lv6d == NULL)
    {
        lforder_debug("OOM\n");
        return NULL;
    }
get_lv5d:
    lv5d = l5d_alloc(lv6d, oid, oq);
    if(lv5d == NULL)
    {
        lforder_debug("OOM\n");
        return NULL;
    }
get_lv4d:
    lv4d = l4d_alloc(lv5d, oid, oq);
    if(lv4d == NULL)
    {
        lforder_debug("OOM\n");
        return NULL;
    }
get_lv3d:        
    lv3d = l3d_alloc(lv4d, oid, oq);
    if(lv3d == NULL)
    {
        lforder_debug("OOM\n");
        return NULL;
    }
get_lv2d:
    lv2d = l2d_alloc(lv3d, oid, oq);
    if(lv2d == NULL)
    {
        lforder_debug("OOM\n");
        return NULL;
    }
get_lv1d:
    lv1d = l1d_alloc(lv2d, oid, oq);
    if(lv1d == NULL)
    {
        lforder_debug("OOM\n");
        return NULL;
    }

    if(unlikely(lxd_none(*lv1d)) && unlikely(__lxd_alloc(lv1d, oq)))
    {
        lforder_debug("OOM\n");
        return NULL;
    }   
    oq->local_l0_pg[thread] = (u64 *)*lv1d;
    return (u64 *)*lv1d;
}


//static
u64 *get_l0_pg(orderq_h_t *oq, int thread, u64 oid)
{
    u64 *lv1d,*lv2d,*lv3d,*lv4d,*lv5d,*lv6d, *lv7d;
    u64 last_oid;
    last_oid = oq->local_oid[thread];
    oq->local_oid[thread] = oid;
    if(oids_in_same_page(oid, last_oid))
    {
        oq->local_oid[thread] = oid;//可以不赋值，等到换页时再赋值
        return oq->local_l0_pg[thread];
    }

    if(oid < l0_PTRS_PER_PG)
    {
        oq->local_l0_pg[thread] = oq->l0_fd;
        oq->local_oid[thread] = oid;
        return oq->local_l0_pg[thread];
    }
    else if(oid < l1_PTRS_PER_PG)
    {
        lv2d = (u64 *)&oq->l1_fd;
        goto get_lv1d;
    }
    else if(oid < l2_PTRS_PER_PG)
    {
        lv3d = (u64 *)&oq->l2_fd;
        goto get_lv2d;
    }        
    else if(oid < l3_PTRS_PER_PG)
    {
        lv4d = (u64 *)&oq->l3_fd;
        goto get_lv3d;
    }        
    else if(oid < l4_PTRS_PER_PG)
    {
        lv5d = (u64 *)&oq->l4_fd;
        goto get_lv4d;
    }        
    else if(oid < l5_PTRS_PER_PG)
    {
        lv6d = (u64 *)&oq->l5_fd;
        goto get_lv5d;
    }
    else if(oid < l6_PTRS_PER_PG) 
    {
        lv7d = (u64 *)&oq->l6_fd;
        goto get_lv6d;
    }
    else
    {
        goto get_lv7d;
    }

get_lv7d:    
    lv7d = l7d_alloc((u64 *)&oq->l7_fd, oid, oq);
    if(lv7d == NULL)
    {
        lforder_debug("OOM\n");
        return NULL;
    }
get_lv6d:        
    lv6d = l6d_alloc(lv7d, oid, oq);
    if(lv6d == NULL)
    {
        lforder_debug("OOM\n");
        return NULL;
    }
get_lv5d:
    lv5d = l5d_alloc(lv6d, oid, oq);
    if(lv5d == NULL)
    {
        lforder_debug("OOM\n");
        return NULL;
    }
get_lv4d:
    lv4d = l4d_alloc(lv5d, oid, oq);
    if(lv4d == NULL)
    {
        lforder_debug("OOM\n");
        return NULL;
    }
get_lv3d:        
    lv3d = l3d_alloc(lv4d, oid, oq);
    if(lv3d == NULL)
    {
        lforder_debug("OOM\n");
        return NULL;
    }
get_lv2d:
    lv2d = l2d_alloc(lv3d, oid, oq);
    if(lv2d == NULL)
    {
        lforder_debug("OOM\n");
        return NULL;
    }
get_lv1d:
    lv1d = l1d_alloc(lv2d, oid, oq);
    if(lv1d == NULL)
    {
        lforder_debug("OOM\n");
        return NULL;
    }

    if(unlikely(lxd_none(*lv1d)) && unlikely(__lxd_alloc(lv1d, oq)))
    {
        lforder_debug("OOM\n");
        return NULL;
    }   
    oq->local_l0_pg[thread] = (u64 *)*lv1d;
    oq->newest_pg = oid >> 9;
    return (u64 *)*lv1d;
}

int reuse_pg(orderq_h_t *oq, void *pg)
{
    u64 *lv1d,*lv2d,*lv3d,*lv4d,*lv5d,*lv6d, *lv7d, *ret;
    u64 oid;

#ifdef LF_DEBUG
    if(pg_test_zero(pg) != 0)
    {
        lforder_debug("find no zero reuse_pg\n");
        while(1)
        {
            sleep(1);
        }
    }
#endif

    oid = (oq->newest_pg + 1)<<9;
    
    if(oid < l1_PTRS_PER_PG)
    {
        lv2d = (u64 *)&oq->l1_fd;
        goto get_lv1d;
    }
    else if(oid < l2_PTRS_PER_PG)
    {
        lv3d = (u64 *)&oq->l2_fd;
        goto get_lv2d;
    }        
    else if(oid < l3_PTRS_PER_PG)
    {
        lv4d = (u64 *)&oq->l3_fd;
        goto get_lv3d;
    }        
    else if(oid < l4_PTRS_PER_PG)
    {
        lv5d = (u64 *)&oq->l4_fd;
        goto get_lv4d;
    }        
    else if(oid < l5_PTRS_PER_PG)
    {
        lv6d = (u64 *)&oq->l5_fd;
        goto get_lv5d;
    }
    else if(oid < l6_PTRS_PER_PG) 
    {
        lv7d = (u64 *)&oq->l6_fd;
        goto get_lv6d;
    }
    else
    {
        goto get_lv7d;
    }

get_lv7d: 
    if(lxd_none(oq->l7_fd))
    {
        ret = (u64 *)atomic64_cmpxchg((atomic64_t *)&oq->l7_fd, 0, (long)pg);
        if(ret != 0)
        {
            return LO_REUSE_FAIL;
        }
        return LO_OK;
    }
    lv7d = l7d_offset((u64 *)&oq->l7_fd, oid);

get_lv6d:        
    if(lxd_none(*lv7d))
    {
        ret = (u64 *)atomic64_cmpxchg((atomic64_t *)lv7d, 0, (long)pg);
        if(ret != 0)
        {
            return LO_REUSE_FAIL;
        }
        return LO_OK;
    }
    lv6d = l6d_offset(lv7d, oid);

get_lv5d:
    if(lxd_none(*lv6d))
    {
        ret = (u64 *)atomic64_cmpxchg((atomic64_t *)lv6d, 0, (long)pg);
        if(ret != 0)
        {
            return LO_REUSE_FAIL;
        }
        return LO_OK;
    }
    lv5d = l5d_offset(lv6d, oid);

get_lv4d:
    if(lxd_none(*lv5d))
    {
        ret = (u64 *)atomic64_cmpxchg((atomic64_t *)lv5d, 0, (long)pg);
        if(ret != 0)
        {
            return LO_REUSE_FAIL;
        }
        return LO_OK;
    }
    lv4d = l4d_offset(lv5d, oid);
get_lv3d:        
    if(lxd_none(*lv4d))
    {
        ret = (u64 *)atomic64_cmpxchg((atomic64_t *)lv4d, 0, (long)pg);
        if(ret != 0)
        {
            return LO_REUSE_FAIL;
        }
        return LO_OK;
    }
    lv3d = l3d_offset(lv4d, oid);
get_lv2d:
    if(lxd_none(*lv3d))
    {
        ret = (u64 *)atomic64_cmpxchg((atomic64_t *)lv3d, 0, (long)pg);
        if(ret != 0)
        {
            return LO_REUSE_FAIL;
        }
        return LO_OK;
    }
    lv2d = l2d_offset(lv3d, oid);

get_lv1d:

    if(lxd_none(*lv2d))
    {
        ret = (u64 *)atomic64_cmpxchg((atomic64_t *)lv2d, 0, (long)pg);
        if(ret != 0)
        {
            return LO_REUSE_FAIL;
        }
        return LO_OK;
    }
    lv1d = l1d_offset(lv2d, oid);
    if(lxd_none(*lv1d))
    {
        ret = (u64 *)atomic64_cmpxchg((atomic64_t *)lv1d, 0, (long)pg);
        if(ret != 0)
        {
            return LO_REUSE_FAIL;
        }
        return LO_OK;
    }
    return LO_REUSE_FAIL;
}

void _deal_finished_pg(orderq_h_t *oq, void *pg)
{
    if((oq->pg_num < oq->pg_max) && (LO_OK == reuse_pg(oq, pg)))
    {
        return;
    }
    free_page((void *)pg);
    atomic_sub(1, (atomic_t *)&oq->pg_num);    
    return;
}

/*oid 低9bit肯定为0*/
void lfo_deal_finished_pgs(orderq_h_t *oq, u64 oid)
{
    u64 *lv1d,*lv2d,*lv3d,*lv4d,*lv5d,*lv6d, *lv7d, *pg, *next_pg;

    if(oid < l1_PTRS_PER_PG)
    {
        if(oid == l0_PTRS_PER_PG)
        {
            lv1d = (u64 *)&oq->l0_fd;
        }
        else
        {
            lv1d = l1d_offset((u64 *)&oq->l1_fd, oid-1);       
        }
        pg = (u64 *)*lv1d;
        *lv1d = 0;
        _deal_finished_pg(oq, (void *)pg);        
    }
    else if(oid < l2_PTRS_PER_PG)
    {
        if(oid == l1_PTRS_PER_PG)
        {
            lv2d = (u64 *)&oq->l1_fd;
            lv1d = l1d_offset(lv2d, oid-1);
            pg = (u64 *)*lv2d;
            *lv2d = 0;
            next_pg = (u64 *)*lv1d;
            *lv1d = 0;
            _deal_finished_pg(oq, (void *)pg);
        }
        else
        {
            lv2d = l2d_offset((u64 *)&oq->l2_fd, oid-1);
            lv1d = l1d_offset(lv2d, oid-1);
            if(l1b_index_zero(oid) == 1)
            {                
                pg = (u64 *)*lv2d;
                *lv2d = 0;
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
                _deal_finished_pg(oq, (void *)pg);
            }
            else
            {
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
            }
        }
        _deal_finished_pg(oq, (void *)next_pg);
    }        
    else if(oid < l3_PTRS_PER_PG)
    {
        if(oid == l2_PTRS_PER_PG)
        {
            lv3d = (u64 *)&oq->l2_fd;
            lv2d = l2d_offset(lv3d, oid-1);
            lv1d = l1d_offset(lv2d, oid-1);
            pg = (u64 *)*lv3d;
            *lv3d = 0;
            next_pg = (u64 *)*lv2d;
            *lv2d = 0;
            _deal_finished_pg(oq, (void *)pg);
            pg = next_pg;
            next_pg = (u64 *)*lv1d;
            *lv1d = 0;
            _deal_finished_pg(oq, (void *)pg);
        }
        else
        {
            lv3d = l3d_offset((u64 *)&oq->l3_fd, oid-1);
            lv2d = l2d_offset(lv3d, oid-1);
            lv1d = l1d_offset(lv2d, oid-1);            
            if(l2b_index_zero(oid) == 1)
            {                
                pg = (u64 *)*lv3d;
                *lv3d = 0;
                next_pg = (u64 *)*lv2d;
                *lv2d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
                _deal_finished_pg(oq, (void *)pg);
            }
            else if(l1b_index_zero(oid) == 1)
            {                
                pg = (u64 *)*lv2d;
                *lv2d = 0;
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
                _deal_finished_pg(oq, (void *)pg);
            }
            else
            {
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
            }
        }
        _deal_finished_pg(oq, (void *)next_pg);
    }        
    else if(oid < l4_PTRS_PER_PG)
    {
        if(oid == l3_PTRS_PER_PG)
        {
            lv4d = (u64 *)&oq->l3_fd;
            lv3d = l3d_offset(lv4d, oid-1);
            lv2d = l2d_offset(lv3d, oid-1);
            lv1d = l1d_offset(lv2d, oid-1);
            pg = (u64 *)*lv4d;
            *lv4d = 0;
            next_pg = (u64 *)*lv3d;
            *lv3d = 0;
            _deal_finished_pg(oq, (void *)pg);
            pg = next_pg;
            next_pg = (u64 *)*lv2d;
            *lv2d = 0;
            _deal_finished_pg(oq, (void *)pg);
            pg = next_pg;
            next_pg = (u64 *)*lv1d;
            *lv1d = 0;
            _deal_finished_pg(oq, (void *)pg);
        }
        else
        {
            lv4d = l4d_offset((u64 *)&oq->l4_fd, oid-1);
            lv3d = l3d_offset(lv4d, oid-1);
            lv2d = l2d_offset(lv3d, oid-1);
            lv1d = l1d_offset(lv2d, oid-1);
            if(l3b_index_zero(oid) == 1)
            {                
                pg = (u64 *)*lv4d;
                *lv4d = 0;
                next_pg = (u64 *)*lv3d;
                *lv3d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv2d;
                *lv2d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
                _deal_finished_pg(oq, (void *)pg);

            }            
            else if(l2b_index_zero(oid) == 1)
            {                
                pg = (u64 *)*lv3d;
                *lv3d = 0;
                next_pg = (u64 *)*lv2d;
                *lv2d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
                _deal_finished_pg(oq, (void *)pg);
            }
            else if(l1b_index_zero(oid) == 1)
            {                
                pg = (u64 *)*lv2d;
                *lv2d = 0;
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
                _deal_finished_pg(oq, (void *)pg);
            }
            else
            {
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
            }
        }
        _deal_finished_pg(oq, (void *)next_pg);
    }        
    else if(oid < l5_PTRS_PER_PG)
    {
        if(oid == l4_PTRS_PER_PG)
        {
            lv5d = (u64 *)&oq->l4_fd;
            lv4d = l4d_offset(lv5d, oid-1);
            lv3d = l3d_offset(lv4d, oid-1);
            lv2d = l2d_offset(lv3d, oid-1);
            lv1d = l1d_offset(lv2d, oid-1);
            pg = (u64 *)*lv5d;
            *lv5d = 0;
            next_pg = (u64 *)*lv4d;
            *lv4d = 0;
            _deal_finished_pg(oq, (void *)pg);
            pg = next_pg;
            next_pg = (u64 *)*lv3d;
            *lv3d = 0;
            _deal_finished_pg(oq, (void *)pg);
            pg = next_pg;
            next_pg = (u64 *)*lv2d;
            *lv2d = 0;
            _deal_finished_pg(oq, (void *)pg);
            pg = next_pg;
            next_pg = (u64 *)*lv1d;
            *lv1d = 0;
            _deal_finished_pg(oq, (void *)pg);
        }
        else
        {
            lv5d = l5d_offset((u64 *)&oq->l5_fd, oid-1);
            lv4d = l4d_offset(lv5d, oid-1);
            lv3d = l3d_offset(lv4d, oid-1);
            lv2d = l2d_offset(lv3d, oid-1);
            lv1d = l1d_offset(lv2d, oid-1);            
            if(l4b_index_zero(oid) == 1)
            {
                pg = (u64 *)*lv5d;
                *lv5d = 0;
                next_pg = (u64 *)*lv4d;
                *lv4d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv3d;
                *lv3d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv2d;
                *lv2d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
                _deal_finished_pg(oq, (void *)pg);
            }
            else if(l3b_index_zero(oid) == 1)
            {                
                pg = (u64 *)*lv4d;
                *lv4d = 0;
                next_pg = (u64 *)*lv3d;
                *lv3d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv2d;
                *lv2d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
                _deal_finished_pg(oq, (void *)pg);

            }            
            else if(l2b_index_zero(oid) == 1)
            {                
                pg = (u64 *)*lv3d;
                *lv3d = 0;
                next_pg = (u64 *)*lv2d;
                *lv2d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
                _deal_finished_pg(oq, (void *)pg);
            }
            else if(l1b_index_zero(oid) == 1)
            {                
                pg = (u64 *)*lv2d;
                *lv2d = 0;
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
                _deal_finished_pg(oq, (void *)pg);
            }
            else
            {
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
            }
        }
        _deal_finished_pg(oq, (void *)next_pg);
    }
    else if(oid < l6_PTRS_PER_PG) 
    {
        if(oid == l5_PTRS_PER_PG)
        {
            lv6d = (u64 *)&oq->l5_fd;
            lv5d = l5d_offset(lv6d, oid-1);
            lv4d = l4d_offset(lv5d, oid-1);
            lv3d = l3d_offset(lv4d, oid-1);
            lv2d = l2d_offset(lv3d, oid-1);
            lv1d = l1d_offset(lv2d, oid-1);
            pg = (u64 *)*lv6d;
            *lv6d = 0;
            next_pg = (u64 *)*lv5d;
            *lv5d = 0;
            _deal_finished_pg(oq, (void *)pg);
            pg = next_pg;
            next_pg = (u64 *)*lv4d;
            *lv4d = 0;
            _deal_finished_pg(oq, (void *)pg);
            pg = next_pg;
            next_pg = (u64 *)*lv3d;
            *lv3d = 0;
            _deal_finished_pg(oq, (void *)pg);
            pg = next_pg;
            next_pg = (u64 *)*lv2d;
            *lv2d = 0;
            _deal_finished_pg(oq, (void *)pg);
            pg = next_pg;
            next_pg = (u64 *)*lv1d;
            *lv1d = 0;
            _deal_finished_pg(oq, (void *)pg);
        }
        else
        {
            lv6d = l6d_offset((u64 *)&oq->l6_fd, oid-1);
            lv5d = l5d_offset(lv6d, oid-1);
            lv4d = l4d_offset(lv5d, oid-1);
            lv3d = l3d_offset(lv4d, oid-1);
            lv2d = l2d_offset(lv3d, oid-1);
            lv1d = l1d_offset(lv2d, oid-1);
            if(l5b_index_zero(oid) == 1)
            {
                pg = (u64 *)*lv6d;
                *lv6d = 0;
                next_pg = (u64 *)*lv5d;
                *lv5d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv4d;
                *lv4d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv3d;
                *lv3d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv2d;
                *lv2d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
                _deal_finished_pg(oq, (void *)pg);

            }            
            else if(l4b_index_zero(oid) == 1)
            {
                pg = (u64 *)*lv5d;
                *lv5d = 0;
                next_pg = (u64 *)*lv4d;
                *lv4d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv3d;
                *lv3d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv2d;
                *lv2d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
                _deal_finished_pg(oq, (void *)pg);
            }
            else if(l3b_index_zero(oid) == 1)
            {                
                pg = (u64 *)*lv4d;
                *lv4d = 0;
                next_pg = (u64 *)*lv3d;
                *lv3d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv2d;
                *lv2d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
                _deal_finished_pg(oq, (void *)pg);

            }            
            else if(l2b_index_zero(oid) == 1)
            {                
                pg = (u64 *)*lv3d;
                *lv3d = 0;
                next_pg = (u64 *)*lv2d;
                *lv2d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
                _deal_finished_pg(oq, (void *)pg);
            }
            else if(l1b_index_zero(oid) == 1)
            {                
                pg = (u64 *)*lv2d;
                *lv2d = 0;
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
                _deal_finished_pg(oq, (void *)pg);
            }
            else
            {
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
            }
        }
        _deal_finished_pg(oq, (void *)next_pg);
    }
    else
    {
        if(oid == l6_PTRS_PER_PG)
        {
            lv7d = (u64 *)&oq->l6_fd;
            lv6d = l6d_offset(lv7d, oid-1);
            lv5d = l5d_offset(lv6d, oid-1);
            lv4d = l4d_offset(lv5d, oid-1);
            lv3d = l3d_offset(lv4d, oid-1);
            lv2d = l2d_offset(lv3d, oid-1);
            lv1d = l1d_offset(lv2d, oid-1);
            pg = (u64 *)*lv7d;
            *lv7d = 0;
            next_pg = (u64 *)*lv6d;
            *lv6d = 0;
            _deal_finished_pg(oq, (void *)pg);
            pg = next_pg;
            next_pg = (u64 *)*lv5d;
            *lv5d = 0;
            _deal_finished_pg(oq, (void *)pg);
            pg = next_pg;
            next_pg = (u64 *)*lv4d;
            *lv4d = 0;
            _deal_finished_pg(oq, (void *)pg);
            pg = next_pg;
            next_pg = (u64 *)*lv3d;
            *lv3d = 0;
            _deal_finished_pg(oq, (void *)pg);
            pg = next_pg;
            next_pg = (u64 *)*lv2d;
            *lv2d = 0;
            _deal_finished_pg(oq, (void *)pg);
            pg = next_pg;
            next_pg = (u64 *)*lv1d;
            *lv1d = 0;
            _deal_finished_pg(oq, (void *)pg);
        }
        else
        {
            lv7d = l7d_offset((u64 *)&oq->l7_fd, oid-1);
            lv6d = l6d_offset(lv7d, oid-1);
            lv5d = l5d_offset(lv6d, oid-1);
            lv4d = l4d_offset(lv5d, oid-1);
            lv3d = l3d_offset(lv4d, oid-1);
            lv2d = l2d_offset(lv3d, oid-1);
            lv1d = l1d_offset(lv2d, oid-1);
            if(l6b_index_zero(oid) == 1)
            {
                pg = (u64 *)*lv7d;
                *lv7d = 0;
                next_pg = (u64 *)*lv6d;
                *lv6d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv5d;
                *lv5d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv4d;
                *lv4d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv3d;
                *lv3d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv2d;
                *lv2d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
                _deal_finished_pg(oq, (void *)pg);
            }                        
            else if(l5b_index_zero(oid) == 1)
            {
                pg = (u64 *)*lv6d;
                *lv6d = 0;
                next_pg = (u64 *)*lv5d;
                *lv5d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv4d;
                *lv4d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv3d;
                *lv3d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv2d;
                *lv2d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
                _deal_finished_pg(oq, (void *)pg);

            }            
            else if(l4b_index_zero(oid) == 1)
            {
                pg = (u64 *)*lv5d;
                *lv5d = 0;
                next_pg = (u64 *)*lv4d;
                *lv4d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv3d;
                *lv3d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv2d;
                *lv2d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
                _deal_finished_pg(oq, (void *)pg);
            }
            else if(l3b_index_zero(oid) == 1)
            {                
                pg = (u64 *)*lv4d;
                *lv4d = 0;
                next_pg = (u64 *)*lv3d;
                *lv3d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv2d;
                *lv2d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
                _deal_finished_pg(oq, (void *)pg);

            }            
            else if(l2b_index_zero(oid) == 1)
            {                
                pg = (u64 *)*lv3d;
                *lv3d = 0;
                next_pg = (u64 *)*lv2d;
                *lv2d = 0;
                _deal_finished_pg(oq, (void *)pg);
                pg = next_pg;
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
                _deal_finished_pg(oq, (void *)pg);
            }
            else if(l1b_index_zero(oid) == 1)
            {                
                pg = (u64 *)*lv2d;
                *lv2d = 0;
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
                _deal_finished_pg(oq, (void *)pg);
            }
            else
            {
                next_pg = (u64 *)*lv1d;
                *lv1d = 0;
            }
        }
        _deal_finished_pg(oq, (void *)next_pg);
    }
    return;
}

int lfo_inq(orderq_h_t *oq, int thread, u64 cmd, u64 oid)
{
    u64 *cur_pg, *entry;
    int idx;

    cur_pg = get_l0_pg(oq, thread, oid);
    if(cur_pg == NULL)
    {
        lforder_debug("OOM\n");
        return LO_OOM;
    }
    idx = l0_index(oid);
    entry = cur_pg + idx;
    *entry = cmd;
    return LO_OK;
}

u64 lfo_deq(orderq_h_t *oq, int thread, u64 oid)
{
    u64 *cur_pg, *entry, ret;
    int idx;
    
    cur_pg = dq_get_l0_pg(oq, thread, oid);
    if(cur_pg == NULL)
    {
        lforder_debug("OOM\n");
        assert(0);
        return 0;
    }
    idx = l0_index(oid);
    entry = cur_pg + idx;
    
    while(*entry == 0)
    {
        smp_mb();
    }
    ret = *entry;
    *entry = 0;
    idx++;
    oid++;
    if(idx == PTRS_PER_LEVEL)
    {
        lfo_deal_finished_pgs(oq, oid);
    }        
    return ret;
}

#ifdef LF_DEBUG
void *send_s(void *arg)
{
    cpu_set_t mask;
    int i, j, thread_id;
    u64 order_id;
//    int (*inq)(lfrwq_t* , void*);
    u64 start, end;
    i = (long)arg;
    if(i<8)
        j=0;
    else
        j=1;
    j = i;
    CPU_ZERO(&mask); 
    CPU_SET(i,&mask);

    if (sched_setaffinity(0, sizeof(mask), &mask) == -1)
    {
        printf("warning: could not set CPU affinity, continuing...\n");
    }
    
    printf("writefn%d,start\n",i);

    thread_id = i-1;
    rdtscll(start);

    for(order_id=thread_id;order_id<0x40000000;order_id += 2)
    {

    }
    rdtscll(end);
    end = end - start;
    lforder_debug("write%d cycle: %lld\n", j,end);
    while(1)
    {
        sleep(1);
    }
    return 0;    
}

int main3(int argc, char **argv)  
{
    long num;
    int err;
    pthread_t ntid;
    cpu_set_t mask;

    CPU_ZERO(&mask); 

    goqh = lfo_q_init(2);
    
#if 1
    for(num=1; num <3; num++)
    {
        err = pthread_create(&ntid, NULL, send_s, (void *)num);
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


