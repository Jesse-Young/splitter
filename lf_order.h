#ifndef _LF_ORDER_H
#define _LF_ORDER_H

#define ORDER_TOKEN (-1)
#define ORDER_DISCARD (-2)

#ifndef unlikely
#define unlikely 
#endif

#define LF_DEBUG

typedef struct { unsigned long pd; } lxd_t;

typedef struct
{
    u64 *l0_fd;
    u64 *l1_fd;
    u64 *l2_fd;
    u64 *l3_fd;
    u64 *l4_fd;
    u64 *l5_fd;
    u64 *l6_fd;
    u64 *l7_fd;

    u64 *local_oid; //order_id
    u64 **local_l0_pg;
    u64 newest_pg;
    u32 thd_num;
    u32 pg_max;
    u32 pg_num;
}orderq_h_t;

#define PTRS_PER_LEVEL 512

#define lxd_none(lxd) (lxd == 0)
#define oids_in_same_page(oid1, oid2)  ((oid1^oid2) < 512)

#define l7_index(oid)   (oid >> 63)
#define l6_index(oid)   ((oid >> 54)&(PTRS_PER_LEVEL - 1))
#define l5_index(oid)   ((oid >> 45)&(PTRS_PER_LEVEL - 1))
#define l4_index(oid)   ((oid >> 36)&(PTRS_PER_LEVEL - 1))
#define l3_index(oid)   ((oid >> 27)&(PTRS_PER_LEVEL - 1))
#define l2_index(oid)   ((oid >> 18)&(PTRS_PER_LEVEL - 1))
#define l1_index(oid)   ((oid >> 9)&(PTRS_PER_LEVEL - 1))
#define l0_index(oid)   ((oid)&(PTRS_PER_LEVEL - 1))

#define l6b_index_zero(oid)   ((oid & (l6_PTRS_PER_PG-1)) == 0)
#define l5b_index_zero(oid)   ((oid & (l5_PTRS_PER_PG-1)) == 0)
#define l4b_index_zero(oid)   ((oid & (l4_PTRS_PER_PG-1)) == 0)
#define l3b_index_zero(oid)   ((oid & (l3_PTRS_PER_PG-1)) == 0)
#define l2b_index_zero(oid)   ((oid & (l2_PTRS_PER_PG-1)) == 0)
#define l1b_index_zero(oid)   ((oid & (l1_PTRS_PER_PG-1)) == 0)
#define l0b_index_zero(oid)   ((oid & (l0_PTRS_PER_PG-1)) == 0)

#define l0_PTRS_PER_PG (1ul<<9)
#define l1_PTRS_PER_PG (1ul<<18)
#define l2_PTRS_PER_PG (1ul<<27)
#define l3_PTRS_PER_PG (1ul<<36)
#define l4_PTRS_PER_PG (1ul<<45)
#define l5_PTRS_PER_PG (1ul<<54)
#define l6_PTRS_PER_PG (1ul<<63)

#define LO_OK 0
#define LO_OOM 1

#define LO_REUSE_FAIL 2



#define lforder_debug(f, a...)	{ \
					printf ("LFORD DEBUG (%s, %d): %s:", \
						__FILE__, __LINE__, __func__); \
				  	printf (f, ## a); \
					}


extern orderq_h_t *lfo_q_init(int thread_num);
extern int lfo_inq(orderq_h_t *oq, int thread, u64 cmd, u64 oid);
extern u64 lfo_deq(orderq_h_t *oq, int thread, u64 oid);

#endif
