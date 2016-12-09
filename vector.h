#ifndef _SPLITTER_VECTOR_H
#define _SPLITTER_VECTOR_H


typedef unsigned char u8;
typedef unsigned char u8;
typedef unsigned short u16;
typedef unsigned int u32;
typedef unsigned long long u64;
typedef long long s64;

////////////////////////////////////////
#define SPT_VEC_FLAG_RIGHT      0
#define SPT_VEC_FLAG_DATA       1
#define SPT_VEC_FlAG_SIGNPOST   2

#define SPT_VEC_VALID       0
#define SPT_VEC_INVALID     1     

#define SPT_VEC_SIGNPOST_BIT    14
#define SPT_VEC_SIGNPOST_MASK   ((1ul<<SPT_VEC_SIGNPOST_BIT)-1)

///////////////////////////////

#define SPT_VEC_POS_INDEX_BIT    8
#define SPT_VEC_POS_INDEX_MASK 0x3F00
#define SPT_VEC_POS_MULTI_MASK 0xFF

#define SPT_VEC_SYS_FLAG_DATA      1

#define spt_get_pos_index(x)                ((x&SPT_VEC_POS_INDEX_MASK)>>SPT_VEC_POS_INDEX_BIT)
#define spt_get_pos_multi(x)                (x&SPT_VEC_POS_MULTI_MASK)
#define spt_set_pos(pos,index,multi)        (pos = (index << SPT_VEC_POS_INDEX_BIT)|(multi))


#define POW256(x)    (1<<(8*x))

#define spt_set_invalid_flag(x)        (x.valid = SPT_VEC_INVALID)
#define spt_set_right_flag(x)        (x.flag = SPT_VEC_FLAG_RIGHT)
#define spt_set_data_flag(x)        (x.flag = SPT_VEC_FLAG_DATA)


typedef struct chunk_head
{
    unsigned short vec_head;
    unsigned short vec_free_head;
    unsigned short blk_free_head;
    unsigned short dblk_free_head;
}CHK_HEAD;

typedef struct cluster_head
{
    unsigned int vec_head;
    volatile unsigned int vec_free_head;
    volatile unsigned int blk_free_head;
    volatile unsigned int dblk_free_head;

    volatile unsigned int vec_buf_head;
    volatile unsigned int dblk_buf_head;
    
    unsigned int pg_num_max;
    unsigned int pg_num_total;
    unsigned int pg_cursor;
    unsigned int blk_per_pg_bits;
//    unsigned int db_per_pg_bits;
//    unsigned int vec_per_pg_bits;
    unsigned int pg_ptr_bits;
    unsigned int blk_per_pg;
    unsigned int db_per_blk;
    unsigned int vec_per_blk;    
    
    unsigned int free_blk_cnt;
    unsigned int free_vec_cnt;
    unsigned int free_dblk_cnt;
    unsigned int used_dblk_cnt;
    unsigned int debug;
    
    volatile char *pglist[0];
}cluster_head_t;

typedef struct block_head
{
    unsigned int magic;
    unsigned int next;
}block_head_t;

typedef struct db_head_t
{
    unsigned int magic;
    unsigned int next;
}db_head_t;

typedef struct spt_data_hd
{
    int ref;/*引用计数*/
    int rank;/*for test*/
}spt_dhd;


typedef struct spt_vec
{
    union
    {
        volatile unsigned long long val;
        struct 
        {
            volatile unsigned long long valid:      1;
            volatile unsigned long long flag:       3;
            volatile unsigned long long pos:        14;
            volatile unsigned long long down:       23;
            volatile unsigned long long rd:         23;    
        };
        struct 
        {
            volatile unsigned long long dummy_flag:         4;
            volatile unsigned long long ext_sys_flg:        6;
            volatile unsigned long long ext_usr_flg:        6;
            volatile unsigned long long idx:                25;
            volatile unsigned long long dummy_rd:           23;
        };
    };
}spt_vec_t;

typedef struct spt_vec_real
{
    int down;
    int right;
    int data;
    long long pos;
}spt_vec_t_r;

typedef struct vec_head
{
    unsigned int magic;
    unsigned int next;
}vec_head_t;

typedef struct vec_cmpret
{
    /*smalldata first set小数在比较向量范围内，如果全零则为比较范围外的下一位?*/
    /*pos位之前比较结果为相同的位可以为零也可以不为零*/
    u64 smallfs;    
//    u64 bigfs;         /*bigdata first set 相对于开始比较的位的位置，没用?*/
    u64 pos;        /*比较到哪一位开始不等，如果两数相等，则指向比较范围外的下一位?*/
    u32 flag;        /*相同的位是否全零，1表示全零*/
    u32 finish;
}vec_cmpret_t;

typedef struct spt_query_info
{
    spt_vec *start_vec;
    u64 signpost;
    char *data;
    u64 endbit;
    u8  op;
}query_info_t;


typedef struct spt_insert_info
{
    spt_vec *pkey_vec;
    u64 key_val;
    u64 signpost;
    u64 startbit;
    u64 fs;
    u64 cmp_pos;
    u64 len;
}insert_info_t;


/*修改完成后需要释放内存*/
typedef struct spt_modify_vec
{
    unsigned int            vec_id;/*需要修改的向量id*/
    spt_vec_t_r                value;/*要修改的向量新值*/
    struct spt_modify_vec    *next;
}spt_md_vec_t;

typedef struct spt_free_vector_node
{
    int                            id;
    struct spt_free_vector_node    *next;
}spt_free_vec_node;

typedef struct spt_del_data_node_st
{
    int                                id;
    struct spt_del_data_node_st    *next;
}spt_del_data_node;


/*修改完成后需要释放内存*/
///TODO: del data need to free data
typedef struct spt_modify_entirety
{
    spt_md_vec_t        *md_head;/*需要修改的向量链表*/
    spt_free_vec_node    *free_head;
    spt_del_data_node    *del_head;//需要释放的数据
}spt_md_entirety;


typedef struct spt_seek_path_node
{    
    char                            direction;
    int                                vec_id;
    char*                            pvec;
    struct spt_seek_path_node*        next;
}spt_path_node;
#if 0
typedef struct spt_seek_path
{
    int                up_first;/*上一层的第一个向量(向量关系的上一层，不是数据关系)*/
    int             up_2_del;/*拐点*/
    int                del_first;
    int                del_2_down;/*拐点*/
    int                down;/*数据关系紧挨着的下一层的第一个向量*/
//    spt_path_node*        next;
}spt_path;
#else
typedef struct spt_seek_path2
{
    int up_2_del;/*拐点*/
    int del_2_down;/*拐点*/
    int down;/*数据关系紧挨着的下一层的第一个向量*//*应该解释为和当前数据有直接向量关联关系*/ 
    int direction;
}spt_path2;

typedef struct spt_seek_path
{
    spt_vec *pkey_vec[2];
//    u64 key_val[2];
    u64 signpost[2];
}spt_path;

#endif

typedef struct spt_stack_st
{
    int *p_top;    /* 栈顶指针 */
    int *p_bottom; /* 栈底指针 */
    int stack_size;    /* 栈大小 */
}spt_stack;

typedef struct spt_traversal_info_st
{
    spt_vec_t_r vec_r;
    int direction;
}travl_info;

typedef struct spt_xy_st
{
    long long x;
    long long y;
}spt_xy;

typedef struct spt_outer_travl_info_st
{
    int pre;
    int cur;
    int data;
    spt_xy xy_pre;
    struct spt_outer_travl_info_st *next;
}travl_outer_info;

typedef struct spt_travl_q_st
{
    travl_outer_info *head;
    travl_outer_info *tail;
}travl_q;

typedef struct spt_test_file_head
{
    int data_size;
    int data_num;
    char rsv[24];
}test_file_h;


//#define DBLK_BITS 3
#define DATA_SIZE 8
#define RSV_SIZE 2
#define DBLK_SIZE (DATA_SIZE + RSV_SIZE)
#define VBLK_BITS 3
#define VBLK_SIZE (1<<VBLK_BITS)
#define DATA_BIT_MAX (DATA_SIZE*8)

//#define vec_id_2_ptr(pchk, id)    ((char *)pchk+id*VBLK_SIZE);

unsigned int vec_alloc(cluster_head_t **ppcluster, spt_vec_t **vec);
void vec_free(cluster_head_t *pcluster, int id);
void vec_list_free(cluster_head_t *pcluster, int id);
void db_free(cluster_head_t *pcluster, int id);



#endif
