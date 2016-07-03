#ifndef MZMETRICS_H
#define MZMETRICS_H

#include <stdint.h>
#include "mzmetrics_counters_interface.h"

/*
 * The resource names will be prepended before the counter values when we
 * try to get all the counter values for every resource the family.
 * We'll be naive and just have a fixed size prefix before the counters,
 * and put the resource name there. This is not compact, but good for now.
 * However, this does put a system limit on the maximum resource name size.
 */
#define MAX_RESOURCE_NAME_SIZE (1000)

#ifdef __cplusplus
extern "C" {
#endif

static inline size_t max_cntr_data_per_resource(unsigned int ncounters)
{
    return sizeof(counter64_t) * ncounters;
}

static inline size_t max_data_per_resource(unsigned int ncounters)
{
    return MAX_RESOURCE_NAME_SIZE + max_cntr_data_per_resource(ncounters);
}

int metrics_init(unsigned int num_cpus);
void metrics_deinit(void);
counter64_t metrics_incr_counter(struct GrpCounter *counter, unsigned int id);
counter64_t metrics_decr_counter(struct GrpCounter *counter, unsigned int id);
counter64_t metrics_update_counter(struct GrpCounter *counter,
                                    unsigned int key, int val);
counter64_t metrics_get_counter_value(struct GrpCounter *counter, unsigned int id);
counter64_t metrics_reset_counter_value(struct GrpCounter *counter, unsigned int id);
struct GrpCounter* metrics_alloc_counter(size_t num);
void metrics_free_counter(struct GrpCounter *counter);
static inline void metrics_incr_refcnt_counter(struct GrpCounter *counter)
{
    __atomic_add_fetch(&counter->ref_cnt, 1, __ATOMIC_SEQ_CST);
}

static inline void metrics_decr_refcnt_counter(struct GrpCounter *counter)
{
   __atomic_sub_fetch(&counter->ref_cnt, 1, __ATOMIC_SEQ_CST);
}

static inline size_t metrics_refcnt_counter(struct GrpCounter *counter)
{
    return __atomic_load_n(&counter->ref_cnt, __ATOMIC_SEQ_CST);
}

#ifdef __cplusplus
}
#endif

#endif  /* MZMETRICS_H */
