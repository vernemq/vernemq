#include "mzmetrics.h"

int metrics_init(unsigned int num_cpus)
{
    return capi_init_counters(num_cpus);
}

void metrics_deinit(void)
{
    capi_deinit_counters();
}

counter64_t metrics_update_counter(struct GrpCounter *counter, unsigned int key, int val)
{
    return capi_update_counter(counter, key, val);
}

counter64_t metrics_decr_counter(struct GrpCounter *counter, unsigned int key)
{
    return capi_decr_counter(counter, key);
}

counter64_t metrics_incr_counter(struct GrpCounter *counter, unsigned int key)
{
    return capi_incr_counter(counter, key);
}

counter64_t metrics_get_counter_value(struct GrpCounter *counter, unsigned int key)
{
    return capi_getval_counter(counter, key);
}

counter64_t metrics_reset_counter_value(struct GrpCounter *counter, unsigned int key)
{
    return capi_resetval_counter(counter, key);
}

struct GrpCounter* metrics_alloc_counter(size_t num)
{
    struct GrpCounter *counter = capi_alloc_counter(num);
    metrics_incr_refcnt_counter(counter);
    return counter;
}

void metrics_free_counter(struct GrpCounter *counter)
{
    capi_free_counter(counter);
}
