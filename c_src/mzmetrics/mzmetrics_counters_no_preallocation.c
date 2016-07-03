#define _POSIX_C_SOURCE 200112L
#ifdef __linux__
#define _DEFAULT_SOURCE
#include <sched.h>
#endif
#include <cpuid.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>

#include "mzmetrics_counters_interface.h"
#define CACHELINE_BYTES 64U

static size_t num_cpus = 1;

int capi_init_counters(size_t ncpus)
{
#ifdef PCPU_COUNTERS
    num_cpus = ncpus;
#else
    num_cpus = 1;
#endif
    return 0;
}

void capi_deinit_counters(void)
{
    return;
}

int mycpu(void)
{
#ifdef PCPU_COUNTERS
#ifdef __linux__
    return sched_getcpu();
#else
    unsigned int eax, ebx, ecx, edx;
    if (!__get_cpuid(1, &eax, &ebx, &ecx, &edx))
        return 0;
    return 0;
#endif
#else
    return 0;
#endif
}

// num - number of counters in this group
struct GrpCounter* capi_alloc_counter(size_t num)
{
    size_t alloc_size = num * sizeof(counter64_t);
    assert(0 == alloc_size % CACHELINE_BYTES);
    struct GrpCounter *gcounter = calloc(1, sizeof(*gcounter));
    assert(gcounter);
    posix_memalign((void **)&gcounter->counter, CACHELINE_BYTES, alloc_size * num_cpus);
    assert(gcounter->counter);
    memset(gcounter->counter, 0, alloc_size * num_cpus);
    gcounter->num_cnt = num;
    gcounter->ref_cnt = 1;
    return gcounter;
}

void capi_free_counter(struct GrpCounter *gcounter)
{
    //assert on ref_cnt
    free(gcounter->counter);
    free(gcounter);
}

// offset of the counter in this group that needs to be updated
counter64_t capi_update_counter(struct GrpCounter *gcounter, size_t offset, int val)
{
    if (val > 0)
        return __atomic_add_fetch(gcounter->counter + mycpu() *
                            gcounter->num_cnt + offset, val, __ATOMIC_SEQ_CST);
    else
        return __atomic_sub_fetch(gcounter->counter + mycpu() *
                            gcounter->num_cnt + offset, -val, __ATOMIC_SEQ_CST);
}

// offset of the counter in this group that needs to be incremented
counter64_t capi_incr_counter(struct GrpCounter *gcounter, size_t offset)
{
    return __atomic_add_fetch(gcounter->counter + mycpu() *
                            gcounter->num_cnt + offset, 1, __ATOMIC_SEQ_CST);
}

// offset of the counter in this group that needs to be decremented
counter64_t capi_decr_counter(struct GrpCounter *gcounter, size_t offset)
{
    return __atomic_sub_fetch(gcounter->counter + mycpu() *
                            gcounter->num_cnt + offset, 1, __ATOMIC_SEQ_CST);
}

// getval for counters
counter64_t capi_getval_counter(struct GrpCounter *gcounter, size_t offset)
{
    counter64_t ret = 0;
#ifndef PCPU_COUNTERS
    __atomic_load(gcounter->counter + mycpu() *
                            gcounter->num_cnt + offset, &ret, __ATOMIC_SEQ_CST);
#else
    for (size_t i = 0; i < num_cpus; i++) {
        counter64_t temp;
        __atomic_load(gcounter->counter + i *
                            gcounter->num_cnt + offset, &temp, __ATOMIC_SEQ_CST);
        ret += temp;
    }
#endif
    return ret;
}

// exchange value for counters
counter64_t capi_resetval_counter(struct GrpCounter *gcounter, size_t offset)
{
    counter64_t ret = 0;
#ifndef PCPU_COUNTERS
    ret = __atomic_exchange_n(gcounter->counter + mycpu() *
                            gcounter->num_cnt + offset, 0ULL, __ATOMIC_SEQ_CST);
#else
    for (size_t i = 0; i < num_cpus; i++) {
        ret += __atomic_exchange_n(gcounter->counter + i *
                            gcounter->num_cnt + offset, 0ULL, __ATOMIC_SEQ_CST);
    }
#endif
    return ret;
}
