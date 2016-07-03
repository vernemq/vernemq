#ifndef MZMETRICS_COUNTERS_INTERFACE_H
#define MZMETRICS_COUNTERS_INTERFACE_H

#include <stdint.h>
#include <stddef.h>

/*
 * This is the interface to actually allocate memory for the counters.
 * Assuming that we will have two kinds of counter allocation
 * 1) pre-allocate a pool of memory and manage it
 * 2) allocate memory on the fly per resource
 * pcpu_counters implements 1 but doesn't have the memory management part
 * Implementing approach 2 is easier, going with it for now
 *
 * Another consideration is that for our use case it's better to have group of
 * counters rather than just one counter and having another wrapper around it.
 */
#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t counter64_t;

struct GrpCounter {
    counter64_t *counter; //reference for this group
    size_t num_cnt; // number of counters in this group
    size_t ref_cnt; //reference count on this counter
};

int capi_init_counters(size_t ncpus);
void capi_deinit_counters(void);
// num - number of counters in this group
struct GrpCounter* capi_alloc_counter(size_t num);
void capi_free_counter(struct GrpCounter *counter);
// offset of the counter in this group that needs to be incremented
counter64_t capi_incr_counter(struct GrpCounter *counter, size_t offset);
// offset of the counter in this group that needs to be decremented
counter64_t capi_decr_counter(struct GrpCounter *counter, size_t offset);
// offset of the counter in this group that needs to be updated
counter64_t capi_update_counter(struct GrpCounter *counter, size_t offset, int val);
// getval for counters
counter64_t capi_getval_counter(struct GrpCounter *counter, size_t offset);
// getval & reset for counters
counter64_t capi_resetval_counter(struct GrpCounter *counter, size_t offset);

#ifdef __cplusplus
}
#endif

#endif  /* MZMETRICS_COUNTERS_INTERFACE_H */
