#ifndef MZMETRICS_HASHTABLE_COUNTERS
#define MZMETRICS_HASHTABLE_COUNTERS

#include "mzmetrics_counters_interface.h"

#ifdef __cplusplus
extern "C" {
#endif

struct hashtable_env;
struct hashtable_env *create_hashtable(unsigned int *num_counters,
                                        size_t num_families);
void delete_hashtables(struct hashtable_env *env);

struct GrpCounter *lookup_counter(struct hashtable_env *env,
                                  char *resource_name, size_t family);
int insert_counter(struct hashtable_env *env, char *resource_name,
                   struct GrpCounter *gcounter, size_t family);
void get_all_counters(struct hashtable_env *env, unsigned char *data,
                      size_t family, unsigned int options,
                      unsigned int ncounters);
size_t get_size(struct hashtable_env *env, size_t family);

#ifdef __cplusplus
}
#endif

#endif  /* MZMETRICS_HASHTABLE_COUNTERS */
