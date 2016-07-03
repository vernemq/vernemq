// This is a wrapper around standard c++ unordered_map
#include <unordered_map>
#include <cassert>
#include <cstring>
#include <string>
#include "mzmetrics.h"
#include "mzmetrics_hashtable_counters.h"

// FIXME: Please rename me
static const uint64_t g_mult_threshold = 50;
typedef std::unordered_map<std::string, struct GrpCounter*> counters_map_t;

struct hashtable_env {
    counters_map_t **counters_map;
    size_t num_families;
};

static inline counters_map_t* my_counters_map(struct hashtable_env *env,
                                                size_t family)
{
    assert(family < env->num_families);
    return env->counters_map[family];
}

extern "C"
struct hashtable_env* create_hashtable(unsigned int *num_counters,
                                        size_t num_families)
{
    counters_map_t **counters_map;
    counters_map = new counters_map_t*[num_families];
    assert(counters_map);
    for (size_t i = 0; i < num_families; i++) {
        counters_map[i] = new counters_map_t(num_counters[i] * g_mult_threshold);
        assert(counters_map[i]);
    }
    struct hashtable_env *env = new hashtable_env();
    env->num_families = num_families;
    env->counters_map = counters_map;
    return env;
}

extern "C"
void delete_hashtables(struct hashtable_env *env)
{
    for (size_t i = 0; i < env->num_families; i++) {
        delete env->counters_map[i];
        env->counters_map[i] = NULL;
    }
    delete[] env->counters_map;
    delete env;
}

extern "C"
struct GrpCounter* lookup_counter(struct hashtable_env *env, char *resource_name, size_t family)
{
    auto counters_map = my_counters_map(env, family);
    counters_map_t::const_iterator entry = counters_map->find(resource_name);
    if (entry == counters_map->end())
        return NULL;
    return entry->second;
}

extern "C"
int insert_counter(struct hashtable_env *env, char *resource_name,
        struct GrpCounter* gcounter, size_t family)
{
    auto counters_map = my_counters_map(env, family);
    counters_map_t::const_iterator entry = counters_map->find(resource_name);
    if (entry != counters_map->end())
        return -1;
    (*counters_map)[resource_name] = gcounter;
    return 0;
}

extern "C"
size_t get_size(struct hashtable_env *env, size_t family)
{
    return my_counters_map(env, family)->size();
}

extern "C"
void get_all_counters(struct hashtable_env *env, unsigned char *data,
                        size_t family, unsigned int options,
                        unsigned int ncounters)
{
    unsigned int offset = 0;
    auto counters_map = my_counters_map(env, family);
    auto metrics_val_fn = metrics_get_counter_value;
    if (options == 1)
        metrics_val_fn = metrics_reset_counter_value;
    // Iterate over all the elements and get counter values
    for (auto local_iter = counters_map->begin();
            local_iter != counters_map->end(); ) {
        //add at offset & increment offset
        //
        strcpy((char *) data + offset, local_iter->first.c_str());
        offset += MAX_RESOURCE_NAME_SIZE;
        auto grp_counter = local_iter->second;
        counter64_t *counter_data = (counter64_t *)(data + offset);
        assert(grp_counter->num_cnt <= ncounters);
        for (unsigned int j = 0; j < grp_counter->num_cnt; j++) {
            *counter_data = metrics_val_fn(grp_counter, j);
            counter_data++;
        }
        local_iter++;
        offset += max_cntr_data_per_resource(ncounters);
    }
}
