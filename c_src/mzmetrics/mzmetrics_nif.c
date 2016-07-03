#include <string.h>
#include <assert.h>

#include "mzmetrics.h"
#include "mzmetrics_hashtable_counters.h"
#include "erl_nif.h"

static const char *g_my_mod_name = "mzmetrics";
static const char *g_resource_res_name = "counters";

struct metrics_resource {
    ErlNifResourceType *res_type;
    ErlNifResourceDtor *res_dtor;
};

struct metrics_env {
    ErlNifRWLock **hashtable_lock;
    unsigned int num_families;
    unsigned int *num_resources;
    void *hashtable_env;
    struct metrics_resource metrics_module;
};

static struct metrics_resource my_metrics_module(ErlNifEnv *env)
{
    struct metrics_env* priv= enif_priv_data(env);
    return priv->metrics_module;
}

static ErlNifRWLock **my_rwlocks(ErlNifEnv *env)
{
    struct metrics_env* priv= enif_priv_data(env);
    return priv->hashtable_lock;
}

static unsigned int my_num_families(ErlNifEnv *env)
{
    struct metrics_env* priv= enif_priv_data(env);
    return priv->num_families;
}

static unsigned int* my_num_resources(ErlNifEnv *env)
{
    struct metrics_env* priv= enif_priv_data(env);
    return priv->num_resources;
}

static struct hashtable_env* my_hashtable_env(ErlNifEnv *env)
{
    struct metrics_env* priv= enif_priv_data(env);
    return priv->hashtable_env;
}

// Does the counter need to know which family it corresponds to
struct CounterResource {
    struct GrpCounter *counter;
    char resource_name[MAX_RESOURCE_NAME_SIZE + 1];
};

static void counter_resource_destructor(ErlNifEnv *env, void *obj)
{
    struct CounterResource *cnt_res = (struct CounterResource*) obj;
    struct GrpCounter *counter = cnt_res->counter;
    metrics_decr_refcnt_counter(counter);
}

unsigned int align_num_resources(unsigned int num)
{
#define CACHELINE_BYTES 64U
    unsigned int size = num * sizeof(counter64_t);
    return ((size + CACHELINE_BYTES - 1) & ~(CACHELINE_BYTES - 1)) / sizeof(counter64_t);
}

static int load(ErlNifEnv *env, void **priv, ERL_NIF_TERM load_info)
{
#define NUM_ARGS        (3)
#define NIF_RSRC_FLAGS  (ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER)
    const ERL_NIF_TERM *args = NULL, *counters = NULL;
    unsigned int ncpus, num_families;
    int arity, ncounters;
    unsigned int *num_resources;
    struct metrics_resource metrics_module = { 0, 0 };
    ErlNifRWLock **hashtable_lock;

    if (!enif_get_tuple(env, load_info, &arity, &args) ||
            arity != NUM_ARGS)
	    return enif_make_badarg(env);
    if (!enif_get_uint(env, args[0], &ncpus))
        return enif_make_badarg(env);
    if (!enif_get_uint(env, args[1], &num_families))
        return enif_make_badarg(env);
    assert(num_families);
    if (!enif_get_tuple(env, args[2], &ncounters, &counters))
        return enif_make_badarg(env);
    assert(ncounters == num_families);
    num_resources = calloc(num_families, sizeof(*num_resources));
    assert(num_resources);
    for (int i = 0; i < ncounters; i++) {
        if (!enif_get_uint(env, counters[i], num_resources + i)) {
            free(num_resources);
            return enif_make_badarg(env);
        }
        num_resources[i] = align_num_resources(num_resources[i]);
    }
    metrics_module.res_type = enif_open_resource_type(env, g_my_mod_name,
                            g_resource_res_name, counter_resource_destructor,
                            NIF_RSRC_FLAGS, NULL);
    assert(metrics_module.res_type);
    hashtable_lock = calloc(num_families, sizeof(*hashtable_lock));
    assert(hashtable_lock);
    for (int i = 0; i < num_families; i++) {
        hashtable_lock[i] = enif_rwlock_create(NULL);
        assert(hashtable_lock[i]);
    }
    assert(0 == metrics_init(ncpus));
    struct hashtable_env *henv = create_hashtable(num_resources, num_families);
    struct metrics_env *my_priv = calloc(1, sizeof(*my_priv));
    assert(my_priv);
    my_priv->num_resources = num_resources;
    my_priv->num_families = num_families;
    my_priv->hashtable_lock = hashtable_lock;
    my_priv->metrics_module = metrics_module;
    my_priv->hashtable_env = henv;
    *priv = my_priv;
    return 0;
}

static int upgrade(ErlNifEnv * env, void ** priv, void ** old_priv,
                    ERL_NIF_TERM load_info)
{
    return load(env, priv, load_info);
}

static void unload(ErlNifEnv * env, void * priv)
{
    assert(enif_priv_data(env)==priv);
    struct metrics_env* my_priv = priv;
    delete_hashtables(my_priv->hashtable_env);
    metrics_deinit();
    for (int i = 0; i < my_priv->num_families;i++) {
        enif_rwlock_destroy(my_priv->hashtable_lock[i]);
    }
    free(my_priv->hashtable_lock);
    free(my_priv->num_resources);
    free(my_priv);
    return;
}

static ERL_NIF_TERM metrics_alloc_resource_nif(ErlNifEnv* env, int argc,
                                                const ERL_NIF_TERM argv[])
{
    unsigned int num_cntrs;
    int name_size = 0;
    unsigned int family;
    struct CounterResource *res =
                (struct CounterResource*) enif_alloc_resource(
                        my_metrics_module(env).res_type, sizeof(*res));
    assert(res);
    ERL_NIF_TERM res_term = enif_make_resource(env, res);

    if (!enif_get_uint(env, argv[0], &family) || family >= my_num_families(env))
        return enif_make_badarg(env);

    // FIXME: What encoding to use
    if (!(name_size = enif_get_string(env, argv[1], res->resource_name,
                    MAX_RESOURCE_NAME_SIZE, ERL_NIF_LATIN1)))
        return enif_make_badarg(env);

    if (!enif_get_uint(env, argv[2], &num_cntrs) ||
            num_cntrs > my_num_resources(env)[family])
	    return enif_make_badarg(env);

    if (name_size < 0) {
        // Add a debug print about this
        name_size = MAX_RESOURCE_NAME_SIZE;
        res->resource_name[MAX_RESOURCE_NAME_SIZE] = '\0';
    }
    enif_rwlock_rwlock(my_rwlocks(env)[family]);
    res->counter = lookup_counter(my_hashtable_env(env), res->resource_name, family);
    if (!res->counter) {
        res->counter = metrics_alloc_counter(align_num_resources(num_cntrs));
        assert(0 == insert_counter(my_hashtable_env(env), res->resource_name, res->counter, family));
    } else {
        metrics_incr_refcnt_counter(res->counter);
    }
    enif_rwlock_rwunlock(my_rwlocks(env)[family]);
    enif_release_resource(res);
    return res_term;

}

static ERL_NIF_TERM metrics_update_resource_counter_nif(ErlNifEnv* env, int argc,
                                            const ERL_NIF_TERM argv[])
{
    int ret;
    unsigned int key;
    int val;
    struct CounterResource *resource = NULL;
    if (!enif_get_resource(env, argv[0], my_metrics_module(env).res_type,
                            (void**) &resource))
            return enif_make_badarg(env);
    if (!enif_get_uint(env, argv[1], &key))
	    return enif_make_badarg(env);
    if (key >= resource->counter->num_cnt)
	    return enif_make_badarg(env);
    if (!enif_get_int(env, argv[2], &val))
	    return enif_make_badarg(env);
    ret = metrics_update_counter(resource->counter, key, val);
    return enif_make_uint64(env, ret);
}

static ERL_NIF_TERM metrics_decr_resource_counter_nif(ErlNifEnv* env, int argc,
                                            const ERL_NIF_TERM argv[])
{
    int ret;
    unsigned int key;
    struct CounterResource *resource = NULL;
    if (!enif_get_resource(env, argv[0], my_metrics_module(env).res_type,
                            (void**) &resource))
            return enif_make_badarg(env);
    if (!enif_get_uint(env, argv[1], &key))
	    return enif_make_badarg(env);
    if (key >= resource->counter->num_cnt)
	    return enif_make_badarg(env);
    ret = metrics_decr_counter(resource->counter, key);
    return enif_make_uint64(env, ret);
}

static ERL_NIF_TERM metrics_incr_resource_counter_nif(ErlNifEnv* env, int argc,
                                            const ERL_NIF_TERM argv[])
{
    int ret;
    unsigned int key;
    struct CounterResource *resource = NULL;
    if (!enif_get_resource(env, argv[0], my_metrics_module(env).res_type,
                            (void**) &resource))
            return enif_make_badarg(env);
    if (!enif_get_uint(env, argv[1], &key))
	    return enif_make_badarg(env);
    if (key >= resource->counter->num_cnt)
	    return enif_make_badarg(env);

    ret = metrics_incr_counter(resource->counter, key);
    return enif_make_uint64(env, ret);
}

static ERL_NIF_TERM metrics_get_resource_counter_nif(ErlNifEnv* env, int argc,
                                        const ERL_NIF_TERM argv[])
{
    int ret;
    unsigned int key;
    struct CounterResource *resource = NULL;
    if (!enif_get_resource(env, argv[0], my_metrics_module(env).res_type,
                            (void**) &resource))
            return enif_make_badarg(env);
    if (!enif_get_uint(env, argv[1], &key))
        return enif_make_badarg(env);
    if (key >= resource->counter->num_cnt)
	    return enif_make_badarg(env);

    ret = metrics_get_counter_value(resource->counter, key);
    return enif_make_uint64(env, ret);
}

static ERL_NIF_TERM metrics_reset_resource_counter_nif(ErlNifEnv* env, int argc,
                                        const ERL_NIF_TERM argv[])
{
    int ret;
    unsigned int key;
    struct CounterResource *resource = NULL;
    if (!enif_get_resource(env, argv[0], my_metrics_module(env).res_type,
                            (void**) &resource))
            return enif_make_badarg(env);
    if (!enif_get_uint(env, argv[1], &key))
        return enif_make_badarg(env);
    if (key >= resource->counter->num_cnt)
	    return enif_make_badarg(env);

    ret = metrics_reset_counter_value(resource->counter, key);
    return enif_make_uint64(env, ret);
}

static ERL_NIF_TERM metrics_get_all_resources(ErlNifEnv* env, int argc,
                                        const ERL_NIF_TERM argv[])
{
    unsigned int family, options;
    if (!enif_get_uint(env, argv[0], &family))
        return enif_make_badarg(env);
    if (!enif_get_uint(env, argv[1], &options))
        return enif_make_badarg(env);
    assert(family < my_num_families(env));
    ERL_NIF_TERM binary;
    unsigned char *data;
    size_t data_size;
    enif_rwlock_rwlock(my_rwlocks(env)[family]);
    data_size = max_data_per_resource(my_num_resources(env)[family]) *
                                  get_size(my_hashtable_env(env), family);
    data = enif_make_new_binary(env, data_size, &binary);
    assert(data);
    get_all_counters(my_hashtable_env(env), data, family, options, my_num_resources(env)[family]);
    enif_rwlock_rwunlock(my_rwlocks(env)[family]);
    return binary;
}

static ErlNifFunc nif_funcs[] = {
    {"alloc_resource", 3, metrics_alloc_resource_nif},
    {"update_resource_counter", 3, metrics_update_resource_counter_nif},
    {"incr_resource_counter", 2, metrics_incr_resource_counter_nif},
    {"decr_resource_counter", 2, metrics_decr_resource_counter_nif},
    {"get_resource_counter", 2, metrics_get_resource_counter_nif},
    {"reset_resource_counter", 2, metrics_reset_resource_counter_nif},
    {"get_all_resources", 2, metrics_get_all_resources},
};

ERL_NIF_INIT(mzmetrics, nif_funcs, load, NULL, upgrade, unload);
