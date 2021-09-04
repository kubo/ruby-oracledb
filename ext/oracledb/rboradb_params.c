// ruby-oracledb - Ruby binding for Oracle database based on ODPI-C
//
// URL: https://github.com/kubo/ruby-oracledb
//
//-----------------------------------------------------------------------------
// Copyright (c) 2021 Kubo Takehiro <kubo@jiubao.org>. All rights reserved.
// This program is free software: you can modify it and/or redistribute it
// under the terms of:
//
// (i)  the Universal Permissive License v 1.0 or at your option, any
//      later version (http://oss.oracle.com/licenses/upl); and/or
//
// (ii) the Apache License v 2.0. (http://www.apache.org/licenses/LICENSE-2.0)
//-----------------------------------------------------------------------------
#include "rboradb.h"
#include <stddef.h>

typedef struct param_def {
    const char *name;
    VALUE name_sym;
    size_t offset1;
    size_t offset2;
    VALUE (*set)(VALUE obj, void *dest, const struct param_def *def);
} param_def_t;

typedef struct {
    void *dest;
    const param_def_t* defs;
    VALUE gc_guard;
    int unknown_key_found;
} set_param_arg_t;

typedef struct {
    VALUE unknown_keys;
    const param_def_t **defs;
} collect_unknown_keys_arg_t;

#define BOOL_TO_INT(name, type_name, member_name) \
    {name, Qnil, (size_t)offsetof(type_name, member_name), 0, set_bool_to_int}
#define CONST_CHAR(name, type_name, member_name) \
    {name, Qnil, (size_t)offsetof(type_name, member_name), 0, set_const_char}
#define CHAR_WITH_LEN(name, type_name, member_name) \
    {name, Qnil, (size_t)offsetof(type_name, member_name), (size_t)offsetof(type_name, member_name##Length), set_char_with_len}
#define ENUM_UINT8(name, type_name, member_name, func) \
    {name, Qnil, (size_t)offsetof(type_name, member_name), (size_t)func, set_enum_uint8}
#define ENUM_UINT32(name, type_name, member_name, func) \
    {name, Qnil, (size_t)offsetof(type_name, member_name), (size_t)func, set_enum_uint32}
#define INT32(name, type_name, member_name) \
    {name, Qnil, (size_t)offsetof(type_name, member_name), 0, set_int32}
#define UINT32(name, type_name, member_name) \
    {name, Qnil, (size_t)offsetof(type_name, member_name), 0, set_uint32}
#define UINT64(name, type_name, member_name) \
    {name, Qnil, (size_t)offsetof(type_name, member_name), 0, set_uint64}
#define SPECIFIC_FUNC(name, set_func) \
    {name, Qnil, 0, 0, set_func}

static VALUE set_bool_to_int(VALUE obj, void *dest, const param_def_t *def);
static VALUE set_const_char(VALUE obj, void *dest, const param_def_t *def);
static VALUE set_char_with_len(VALUE obj, void *dest, const param_def_t *def);
static VALUE set_enum_uint8(VALUE obj, void *dest, const param_def_t *def);
static VALUE set_enum_uint32(VALUE obj, void *dest, const param_def_t *def);
static VALUE set_int32(VALUE obj, void *dest, const param_def_t *def);
static VALUE set_uint32(VALUE obj, void *dest, const param_def_t *def);
static VALUE set_uint64(VALUE obj, void *dest, const param_def_t *def);
static VALUE set_soda_oper_options_keys(VALUE obj, void *dest, const param_def_t *def);
static VALUE set_subscr_create_params_callback(VALUE obj, void *dest, const param_def_t *def);

static void init_params(param_def_t *defs);
static VALUE rboradb_params_set(VALUE hash, void **dests, const param_def_t **defs);
static int set_param_func(VALUE key, VALUE val, VALUE data);
NORETURN(static void raise_unknown_keys(VALUE hash, const param_def_t **defs));
static int collect_unknown_keys(VALUE key, VALUE val, VALUE data);

static param_def_t rboradb_common_create_params[] = {
    ENUM_UINT32("create_mode", dpiCommonCreateParams, createMode, rboradb_to_dpiCreateMode),
    CHAR_WITH_LEN("edition", dpiCommonCreateParams, edition),
    CHAR_WITH_LEN("driver_name", dpiCommonCreateParams, driverName),
    BOOL_TO_INT("soda_metadata_cache", dpiCommonCreateParams, sodaMetadataCache),
    UINT32("stmt_cache_size", dpiCommonCreateParams, stmtCacheSize),
    {NULL,},
};

static param_def_t rboradb_conn_create_params[] = {
    ENUM_UINT32("auth_mode", dpiConnCreateParams, authMode, rboradb_to_dpiAuthMode),
    CHAR_WITH_LEN("connection_class", dpiConnCreateParams, connectionClass),
    ENUM_UINT32("purity", dpiConnCreateParams, purity, rboradb_to_dpiPurity),
    CHAR_WITH_LEN("new_password", dpiConnCreateParams, newPassword),
    // APPCONTEXT("appcontext", dpiConnCreateParams, appContext, numAppContext), // TODO
    BOOL_TO_INT("external_auth", dpiConnCreateParams, externalAuth),
    // void *externalHandle;
    // dpiPool *pool; // TODO
    CHAR_WITH_LEN("tag", dpiConnCreateParams, tag),
    BOOL_TO_INT("match_any_tag", dpiConnCreateParams, matchAnyTag),
    // SHARDING_KEY_COLUMN("sharding_key_columns", dpiConnCreateParams, shardingKeyColumns, numShardingKeyColumns), // TODO
    // SHARDING_KEY_COLUMN("super_sharding_key_columns", dpiConnCreateParams, superShardingKeyColumns, numSuperShardingKeyColumns), // TODO
    {NULL,},
};

static param_def_t rboradb_pool_create_params[] = {
    UINT32("min_sessions", dpiPoolCreateParams, minSessions),
    UINT32("max_sessions", dpiPoolCreateParams, maxSessions),
    UINT32("session_increment", dpiPoolCreateParams, sessionIncrement),
    INT32("ping_interval", dpiPoolCreateParams, pingInterval),
    INT32("ping_timeout", dpiPoolCreateParams, pingTimeout),
    BOOL_TO_INT("homogeneous", dpiPoolCreateParams, homogeneous),
    BOOL_TO_INT("external_auth", dpiPoolCreateParams, externalAuth),
    ENUM_UINT32("get_mode", dpiPoolCreateParams, getMode, rboradb_to_dpiPoolGetMode),
    // const char *outPoolName;
    // uint32_t outPoolNameLength;
    UINT32("timeout", dpiPoolCreateParams, timeout),
    UINT32("wait_timeout", dpiPoolCreateParams, waitTimeout),
    UINT32("max_lifetime_session", dpiPoolCreateParams, maxLifetimeSession),
    CHAR_WITH_LEN("plsql_fixup_callback", dpiPoolCreateParams, plsqlFixupCallback),
    UINT32("max_sessions_per_shard", dpiPoolCreateParams, maxSessionsPerShard),
    {NULL,},
};

static param_def_t rboradb_context_create_params[] = {
    CONST_CHAR("default_driver_name", dpiContextCreateParams, defaultDriverName),
    CONST_CHAR("load_error_url", dpiContextCreateParams, loadErrorUrl),
    CONST_CHAR("oracle_client_lib_dir", dpiContextCreateParams, oracleClientLibDir),
    CONST_CHAR("oracle_client_config_dir", dpiContextCreateParams, oracleClientConfigDir),
    {NULL,}
};

static param_def_t rboradb_soda_oper_options[] = {
    SPECIFIC_FUNC("keys", set_soda_oper_options_keys),
    CHAR_WITH_LEN("key", dpiSodaOperOptions, key),
    CHAR_WITH_LEN("version", dpiSodaOperOptions, version),
    CHAR_WITH_LEN("filter", dpiSodaOperOptions, filter),
    UINT32("skip", dpiSodaOperOptions, skip),
    UINT32("limit", dpiSodaOperOptions, limit),
    UINT32("fetch_array_size", dpiSodaOperOptions, fetchArraySize),
    CHAR_WITH_LEN("hint", dpiSodaOperOptions, hint),
    {NULL,}
};

static param_def_t rboradb_subscr_create_params[] = {
    ENUM_UINT32("subscr_namespace", dpiSubscrCreateParams, subscrNamespace, rboradb_to_dpiSubscrNamespace),
    ENUM_UINT32("protocol", dpiSubscrCreateParams, protocol, rboradb_to_dpiSubscrProtocol),
    ENUM_UINT32("qos", dpiSubscrCreateParams, qos, rboradb_to_dpiSubscrQOS),
    ENUM_UINT32("operations", dpiSubscrCreateParams, operations, rboradb_to_dpiOpCode),
    UINT32("port_number", dpiSubscrCreateParams, portNumber),
    UINT32("timeout", dpiSubscrCreateParams, timeout),
    CHAR_WITH_LEN("name", dpiSubscrCreateParams, name),
    SPECIFIC_FUNC("callback", set_subscr_create_params_callback),
    CHAR_WITH_LEN("recipient_name", dpiSubscrCreateParams, recipientName),
    CHAR_WITH_LEN("ip_address", dpiSubscrCreateParams, ipAddress),
    ENUM_UINT8("grouping_class", dpiSubscrCreateParams, groupingClass, rboradb_to_dpiSubscrGroupingClass),
    UINT32("grouping_value", dpiSubscrCreateParams, groupingValue),
    ENUM_UINT8("grouping_type", dpiSubscrCreateParams, groupingType, rboradb_to_dpiSubscrGroupingType),
    UINT64("outRegId", dpiSubscrCreateParams, outRegId),
    BOOL_TO_INT("client_initiated", dpiSubscrCreateParams, clientInitiated),
    {NULL,}
};

static VALUE set_bool_to_int(VALUE obj, void *dest, const struct param_def *def)
{
    *(int *)((size_t)dest + def->offset1) = RTEST(obj) ? 1 : 0;
    return Qnil;
}

static VALUE set_const_char(VALUE obj, void *dest, const struct param_def *def)
{
    ExportString(obj);
    *(const char **)((size_t)dest + def->offset1) = rb_string_value_cstr(&obj);
    return obj;
}

static VALUE set_char_with_len(VALUE obj, void *dest, const param_def_t *def)
{
    ExportString(obj);
    *(const char **)((size_t)dest + def->offset1) = RSTRING_PTR(obj);
    *(uint32_t *)((size_t)dest + def->offset2) = RSTRING_LEN(obj);
    return obj;
}

static VALUE set_enum_uint8(VALUE obj, void *dest, const param_def_t *def)
{
    uint8_t (*func)(VALUE) = (uint8_t (*)(VALUE))def->offset2;
    *(uint8_t *)((size_t)dest + def->offset1) = func(obj);
    return Qnil;
}

static VALUE set_enum_uint32(VALUE obj, void *dest, const param_def_t *def)
{
    uint32_t (*func)(VALUE) = (uint32_t (*)(VALUE))def->offset2;
    *(uint32_t *)((size_t)dest + def->offset1) = func(obj);
    return Qnil;
}

static VALUE set_int32(VALUE obj, void *dest, const param_def_t *def)
{
    *(int32_t *)((size_t)dest + def->offset1) = NUM2INT(obj);
    return Qnil;
}

static VALUE set_uint32(VALUE obj, void *dest, const param_def_t *def)
{
    *(uint32_t *)((size_t)dest + def->offset1) = NUM2UINT(obj);
    return Qnil;
}

static VALUE set_uint64(VALUE obj, void *dest, const param_def_t *def)
{
    *(uint64_t *)((size_t)dest + def->offset1) = NUM2ULL(obj);
    return Qnil;
}

static VALUE set_soda_oper_options_keys(VALUE obj, void *dest, const param_def_t *def)
{
    dpiSodaOperOptions *opts = (dpiSodaOperOptions*)dest;
    size_t idx, count;
    VALUE gc_guard;
    VALUE tmp;

    Check_Type(obj, T_ARRAY);
    count = RARRAY_LEN(obj);
    gc_guard = rb_ary_new_capa(count + 2);
    opts->keys = rb_alloc_tmp_buffer_with_count(&tmp, sizeof(*opts->keys), count);
    rb_ary_push(gc_guard, tmp);
    opts->keyLengths = rb_alloc_tmp_buffer_with_count(&tmp, sizeof(*opts->keyLengths), count);
    rb_ary_push(gc_guard, tmp);
    for (idx = 0; idx < count; idx++) {
        tmp = RARRAY_AREF(obj, idx);
        ExportString(tmp);
        opts->keys[idx] = RSTRING_PTR(tmp);
        opts->keyLengths[idx] = RSTRING_LEN(tmp);
        rb_ary_push(gc_guard, tmp);
    }
    opts->numKeys = count;
    return gc_guard;
}

static VALUE set_subscr_create_params_callback(VALUE obj, void *dest, const param_def_t *def)
{
    dpiSubscrCreateParams *prms = (dpiSubscrCreateParams *)dest;

    if (NIL_P(obj)) {
        return Qnil;
    }
    if (RB_SYMBOL_P(obj)) {
        static ID id;
        CONST_ID(id, "to_proc");
        obj = rb_funcall(obj, id, 0);
    }
    if (rb_obj_is_proc(obj)) {
        prms->callbackContext = (void*)obj;
        return obj;
    }
    rb_raise(rb_eArgError, "wrong callback type (given %s, expected nil, symbol, proc or lambda)", rb_obj_classname(obj));
}

void rboradb_params_init(void)
{
    init_params(rboradb_context_create_params);
}

static void init_params(param_def_t *defs)
{
    while (defs->name != NULL) {
        defs->name_sym = ID2SYM(rb_intern(defs->name));
        defs++;
    }
}

VALUE rboradb_set_dpiContextCreateParams(dpiContextCreateParams *context_params, VALUE hash)
{
    const param_def_t *defs[2];
    void *data[1];
    defs[0] = rboradb_context_create_params;
    data[0] = context_params;
    defs[1] = NULL;
    return rboradb_params_set(hash, data, defs);
}

VALUE rboradb_set_common_and_dpiConnCreateParams(dpiCommonCreateParams *common_params, dpiConnCreateParams *conn_params, VALUE hash)
{
    const param_def_t *defs[3];
    void *data[2];
    defs[0] = rboradb_common_create_params;
    data[0] = common_params;
    defs[1] = rboradb_conn_create_params;
    data[1] = conn_params;
    defs[2] = NULL;
    return rboradb_params_set(hash, data, defs);
}

VALUE rboradb_set_common_and_dpiPoolCreateParams(dpiCommonCreateParams *common_params, dpiPoolCreateParams *pool_params, VALUE hash)
{
    const param_def_t *defs[3];
    void *data[2];
    defs[0] = rboradb_common_create_params;
    data[0] = common_params;
    defs[1] = rboradb_pool_create_params;
    data[1] = pool_params;
    defs[2] = NULL;
    return rboradb_params_set(hash, data, defs);
}

VALUE rboradb_set_dpiConnCreateParams(dpiConnCreateParams *conn_params, VALUE hash)
{
    const param_def_t *defs[2];
    void *data[1];
    defs[0] = rboradb_conn_create_params;
    data[0] = conn_params;
    defs[1] = NULL;
    return rboradb_params_set(hash, data, defs);
}

VALUE rboradb_set_dpiSodaOperOptions(dpiSodaOperOptions *opts, VALUE hash)
{
    const param_def_t *defs[2];
    void *data[1];
    defs[0] = rboradb_soda_oper_options;
    data[0] = opts;
    defs[1] = NULL;
    return rboradb_params_set(hash, data, defs);
}

VALUE rboradb_set_dpiSubscrCreateParams(dpiSubscrCreateParams *params, VALUE hash)
{
    const param_def_t *defs[2];
    void *data[1];
    defs[0] = rboradb_subscr_create_params;
    data[0] = params;
    defs[1] = NULL;
    return rboradb_params_set(hash, data, defs);
}

static VALUE rboradb_params_set(VALUE hash, void **dests, const param_def_t **defs)
{
    set_param_arg_t arg;
    int i;
    arg.gc_guard = Qnil;
    arg.unknown_key_found = 0;

    Check_Type(hash, T_HASH);

    for (i = 0; defs[i] != NULL; i++) {
        arg.dest = dests[i];
        arg.defs = defs[i];
        rb_hash_foreach(hash, set_param_func, (VALUE)&arg);
        if (arg.unknown_key_found) {
            raise_unknown_keys(hash, defs);
        }
    }
    return arg.gc_guard;
}

static int set_param_func(VALUE key, VALUE val, VALUE data)
{
    set_param_arg_t *arg = (set_param_arg_t*)data;
    void *dest = arg->dest;
    const param_def_t *defs = arg->defs;

    while (defs->name != NULL) {
        if (defs->name_sym == key) {
            VALUE tmp = defs->set(val, dest, defs);
            if (!NIL_P(tmp)) {
                if (NIL_P(arg->gc_guard)) {
                    arg->gc_guard = rb_ary_new();
                }
                rb_ary_push(arg->gc_guard, tmp);
            }
            return ST_CONTINUE;
        }
        defs++;
    }
    arg->unknown_key_found = 1;
    return ST_STOP;
}

static void raise_unknown_keys(VALUE hash, const param_def_t **defs)
{
    collect_unknown_keys_arg_t arg;
    VALUE msg;
    long i, len;

    arg.unknown_keys = rb_ary_new();
    arg.defs = defs;
    rb_hash_foreach(hash, collect_unknown_keys, (VALUE)&arg);
    len = RARRAY_LEN(arg.unknown_keys);

    if (len > 1) {
        msg = rb_str_new_cstr("unknown keywords: ");
    } else {
        msg = rb_str_new_cstr("unknown keyword: ");
    }
    for (i = 0; i < len; i++) {
        if (i != 0) {
            rb_str_cat_cstr(msg, ", ");
        }
        rb_str_append(msg, rb_inspect(RARRAY_AREF(arg.unknown_keys, i)));
    }
    rb_exc_raise(rb_exc_new_str(rb_eArgError, msg));
}

static int collect_unknown_keys(VALUE key, VALUE val, VALUE data)
{
    collect_unknown_keys_arg_t *arg = (collect_unknown_keys_arg_t *)data;
    const param_def_t **defs = arg->defs;

    while (*defs != NULL) {
        const param_def_t *def = *defs;
        while (def->name != NULL) {
            if (def->name_sym == key) {
                return ST_CONTINUE;
            }
            def++;
        }
        defs++;
    }
    rb_ary_push(arg->unknown_keys, key);
    return ST_CONTINUE;
}
