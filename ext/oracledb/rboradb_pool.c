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

#define To_Pool(obj) ((Pool_t *)rb_check_typeddata((obj), &pool_data_type))

static VALUE cPool;

typedef struct {
    rbOraDBContext *ctxt;
    dpiPool *handle;
    VALUE pool_name;
} Pool_t;

static void pool_mark(void *arg)
{
    Pool_t *pool = (Pool_t *)arg;
    rb_gc_mark(pool->pool_name);
}

static void pool_free(void *arg)
{
    Pool_t *pool = (Pool_t *)arg;
    if (pool->handle) {
        dpiPool_release(pool->handle);
    }
    if (pool->ctxt) {
        rbOraDBContext_release(pool->ctxt);
    }
    xfree(arg);
}

static const struct rb_data_type_struct pool_data_type = {
    "OracleDB::Pool",
    {pool_mark, pool_free,},
    NULL, NULL,
};

static VALUE pool_alloc(VALUE klass)
{
    Pool_t *pool;
    VALUE obj = TypedData_Make_Struct(klass, Pool_t, &pool_data_type, pool);
    pool->pool_name = Qnil;
    return obj;
}

static VALUE pool_initialize(int argc, VALUE *argv, VALUE self)
{
    Pool_t *pool = To_Pool(self);
    rbOraDBContext *ctxt;
    dpiCommonCreateParams common_params;
    dpiPoolCreateParams pool_params;
    VALUE ctxt_obj, username, password, connstr, params;
    VALUE gc_guard = Qnil;

    rb_scan_args(argc, argv, "41", &ctxt_obj, &username, &password, &connstr, &params);
    ctxt = rboradb_get_rbOraDBContext(ctxt_obj);
    dpiContext_initCommonCreateParams(ctxt->handle, &common_params);
    dpiContext_initPoolCreateParams(ctxt->handle, &pool_params);
    OptExportString(username);
    OptExportString(password);
    OptExportString(connstr);
    if (!NIL_P(params)) {
        gc_guard = rboradb_set_common_and_dpiPoolCreateParams(&common_params, &pool_params, params);
    }

    if (rbOraDBPool_create(ctxt->handle, OPT_RSTRING_PTR(username), OPT_RSTRING_LEN(username),
                         OPT_RSTRING_PTR(password), OPT_RSTRING_LEN(password),
                         OPT_RSTRING_PTR(connstr), OPT_RSTRING_LEN(connstr),
                         &common_params, &pool_params, &pool->handle) != DPI_SUCCESS) {
        rboradb_raise_error(ctxt);
    }
    RB_GC_GUARD(gc_guard);
    rbOraDBContext_addRef(ctxt);
    pool->ctxt = ctxt;
    if (pool_params.outPoolName) {
        pool->pool_name = rb_external_str_new_with_enc(pool_params.outPoolName, pool_params.outPoolNameLength, rb_utf8_encoding());
    }
    return Qnil;
}

static VALUE pool_acquire_connection(int argc, VALUE *argv, VALUE self)
{
    Pool_t *pool = To_Pool(self);
    dpiConnCreateParams conn_params;
    VALUE username, password, params;
    VALUE gc_guard = Qnil;
    dpiConn *dpi_conn;

    rb_scan_args(argc, argv, "21", &username, &password, &params);
    OptExportString(username);
    OptExportString(password);
    dpiContext_initConnCreateParams(pool->ctxt->handle, &conn_params);
    if (!NIL_P(params)) {
        gc_guard = rboradb_set_dpiConnCreateParams(&conn_params, params);
    }

    if (rbOraDBPool_acquireConnection(pool->handle,
                                    OPT_RSTRING_PTR(username), OPT_RSTRING_LEN(username),
                                    OPT_RSTRING_PTR(password), OPT_RSTRING_LEN(password),
                                    &conn_params, &dpi_conn) != DPI_SUCCESS) {
        rboradb_raise_error(pool->ctxt);
    }
    RB_GC_GUARD(gc_guard);
    return rboradb_to_conn(pool->ctxt, dpi_conn, &conn_params);
}

static VALUE pool_close(int argc, VALUE *argv, VALUE self)
{
    Pool_t *pool = To_Pool(self);
    VALUE mode;

    rb_scan_args(argc, argv, "01", &mode);
    if (rbOraDBPool_close(pool->handle, rboradb_to_dpiPoolCloseMode(mode)) != DPI_SUCCESS) {
        rboradb_raise_error(pool->ctxt);
    }
    return Qnil;
}

static VALUE pool_busy_count(VALUE self)
{
    Pool_t *pool = To_Pool(self);
    uint32_t value;

    if (dpiPool_getBusyCount(pool->handle, &value) != DPI_SUCCESS) {
        rboradb_raise_error(pool->ctxt);
    }
    return UINT2NUM(value);
}

static VALUE pool_get_mode(VALUE self)
{
    Pool_t *pool = To_Pool(self);
    dpiPoolGetMode value;

    if (dpiPool_getGetMode(pool->handle, &value) != DPI_SUCCESS) {
        rboradb_raise_error(pool->ctxt);
    }
    return rboradb_from_dpiPoolGetMode(value);
}

static VALUE pool_set_get_mode(VALUE self, VALUE obj)
{
    Pool_t *pool = To_Pool(self);

    if (dpiPool_setGetMode(pool->handle, rboradb_to_dpiPoolGetMode(obj)) != DPI_SUCCESS) {
        rboradb_raise_error(pool->ctxt);
    }
    return Qnil;
}

static VALUE pool_max_lifetime_session(VALUE self)
{
    Pool_t *pool = To_Pool(self);
    uint32_t value;

    if (dpiPool_getMaxLifetimeSession(pool->handle, &value) != DPI_SUCCESS) {
        rboradb_raise_error(pool->ctxt);
    }
    return UINT2NUM(value);
}

static VALUE pool_set_max_lifetime_session(VALUE self, VALUE obj)
{
    Pool_t *pool = To_Pool(self);

    if (dpiPool_setMaxLifetimeSession(pool->handle, NUM2UINT(obj)) != DPI_SUCCESS) {
        rboradb_raise_error(pool->ctxt);
    }
    return Qnil;
}

static VALUE pool_max_sessions_per_shard(VALUE self)
{
    Pool_t *pool = To_Pool(self);
    uint32_t value;

    if (dpiPool_getMaxSessionsPerShard(pool->handle, &value) != DPI_SUCCESS) {
        rboradb_raise_error(pool->ctxt);
    }
    return UINT2NUM(value);
}

static VALUE pool_set_max_sessions_per_shard(VALUE self, VALUE obj)
{
    Pool_t *pool = To_Pool(self);

    if (dpiPool_setMaxSessionsPerShard(pool->handle, NUM2UINT(obj)) != DPI_SUCCESS) {
        rboradb_raise_error(pool->ctxt);
    }
    return Qnil;
}

static VALUE pool_open_count(VALUE self)
{
    Pool_t *pool = To_Pool(self);
    uint32_t value;

    if (dpiPool_getOpenCount(pool->handle, &value) != DPI_SUCCESS) {
        rboradb_raise_error(pool->ctxt);
    }
    return UINT2NUM(value);
}

static VALUE pool_reconfigure(VALUE self, VALUE min_sessions, VALUE max_sessions, VALUE session_increment)
{
    Pool_t *pool = To_Pool(self);

    if (dpiPool_reconfigure(pool->handle, NUM2UINT(min_sessions), NUM2UINT(max_sessions), NUM2UINT(session_increment)) != DPI_SUCCESS) {
        rboradb_raise_error(pool->ctxt);
    }
    return Qnil;
}

static VALUE pool_soda_metadata_cache(VALUE self)
{
    Pool_t *pool = To_Pool(self);
    int value;

    if (dpiPool_getSodaMetadataCache(pool->handle, &value) != DPI_SUCCESS) {
        rboradb_raise_error(pool->ctxt);
    }
    return value ? Qtrue : Qfalse;
}

static VALUE pool_set_soda_metadata_cache(VALUE self, VALUE obj)
{
    Pool_t *pool = To_Pool(self);

    if (dpiPool_setSodaMetadataCache(pool->handle, RTEST(obj)) != DPI_SUCCESS) {
        rboradb_raise_error(pool->ctxt);
    }
    return Qnil;
}

static VALUE pool_stmt_cache_size(VALUE self)
{
    Pool_t *pool = To_Pool(self);
    uint32_t value;

    if (dpiPool_getStmtCacheSize(pool->handle, &value) != DPI_SUCCESS) {
        rboradb_raise_error(pool->ctxt);
    }
    return UINT2NUM(value);
}

static VALUE pool_set_stmt_cache_size(VALUE self, VALUE obj)
{
    Pool_t *pool = To_Pool(self);

    if (dpiPool_setStmtCacheSize(pool->handle, NUM2UINT(obj)) != DPI_SUCCESS) {
        rboradb_raise_error(pool->ctxt);
    }
    return Qnil;
}

static VALUE pool_timeout(VALUE self)
{
    Pool_t *pool = To_Pool(self);
    uint32_t value;

    if (dpiPool_getTimeout(pool->handle, &value) != DPI_SUCCESS) {
        rboradb_raise_error(pool->ctxt);
    }
    return UINT2NUM(value);
}

static VALUE pool_set_timeout(VALUE self, VALUE obj)
{
    Pool_t *pool = To_Pool(self);

    if (dpiPool_setTimeout(pool->handle, NUM2UINT(obj)) != DPI_SUCCESS) {
        rboradb_raise_error(pool->ctxt);
    }
    return Qnil;
}

static VALUE pool_wait_timeout(VALUE self)
{
    Pool_t *pool = To_Pool(self);
    uint32_t value;

    if (dpiPool_getWaitTimeout(pool->handle, &value) != DPI_SUCCESS) {
        rboradb_raise_error(pool->ctxt);
    }
    return UINT2NUM(value);
}

static VALUE pool_set_wait_timeout(VALUE self, VALUE obj)
{
    Pool_t *pool = To_Pool(self);

    if (dpiPool_setWaitTimeout(pool->handle, NUM2UINT(obj)) != DPI_SUCCESS) {
        rboradb_raise_error(pool->ctxt);
    }
    return Qnil;
}

static VALUE pool_ping_interval(VALUE self)
{
    Pool_t *pool = To_Pool(self);
    int value;

    if (dpiPool_getPingInterval(pool->handle, &value) != DPI_SUCCESS) {
        rboradb_raise_error(pool->ctxt);
    }
    return INT2NUM(value);
}

static VALUE pool_set_ping_interval(VALUE self, VALUE obj)
{
    Pool_t *pool = To_Pool(self);

    if (dpiPool_setPingInterval(pool->handle, NUM2INT(obj)) != DPI_SUCCESS) {
        rboradb_raise_error(pool->ctxt);
    }
    return Qnil;
}

void rboradb_pool_init(VALUE mOracleDB)
{
    cPool = rb_define_class_under(mOracleDB, "Pool", rb_cObject);
    rb_define_alloc_func(cPool, pool_alloc);
    rb_define_method(cPool, "initialize", pool_initialize, -1);
    rb_define_private_method(cPool, "initialize_copy", rboradb_notimplement, -1);
    rb_define_method(cPool, "acquire_connection", pool_acquire_connection, -1);
    rb_define_method(cPool, "close", pool_close, -1);
    rb_define_method(cPool, "busy_count", pool_busy_count, 0);
    rb_define_method(cPool, "get_mode", pool_get_mode, 0);
    rb_define_method(cPool, "get_mode=", pool_set_get_mode, 1);
    rb_define_method(cPool, "max_lifetime_session", pool_max_lifetime_session, 0);
    rb_define_method(cPool, "max_lifetime_session=", pool_set_max_lifetime_session, 1);
    rb_define_method(cPool, "max_sessions_per_shard", pool_max_sessions_per_shard, 0);
    rb_define_method(cPool, "max_sessions_per_shard=", pool_set_max_sessions_per_shard, 1);
    rb_define_method(cPool, "open_count", pool_open_count, 0);
    rb_define_method(cPool, "reconfigure", pool_reconfigure, 3);
    rb_define_method(cPool, "soda_metadata_cache", pool_soda_metadata_cache, 0);
    rb_define_method(cPool, "soda_metadata_cache=", pool_set_soda_metadata_cache, 1);
    rb_define_method(cPool, "stmt_cache_size", pool_stmt_cache_size, 0);
    rb_define_method(cPool, "stmt_cache_size=", pool_set_stmt_cache_size, 1);
    rb_define_method(cPool, "timeout", pool_timeout, 0);
    rb_define_method(cPool, "timeout=", pool_set_timeout, 1);
    rb_define_method(cPool, "wait_timeout", pool_wait_timeout, 0);
    rb_define_method(cPool, "wait_timeout=", pool_set_wait_timeout, 1);
    rb_define_method(cPool, "ping_interval", pool_ping_interval, 0);
    rb_define_method(cPool, "ping_interval=", pool_set_ping_interval, 1);
}
