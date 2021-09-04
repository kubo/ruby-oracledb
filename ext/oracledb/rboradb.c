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

static VALUE cContext;
static VALUE eError;

typedef struct {
    rbOraDBContext *ctxt;
} context_t;

static void context_free(void *arg)
{
    context_t *ctxt = (context_t *)arg;
    if (ctxt != NULL && ctxt->ctxt != NULL) {
        rbOraDBContext_release(ctxt->ctxt);
    }
    xfree(arg);
}

static const struct rb_data_type_struct context_data_type = {
    "OracleDB::Context",
    {NULL, context_free,},
    NULL, NULL,
};

VALUE rboradb_from_dpiErrorInfo(const dpiErrorInfo *error)
{
    rb_encoding *enc = rb_enc_find(error->encoding);
    VALUE message = rb_enc_str_new(error->message, error->messageLength, enc);
    VALUE exc = rb_exc_new_str(eError, message);

    rb_iv_set(exc, "code", INT2NUM(error->code));
    rb_iv_set(exc, "offset", UINT2NUM(error->offset));
    rb_iv_set(exc, "fn_name", rb_str_new_cstr(error->fnName));
    rb_iv_set(exc, "action", rb_str_new_cstr(error->action));
    rb_iv_set(exc, "sql_state", rb_str_new_cstr(error->sqlState));
    rb_iv_set(exc, "is_recoverable", error->isRecoverable ? Qtrue : Qfalse);
    rb_iv_set(exc, "is_warning", error->isWarning ? Qtrue : Qfalse);
    return exc;
}

static VALUE context_alloc(VALUE klass)
{
    context_t *ctxt;
    VALUE obj = TypedData_Make_Struct(klass, context_t, &context_data_type, ctxt);
    return obj;
}

static VALUE context_initialize(int argc, VALUE *argv, VALUE self)
{
    context_t *ctxt = rb_check_typeddata(self, &context_data_type);
    dpiContextCreateParams params = {0,};
    VALUE arg;
    dpiContext *handle;
    dpiErrorInfo errorInfo;
    volatile VALUE gc_guard = Qnil;

    rb_scan_args(argc, argv, "01", &arg);
    if (!NIL_P(arg)) {
        gc_guard = rboradb_set_dpiContextCreateParams(&params, arg);
    }
    if (dpiContext_createWithParams(DPI_MAJOR_VERSION, DPI_MINOR_VERSION, &params, &handle, &errorInfo) != DPI_SUCCESS) {
        rb_exc_raise(rboradb_from_dpiErrorInfo(&errorInfo));
    }
    RB_GC_GUARD(gc_guard);
    ctxt->ctxt = RB_ALLOC(rbOraDBContext);
    ctxt->ctxt->handle = handle;
    ctxt->ctxt->refcnt = 1;
    return Qnil;
}

static VALUE context_client_version(VALUE self)
{
    dpiVersionInfo verinfo;
    dpiContext_getClientVersion(rboradb_get_rbOraDBContext(self)->handle, &verinfo);
    return rboradb_from_dpiVersionInfo(&verinfo);
}

rbOraDBContext *rboradb_get_rbOraDBContext(VALUE obj)
{
    context_t *ctxt = rb_check_typeddata(obj, &context_data_type);
    if (ctxt == NULL || ctxt->ctxt == NULL) {
        rb_raise(rb_eRuntimeError, "OracleDB::Context isn't initialized");
    }
    return ctxt->ctxt;
}

rbOraDBConn *rboradb_get_dconn(VALUE obj)
{
    rbOraDBConn *dconn = rboradb_get_dconn_in_conn(obj);
    if (dconn != NULL) {
        return dconn;
    }
    dconn = rboradb_get_dconn_in_stmt(obj);
    if (dconn != NULL) {
        return dconn;
    }
    rb_raise(rb_eTypeError, "wrong argument type %s (expected OracleDB::Conn or OracleDB::Stmt)", rb_obj_classname(obj));
}

void rboradb_raise_error(rbOraDBContext *ctxt)
{
    dpiErrorInfo error;
    dpiContext_getError(ctxt->handle, &error);
    rb_exc_raise(rboradb_from_dpiErrorInfo(&error));
}

VALUE rboradb_notimplement(int argc, VALUE *argv, VALUE self)
{
    rb_notimplement();
}

void Init_oracledb()
{
    VALUE mOracleDB = rb_define_module("OracleDB");

    eError = rb_define_class_under(mOracleDB, "Error", rb_eStandardError);
    rb_define_attr(eError, "code", 1, 0);
    rb_define_attr(eError, "offset", 1, 0);
    rb_define_attr(eError, "fn_name", 1, 0);
    rb_define_attr(eError, "action", 1, 0);
    rb_define_attr(eError, "sql_state", 1, 0);
    rb_define_attr(eError, "is_recoverable", 1, 0);

    cContext = rb_define_class_under(mOracleDB, "Context", rb_cObject);
    rb_define_alloc_func(cContext, context_alloc);
    rb_define_method(cContext, "initialize", context_initialize, -1);
    rb_define_private_method(cContext, "initialize_copy", rboradb_notimplement, -1);
    rb_define_method(cContext, "client_version", context_client_version, 0);

    rboradb_aq_init(mOracleDB);
    rboradb_conn_init(mOracleDB);
    rboradb_data_init();
    rboradb_datetime_init(mOracleDB);
    rboradb_info_types_init(mOracleDB);
    rboradb_json_init(mOracleDB);
    rboradb_lob_init(mOracleDB);
    rboradb_object_init(mOracleDB);
    rboradb_params_init();
    rboradb_pool_init(mOracleDB);
    rboradb_rowid_init(mOracleDB);
    rboradb_soda_init(mOracleDB);
    rboradb_stmt_init(mOracleDB);
    rboradb_subscr_init(mOracleDB);
    rboradb_var_init(mOracleDB);
}
