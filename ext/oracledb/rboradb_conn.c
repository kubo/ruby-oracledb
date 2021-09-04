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

#define To_Conn(obj) ((Conn_t *)rb_check_typeddata((obj), &conn_data_type))

static VALUE cConn;

typedef struct conn {
    rbOraDBConn *dconn;
    VALUE tag;
    VALUE tag_found;
    VALUE new_session;
} Conn_t;

static void init_conn(Conn_t *conn, rbOraDBContext *ctxt, dpiConn* dpi_conn, const dpiConnCreateParams *params);

static void conn_mark(void *arg)
{
    Conn_t *conn = (Conn_t *)arg;
    rb_gc_mark(conn->tag);
}

static void conn_free(void *arg)
{
    Conn_t *conn = (Conn_t *)arg;

    if (conn && conn->dconn) {
        rbOraDBConn_release(conn->dconn);
    }
    xfree(arg);
}

static const struct rb_data_type_struct conn_data_type = {
    "OracleDB::Conn",
    {conn_mark, conn_free,},
    NULL, NULL,
};

static VALUE conn_alloc(VALUE klass)
{
    Conn_t *conn;
    return TypedData_Make_Struct(klass, Conn_t, &conn_data_type, conn);
}

static VALUE conn_initialize(int argc, VALUE *argv, VALUE self)
{
    Conn_t *conn = To_Conn(self);
    rbOraDBContext *ctxt;
    dpiCommonCreateParams common_params;
    dpiConnCreateParams conn_params;
    VALUE ctxt_obj, username, password, connstr, params;
    dpiConn *dpi_conn;
    VALUE gc_guard = Qnil;

    rb_scan_args(argc, argv, "41", &ctxt_obj, &username, &password, &connstr, &params);
    ctxt = rboradb_get_rbOraDBContext(ctxt_obj);
    OptExportString(username);
    OptExportString(password);
    OptExportString(connstr);
    dpiContext_initCommonCreateParams(ctxt->handle, &common_params);
    dpiContext_initConnCreateParams(ctxt->handle, &conn_params);
    if (!NIL_P(params)) {
        gc_guard = rboradb_set_common_and_dpiConnCreateParams(&common_params, &conn_params, params);
    }

    if (rbOraDBConn_create(ctxt->handle, OPT_RSTRING_PTR(username), OPT_RSTRING_LEN(username),
                         OPT_RSTRING_PTR(password), OPT_RSTRING_LEN(password),
                         OPT_RSTRING_PTR(connstr), OPT_RSTRING_LEN(connstr),
                         &common_params, &conn_params, &dpi_conn) != DPI_SUCCESS) {
        rboradb_raise_error(ctxt);
    }
    RB_GC_GUARD(gc_guard);
    init_conn(conn, ctxt, dpi_conn, &conn_params);
    return Qnil;
}

VALUE rboradb_to_conn(rbOraDBContext *ctxt, dpiConn* dpi_conn, const dpiConnCreateParams *params)
{
    Conn_t *conn;
    VALUE obj = TypedData_Make_Struct(cConn, Conn_t, &conn_data_type, conn);
    init_conn(conn, ctxt, dpi_conn, params);
    return obj;
}

static void init_conn(Conn_t *conn, rbOraDBContext *ctxt, dpiConn* dpi_conn, const dpiConnCreateParams *params)
{
    conn->dconn = RB_ZALLOC(rbOraDBConn);
    conn->dconn->refcnt = 1;
    conn->dconn->ctxt = ctxt;
    conn->dconn->handle = dpi_conn;
    rbOraDBContext_addRef(ctxt);
    conn->tag = rb_external_str_new_with_enc(params->outTag, params->outTagLength, rb_utf8_encoding());
    conn->tag_found = params->outTagFound ? Qtrue : Qfalse;
    conn->new_session = params->outNewSession ? Qtrue : Qfalse;
}

static VALUE conn_begin_distrib_trans(VALUE self, VALUE format_id, VALUE transaction_id, VALUE branch_id)
{
    Conn_t *conn = To_Conn(self);

    SafeStringValue(transaction_id);
    SafeStringValue(branch_id);
    if (rbOraDBConn_beginDistribTrans(conn->dconn->handle, NUM2LONG(format_id),
                                    RSTRING_PTR(transaction_id), RSTRING_LEN(transaction_id),
                                    RSTRING_PTR(branch_id), RSTRING_LEN(branch_id)) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    return Qnil;
}

static VALUE conn_break_execution(VALUE self)
{
    Conn_t *conn = To_Conn(self);

    if (rbOraDBConn_breakExecution(conn->dconn->handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    return Qnil;
}

static VALUE conn_change_password(VALUE self, VALUE username, VALUE old_password, VALUE new_password)
{
    Conn_t *conn = To_Conn(self);

    ExportString(username);
    ExportString(old_password);
    ExportString(new_password);
    if (rbOraDBConn_changePassword(conn->dconn->handle,
                                 RSTRING_PTR(username), RSTRING_LEN(username),
                                 RSTRING_PTR(old_password), RSTRING_LEN(old_password),
                                 RSTRING_PTR(new_password), RSTRING_LEN(new_password)) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    return Qnil;
}

static VALUE conn_close(int argc, VALUE *argv, VALUE self)
{
    Conn_t *conn = To_Conn(self);
    VALUE mode;
    VALUE tag;

    rb_scan_args(argc, argv, "02", &mode, &tag);
    OptExportString(tag);
    if (rbOraDBConn_close(conn->dconn->handle, rboradb_to_dpiConnCloseMode(mode),
                        OPT_RSTRING_PTR(tag), OPT_RSTRING_LEN(tag)) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    return Qnil;
}

static VALUE conn_commit(VALUE self)
{
    Conn_t *conn = To_Conn(self);

    if (rbOraDBConn_commit(conn->dconn->handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    return Qnil;
}

static VALUE conn_call_timeout(VALUE self)
{
    Conn_t *conn = To_Conn(self);
    uint32_t msecs;

    if (dpiConn_getCallTimeout(conn->dconn->handle, &msecs) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    return DBL2NUM((double)msecs / 1000.0);
}

static VALUE conn_current_schema(VALUE self)
{
    Conn_t *conn = To_Conn(self);
    const char *ptr;
    uint32_t len;

    if (dpiConn_getCurrentSchema(conn->dconn->handle, &ptr, &len) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    return rb_external_str_new_with_enc(ptr, len, rb_utf8_encoding());
}

static VALUE conn_edition(VALUE self)
{
    Conn_t *conn = To_Conn(self);
    const char *ptr;
    uint32_t len;

    if (dpiConn_getEdition(conn->dconn->handle, &ptr, &len) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    return rb_external_str_new_with_enc(ptr, len, rb_utf8_encoding());
}

static VALUE conn_external_name(VALUE self)
{
    Conn_t *conn = To_Conn(self);
    const char *ptr;
    uint32_t len;

    if (dpiConn_getExternalName(conn->dconn->handle, &ptr, &len) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    return rb_external_str_new_with_enc(ptr, len, rb_utf8_encoding());
}

static VALUE conn_internal_name(VALUE self)
{
    Conn_t *conn = To_Conn(self);
    const char *ptr;
    uint32_t len;

    if (dpiConn_getInternalName(conn->dconn->handle, &ptr, &len) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    return rb_external_str_new_with_enc(ptr, len, rb_utf8_encoding());
}

static VALUE conn_ltxid(VALUE self)
{
    Conn_t *conn = To_Conn(self);
    const char *ptr;
    uint32_t len;

    if (dpiConn_getLTXID(conn->dconn->handle, &ptr, &len) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    return rb_external_str_new(ptr, len);
}

static VALUE conn_oci_attr(VALUE self, VALUE handle_type, VALUE attr, VALUE attr_data_type)
{
    Conn_t *conn = To_Conn(self);
    uint32_t htype = rboradb_to_OciHandleType(handle_type);
    OciAttrDataType type = rboradb_to_OciAttrDataType(attr_data_type);
    dpiDataBuffer val;
    uint32_t len;

    if (dpiConn_getOciAttr(conn->dconn->handle, htype, NUM2UINT(attr), &val, &len) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    switch (type) {
    case RBORADB_OCI_ATTR_DATA_TYPE_BOOLEAN:
        return val.asBoolean ? Qtrue : Qfalse;
    case RBORADB_OCI_ATTR_DATA_TYPE_TEXT:
        return rb_enc_str_new(val.asString, len, rb_utf8_encoding());
    case RBORADB_OCI_ATTR_DATA_TYPE_UB1:
        return INT2FIX(val.asUint8);
    case RBORADB_OCI_ATTR_DATA_TYPE_UB2:
        return INT2FIX(val.asUint16);
    case RBORADB_OCI_ATTR_DATA_TYPE_UB4:
        return UINT2NUM(val.asUint32);
    case RBORADB_OCI_ATTR_DATA_TYPE_UB8:
        return ULL2NUM(val.asUint64);
    }
    return Qnil;
}

static VALUE conn_server_version(VALUE self)
{
    Conn_t *conn = To_Conn(self);
    dpiVersionInfo ver;

    if (dpiConn_getServerVersion(conn->dconn->handle, NULL, NULL, &ver) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    return rboradb_from_dpiVersionInfo(&ver);
}

static VALUE conn_stmt_cache_size(VALUE self)
{
    Conn_t *conn = To_Conn(self);
    uint32_t size;

    if (dpiConn_getStmtCacheSize(conn->dconn->handle, &size) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    return UINT2NUM(size);
}

static VALUE conn_ping(VALUE self)
{
    Conn_t *conn = To_Conn(self);

    if (rbOraDBConn_ping(conn->dconn->handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    return Qnil;
}

static VALUE conn_prepare_distrib_trans(VALUE self)
{
    Conn_t *conn = To_Conn(self);
    int commit_needed;

    if (rbOraDBConn_prepareDistribTrans(conn->dconn->handle, &commit_needed) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    return commit_needed ? Qtrue : Qfalse;
}

static VALUE conn___prepare_stmt(VALUE self, VALUE sql, VALUE fetch_array_size, VALUE scrollable, VALUE tag)
{
    Conn_t *conn = To_Conn(self);
    uint32_t array_size;
    dpiStmt *handle;

    ExportString(sql);
    array_size = NUM2UINT(fetch_array_size);
    OptExportString(tag);

    if (dpiConn_prepareStmt(conn->dconn->handle, RTEST(scrollable), RSTRING_PTR(sql), RSTRING_LEN(sql),
                            OPT_RSTRING_PTR(tag), OPT_RSTRING_LEN(tag), &handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    return rboradb_from_dpiStmt(handle, conn->dconn, 0, array_size);
}

static VALUE conn_rollback(VALUE self)
{
    Conn_t *conn = To_Conn(self);

    if (rbOraDBConn_rollback(conn->dconn->handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    return Qnil;
}

static VALUE set_char(VALUE self, VALUE value, int (*func)(dpiConn *, const char *, uint32_t))
{
    Conn_t *conn = To_Conn(self);

    ExportString(value);
    if (func(conn->dconn->handle, RSTRING_PTR(value), RSTRING_LEN(value)) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    return Qnil;
}

static VALUE conn_set_action(VALUE self, VALUE action)
{
    return set_char(self, action, dpiConn_setAction);
}

static VALUE conn_set_call_timeout(VALUE self, VALUE secs)
{
    Conn_t *conn = To_Conn(self);
    double fsecs = NUM2DBL(secs);
    uint32_t msecs = 0;

    if (fsecs > 0) {
        msecs = fsecs * 1000;
        if (msecs == 0) {
            msecs = 1;
        }
    }
    if (dpiConn_setCallTimeout(conn->dconn->handle, msecs) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    return Qnil;
}

static VALUE conn_set_client_identifier(VALUE self, VALUE action)
{
    return set_char(self, action, dpiConn_setClientIdentifier);
}

static VALUE conn_set_client_info(VALUE self, VALUE action)
{
    return set_char(self, action, dpiConn_setClientInfo);
}

static VALUE conn_set_current_schema(VALUE self, VALUE action)
{
    return set_char(self, action, dpiConn_setCurrentSchema);
}

static VALUE conn_set_db_op(VALUE self, VALUE action)
{
    return set_char(self, action, dpiConn_setDbOp);
}

static VALUE conn_set_external_name(VALUE self, VALUE action)
{
    return set_char(self, action, dpiConn_setExternalName);
}

static VALUE conn_set_internal_name(VALUE self, VALUE action)
{
    return set_char(self, action, dpiConn_setInternalName);
}

static VALUE conn_set_module(VALUE self, VALUE action)
{
    return set_char(self, action, dpiConn_setModule);
}

static VALUE conn_soda_db(VALUE self)
{
    Conn_t *conn = To_Conn(self);
    dpiSodaDb *db;

    if (dpiConn_getSodaDb(conn->dconn->handle, &db) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    return rboradb_soda_db_new(db, conn->dconn);
}

static VALUE conn_set_oci_attr(VALUE self, VALUE handle_type, VALUE attr, VALUE attr_data_type, VALUE value)
{
    Conn_t *conn = To_Conn(self);
    uint32_t htype = rboradb_to_OciHandleType(handle_type);
    OciAttrDataType type = rboradb_to_OciAttrDataType(attr_data_type);
    union {
        int boolval;
        char *textval;
        uint8_t ub1val;
        uint16_t ub2val;
        uint32_t ub4val;
        uint64_t ub8val;
    } val;
    uint32_t len = 0;

    switch (type) {
    case RBORADB_OCI_ATTR_DATA_TYPE_BOOLEAN:
        val.boolval = RTEST(value) ? 1 : 0;
        break;
    case RBORADB_OCI_ATTR_DATA_TYPE_TEXT:
        ExportString(value);
        val.textval = RSTRING_PTR(value);
        len = RSTRING_LEN(value);
        break;
    case RBORADB_OCI_ATTR_DATA_TYPE_UB1:
        val.ub1val = NUM2UINT(value);
        break;
    case RBORADB_OCI_ATTR_DATA_TYPE_UB2:
        val.ub2val = NUM2UINT(value);
        break;
    case RBORADB_OCI_ATTR_DATA_TYPE_UB4:
        val.ub4val = NUM2UINT(value);
        break;
    case RBORADB_OCI_ATTR_DATA_TYPE_UB8:
        val.ub8val = NUM2ULL(value);
        break;
    }
    if (dpiConn_setOciAttr(conn->dconn->handle, htype, NUM2UINT(attr), &val, len) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    return Qnil;
}

static VALUE conn_set_stmt_cache_size(VALUE self, VALUE size)
{
    Conn_t *conn = To_Conn(self);

    if (dpiConn_setStmtCacheSize(conn->dconn->handle, NUM2UINT(size)) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    return Qnil;
}

static VALUE conn_shutdown_database(VALUE self, VALUE mode)
{
    Conn_t *conn = To_Conn(self);

    if (rbOraDBConn_shutdownDatabase(conn->dconn->handle, rboradb_to_dpiShutdownMode(mode)) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    return Qnil;
}

static VALUE conn_startup_database(int argc, VALUE *argv, VALUE self)
{
    Conn_t *conn = To_Conn(self);
    VALUE mode, pfile;

    rb_scan_args(argc, argv, "11", &mode, &pfile);
    OptExportString(pfile);
    if (rbOraDBConn_startupDatabaseWithPfile(conn->dconn->handle, OPT_RSTRING_PTR(pfile), OPT_RSTRING_LEN(pfile), rboradb_to_dpiShutdownMode(mode)) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(conn);
    }
    RB_GC_GUARD(pfile);
    return Qnil;
}

void rboradb_conn_init(VALUE mOracleDB)
{
    cConn = rb_define_class_under(mOracleDB, "Conn", rb_cObject);
    rb_define_alloc_func(cConn, conn_alloc);
    rb_define_method(cConn, "initialize", conn_initialize, -1);
    rb_define_private_method(cConn, "initialize_copy", rboradb_notimplement, -1);
    rb_define_method(cConn, "begin_distrib_trans", conn_begin_distrib_trans, 3);
    rb_define_method(cConn, "break_execution", conn_break_execution, 0);
    rb_define_method(cConn, "change_password", conn_change_password, 3);
    rb_define_method(cConn, "close", conn_close, -1);
    rb_define_method(cConn, "commit", conn_commit, 0);
    rb_define_method(cConn, "call_timeout", conn_call_timeout, 0);
    rb_define_method(cConn, "current_schema", conn_current_schema, 0);
    rb_define_method(cConn, "edition", conn_edition, 0);
    rb_define_method(cConn, "external_name", conn_external_name, 0);
    rb_define_method(cConn, "internal_name", conn_internal_name, 0);
    rb_define_method(cConn, "ltxid", conn_ltxid, 0);
    rb_define_method(cConn, "oci_attr", conn_oci_attr, 3);
    rb_define_method(cConn, "server_version", conn_server_version, 0);
    rb_define_method(cConn, "soda_db", conn_soda_db, 0);
    rb_define_method(cConn, "stmt_cache_size", conn_stmt_cache_size, 0);
    rb_define_method(cConn, "ping", conn_ping, 0);
    rb_define_method(cConn, "prepare_distrib_trans", conn_prepare_distrib_trans, 0);
    rb_define_private_method(cConn, "__prepare_stmt", conn___prepare_stmt, 4);
    rb_define_method(cConn, "rollback", conn_rollback, 0);
    rb_define_method(cConn, "action=", conn_set_action, 1);
    rb_define_method(cConn, "call_timeout=", conn_set_call_timeout, 1);
    rb_define_method(cConn, "client_identifier=", conn_set_client_identifier, 1);
    rb_define_method(cConn, "client_info=", conn_set_client_info, 1);
    rb_define_method(cConn, "current_schema=", conn_set_current_schema, 1);
    rb_define_method(cConn, "db_op=", conn_set_db_op, 1);
    rb_define_method(cConn, "external_name=", conn_set_external_name, 1);
    rb_define_method(cConn, "internal_name=", conn_set_internal_name, 1);
    rb_define_method(cConn, "module=", conn_set_module, 1);
    rb_define_method(cConn, "set_oci_attr", conn_set_oci_attr, 4);
    rb_define_method(cConn, "stmt_cache_size=", conn_set_stmt_cache_size, 1);
    rb_define_method(cConn, "shutdown_database", conn_shutdown_database, 1);
    rb_define_method(cConn, "startup_database", conn_startup_database, -1);
}

rbOraDBConn *rboradb_get_dconn_in_conn(VALUE obj)
{
    if (rb_typeddata_is_kind_of(obj, &conn_data_type)) {
        return ((Conn_t *)RTYPEDDATA_DATA(obj))->dconn;
    } else {
        return NULL;
    }
}
