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

#define To_Stmt(obj) ((Stmt_t *)rb_check_typeddata((obj), &stmt_data_type))

static ID id_at_array_size;
static ID id_at_bind_vars;
static ID id_at_define_vars;
static ID id_at_defined;
static ID id_at_info;
static ID id_at_num_query_columns;
static VALUE cStmt;

typedef struct {
    RBORADB_COMMON_HEADER(dpiStmt);
    uint32_t num_query_columns;
    uint32_t buffer_row_index;
} Stmt_t;

static void stmt_free(void *arg)
{
    Stmt_t *stmt = (Stmt_t *)arg;
    RBORADB_RELEASE(stmt, dpiStmt);
    xfree(arg);
}

static const struct rb_data_type_struct stmt_data_type = {
    "OracleDB::Stmt",
    {NULL, stmt_free,},
    NULL, NULL,
};

static VALUE stmt_alloc(VALUE klass)
{
    Stmt_t *stmt;
    return TypedData_Make_Struct(klass, Stmt_t, &stmt_data_type, stmt);
}

static VALUE stmt___initialize(VALUE self, VALUE conn, VALUE sql, VALUE fetch_array_size, VALUE scrollable, VALUE tag)
{
    Stmt_t *stmt = To_Stmt(self);
    rbOraDBConn *dconn = rboradb_get_dconn(conn);
    uint32_t array_size;

    ExportString(sql);
    array_size = NUM2UINT(fetch_array_size);
    OptExportString(tag);

    RBORADB_INIT(stmt, dconn);
    if (dpiConn_prepareStmt(dconn->handle, RTEST(scrollable), RSTRING_PTR(sql), RSTRING_LEN(sql),
                            OPT_RSTRING_PTR(tag), OPT_RSTRING_LEN(tag), &stmt->handle) != DPI_SUCCESS) {
        rboradb_raise_error(dconn->ctxt);
    }
    rb_ivar_set(self, id_at_array_size, UINT2NUM(array_size));
    return Qnil;
}

static VALUE stmt___bind_by_name(VALUE self, VALUE name, VALUE var)
{
    Stmt_t *stmt = To_Stmt(self);

    ExportString(name);
    if (dpiStmt_bindByName(stmt->handle, RSTRING_PTR(name), RSTRING_LEN(name), rboradb_to_dpiVar(var)) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(stmt);
    }
    return Qnil;
}

static VALUE stmt___bind_by_pos(VALUE self, VALUE pos, VALUE var)
{
    Stmt_t *stmt = To_Stmt(self);

    if (dpiStmt_bindByPos(stmt->handle, NUM2UINT(pos), rboradb_to_dpiVar(var)) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(stmt);
    }
    return Qnil;
}

static VALUE stmt_close(int argc, VALUE *argv, VALUE self)
{
    Stmt_t *stmt = To_Stmt(self);
    VALUE tag;

    rb_scan_args(argc, argv, "01", &tag);
    OptExportString(tag);
    if (dpiStmt_close(stmt->handle, OPT_RSTRING_PTR(tag), OPT_RSTRING_LEN(tag)) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(stmt);
    }
    RB_GC_GUARD(tag);
    return Qnil;
}

static VALUE stmt_batch_errors(VALUE self)
{
    Stmt_t *stmt = To_Stmt(self);
    uint32_t i, count;
    dpiErrorInfo *errors;
    VALUE ary, tmp;

    if (dpiStmt_getBatchErrorCount(stmt->handle, &count) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(stmt);
    }
    ary = rb_ary_new_capa(count);
    if (count == 0) {
        return ary;
    }
    errors = RB_ALLOCV_N(dpiErrorInfo, tmp, count);
    if (dpiStmt_getBatchErrors(stmt->handle, count, errors) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(stmt);
    }
    for (i = 0; i < count; i++) {
        rb_ary_push(ary, rboradb_from_dpiErrorInfo(&errors[i]));
    }
    RB_ALLOCV_END(tmp);
    return ary;
}

static VALUE stmt_bind_count(VALUE self)
{
    GET_UINT32(Stmt, BindCount);
}

static VALUE stmt_bind_names(VALUE self)
{
    Stmt_t *stmt = To_Stmt(self);
    const char **names;
    uint32_t *name_lengths;
    uint32_t idx, count;
    VALUE bind_names;

    if (dpiStmt_getBindCount(stmt->handle, &count) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(stmt);
    }
    names = ALLOCA_N(const char *, count);
    name_lengths = ALLOCA_N(uint32_t, count);
    if (dpiStmt_getBindNames(stmt->handle, &count, names, name_lengths) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(stmt);
    }
    bind_names = rb_ary_new_capa(count);
    for (idx = 0; idx < count; idx++) {
        rb_ary_push(bind_names, rb_enc_str_new(names[idx], name_lengths[idx], rb_utf8_encoding()));
    }
    return bind_names;
}

static VALUE stmt_fetch_array_size(VALUE self)
{
    return rb_ivar_get(self, id_at_array_size);
}

static VALUE stmt_implicit_result(VALUE self)
{
    Stmt_t *stmt = To_Stmt(self);
    dpiStmt *implicit_result;

    if (dpiStmt_getImplicitResult(stmt->handle, &implicit_result) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(stmt);
    }
    return implicit_result ? rboradb_from_dpiStmt(implicit_result, stmt->dconn, 0, 0) : Qnil;
}

static VALUE stmt___info(VALUE self)
{
    GET_STRUCT(Stmt, Info, dpiStmtInfo);
}

static VALUE stmt_last_rowid(VALUE self)
{
    Stmt_t *stmt = To_Stmt(self);
    dpiRowid *rowid;

    if (dpiStmt_getLastRowid(stmt->handle, &rowid) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(stmt);
    }
    return rowid ? rboradb_from_dpiRowid(rowid, stmt->dconn, 1) : Qnil;
}

static VALUE stmt___execute(VALUE self, VALUE mode)
{
    Stmt_t *stmt = To_Stmt(self);
    uint32_t num_query_columns;

    if (rbOraDBStmt_execute(stmt->dconn->handle, stmt->handle, rboradb_to_dpiExecMode(mode), &num_query_columns) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(stmt);
    }
    stmt->num_query_columns = num_query_columns;
    return INT2FIX(num_query_columns);
}

static VALUE stmt___execute_many(VALUE self, VALUE mode, VALUE num_iters)
{
    Stmt_t *stmt = To_Stmt(self);

    if (rbOraDBStmt_executeMany(stmt->dconn->handle, stmt->handle, rboradb_to_dpiExecMode(mode), NUM2UINT(num_iters)) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(stmt);
    }
    return Qnil;
}

static VALUE stmt___define(VALUE self, VALUE pos, VALUE var)
{
    Stmt_t *stmt = To_Stmt(self);

    if (dpiStmt_define(stmt->handle, NUM2UINT(pos), rboradb_to_dpiVar(var)) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(stmt);
    }
    return Qnil;
}

static VALUE stmt_num_query_columns(VALUE self)
{
    Stmt_t *stmt = To_Stmt(self);
    return UINT2NUM(stmt->num_query_columns);
}

static VALUE stmt_oci_attr(VALUE self, VALUE attr, VALUE attr_data_type)
{
    Stmt_t *stmt = To_Stmt(self);
    OciAttrDataType type = rboradb_to_OciAttrDataType(attr_data_type);
    dpiDataBuffer val;
    uint32_t len;

    if (dpiStmt_getOciAttr(stmt->handle, NUM2UINT(attr), &val, &len) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(stmt);
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

static VALUE stmt_prefetch_rows(VALUE self)
{
    GET_UINT32(Stmt, PrefetchRows);
}

static VALUE stmt_query_info(VALUE self, VALUE pos)
{
    Stmt_t *stmt = To_Stmt(self);
    dpiQueryInfo info;

    if (dpiStmt_getQueryInfo(stmt->handle, NUM2UINT(pos), &info) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(stmt);
    }
    return rboradb_from_dpiQueryInfo(&info, stmt->dconn);
}

static VALUE stmt___fetch(VALUE self)
{
    Stmt_t *stmt = To_Stmt(self);
    int found;

    if (rbOraDBStmt_fetch(stmt->dconn->handle, stmt->handle, &found, &stmt->buffer_row_index) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(stmt);
    }
    return found ? UINT2NUM(stmt->buffer_row_index) : Qnil;
}

static VALUE stmt_row_count(VALUE self)
{
    GET_UINT64(Stmt, RowCount);
}

static VALUE stmt_row_counts(VALUE self)
{
    Stmt_t *stmt = To_Stmt(self);
    uint32_t i, num_counts;
    uint64_t *counts;
    VALUE ary;

    if (dpiStmt_getRowCounts(stmt->handle, &num_counts, &counts) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(stmt);
    }
    ary = rb_ary_new_capa(num_counts);
    for (i = 0; i < num_counts; i++) {
        rb_ary_push(ary, ULL2NUM(counts[i]));
    }
    return ary;
}

static VALUE stmt_subscr_query_id(VALUE self)
{
    GET_UINT64(Stmt, SubscrQueryId);
}

static VALUE stmt_scroll(int argc, VALUE *argv, VALUE self)
{
    Stmt_t *stmt = To_Stmt(self);
    VALUE mode;
    VALUE offset;

    rb_scan_args(argc, argv, "11", &mode, &offset);
    if (rbOraDBStmt_scroll(stmt->dconn->handle, stmt->handle, rboradb_to_dpiFetchMode(mode), NIL_P(offset) ? 0 : NUM2INT(offset), stmt->buffer_row_index) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(stmt);
    }
    return Qnil;
}

static VALUE stmt_set_fetch_array_size(VALUE self, VALUE array_size)
{
    Stmt_t *stmt = To_Stmt(self);
    uint32_t size = NUM2UINT(array_size);

    if (dpiStmt_setFetchArraySize(stmt->handle, size) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(stmt);
    }
    rb_ivar_set(self, id_at_array_size, UINT2NUM(size));
    return Qnil;
}

static VALUE stmt_set_prefetch_rows(VALUE self, VALUE obj)
{
    SET_UINT32(Stmt, PrefetchRows);
}

static VALUE stmt_set_oci_attr(VALUE self, VALUE attr, VALUE attr_data_type, VALUE value)
{
    Stmt_t *stmt = To_Stmt(self);
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
    if (dpiStmt_setOciAttr(stmt->handle, NUM2UINT(attr), &val, len) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(stmt);
    }
    return Qnil;
}

void rboradb_stmt_init(VALUE mOracleDB)
{
    id_at_array_size = rb_intern("@array_size");
    id_at_bind_vars = rb_intern("@bind_vars");
    id_at_define_vars = rb_intern("@define_vars");
    id_at_defined = rb_intern("@defined");
    id_at_info = rb_intern("@info");
    id_at_num_query_columns = rb_intern("@num_query_columns");

    cStmt = rb_define_class_under(mOracleDB, "Stmt", rb_cObject);
    rb_define_alloc_func(cStmt, stmt_alloc);
    rb_define_private_method(cStmt, "__initialize", stmt___initialize, 5);
    rb_define_private_method(cStmt, "initialize_copy", rboradb_notimplement, -1);
    rb_define_private_method(cStmt, "__bind_by_name", stmt___bind_by_name, 2);
    rb_define_private_method(cStmt, "__bind_by_pos", stmt___bind_by_pos, 2);
    rb_define_method(cStmt, "close", stmt_close, -1);
    rb_define_method(cStmt, "batch_errors", stmt_batch_errors, 0);
    rb_define_method(cStmt, "bind_count", stmt_bind_count, 0);
    rb_define_method(cStmt, "bind_names", stmt_bind_names, 0);
    rb_define_method(cStmt, "fetch_array_size", stmt_fetch_array_size, 0);
    rb_define_method(cStmt, "implicit_result", stmt_implicit_result, 0);
    rb_define_private_method(cStmt, "__info", stmt___info, 0);
    rb_define_method(cStmt, "last_rowid", stmt_last_rowid, 0);
    rb_define_private_method(cStmt, "__execute", stmt___execute, 1);
    rb_define_private_method(cStmt, "__execute_many", stmt___execute_many, 2);
    rb_define_private_method(cStmt, "__define", stmt___define, 2);
    rb_define_method(cStmt, "num_query_columns", stmt_num_query_columns, 0);
    rb_define_method(cStmt, "oci_attr", stmt_oci_attr, 2);
    rb_define_method(cStmt, "prefetch_rows", stmt_prefetch_rows, 0);
    rb_define_method(cStmt, "query_info", stmt_query_info, 1);
    rb_define_private_method(cStmt, "__fetch", stmt___fetch, 0);
    rb_define_method(cStmt, "row_count", stmt_row_count, 0);
    rb_define_method(cStmt, "row_counts", stmt_row_counts, 0);
    rb_define_method(cStmt, "subscr_query_id", stmt_subscr_query_id, 0);
    rb_define_method(cStmt, "scroll", stmt_scroll, -1);
    rb_define_method(cStmt, "fetch_array_size=", stmt_set_fetch_array_size, 1);
    rb_define_method(cStmt, "prefetch_rows=", stmt_set_prefetch_rows, 1);
    rb_define_method(cStmt, "set_oci_attr", stmt_set_oci_attr, 3);
}

dpiStmt *rboradb_to_dpiStmt(VALUE obj)
{
    return To_Stmt(obj)->handle;
}

VALUE rboradb_from_dpiStmt(dpiStmt *handle, rbOraDBConn *dconn, int ref, uint32_t fetch_array_size)
{
    Stmt_t *stmt;
    VALUE obj = TypedData_Make_Struct(cStmt, Stmt_t, &stmt_data_type, stmt);

    if (ref) {
        RBORADB_SET(stmt, dpiStmt, handle, dconn);
    } else {
        RBORADB_INIT(stmt, dconn);
        stmt->handle = handle;
    }
    if (fetch_array_size == 0) {
        if (dpiStmt_getFetchArraySize(stmt->handle, &fetch_array_size) != DPI_SUCCESS) {
            RBORADB_RAISE_ERROR(stmt);
        }
    }
    rb_ivar_set(obj, id_at_array_size, UINT2NUM(fetch_array_size));
    rb_ivar_set(obj, id_at_bind_vars, Qnil);
    rb_ivar_set(obj, id_at_define_vars, Qnil);
    rb_ivar_set(obj, id_at_defined, Qfalse);
    rb_ivar_set(obj, id_at_info, Qnil);
    rb_ivar_set(obj, id_at_num_query_columns, Qnil);
    return obj;
}

rbOraDBConn *rboradb_get_dconn_in_stmt(VALUE obj)
{
    if (rb_typeddata_is_kind_of(obj, &stmt_data_type)) {
        return ((Stmt_t *)RTYPEDDATA_DATA(obj))->dconn;
    } else {
        return NULL;
    }
}
