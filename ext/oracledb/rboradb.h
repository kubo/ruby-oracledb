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
#ifndef RBORADB_H
#define RBORADB_H

#include <ruby.h>
#include <ruby/atomic.h>
#include <ruby/encoding.h>
#include "dpi.h"
#include "_gen_dpi_funcs.h"
#include "_gen_dpi_enums.h"

#define RBORADB_COMMON_HEADER(type) \
    type *handle; \
    rbOraDBConn *dconn

#define RBORADB_INIT(var_, dconn_) do { \
    (var_)->dconn = (dconn_); \
    if ((var_)->dconn) { \
        rbOraDBConn_addRef((var_)->dconn); \
    } \
} while (0)

#define RBORADB_SET(var_, type_, handle_, dconn_) do { \
    (var_)->handle = (handle_); \
    if ((var_)->handle) { \
        type_##_addRef((var_)->handle); \
    } \
    (var_)->dconn = (dconn_); \
    if ((var_)->dconn) { \
        rbOraDBConn_addRef((var_)->dconn); \
    } \
} while (0)

#define RBORADB_RELEASE(var_, type_) do { \
    if ((var_) && (var_)->handle) { \
        type_##_release((var_)->handle); \
        (var_)->handle = NULL; \
    } \
    if ((var_) && (var_)->dconn) { \
        rbOraDBConn_release((var_)->dconn); \
        (var_)->dconn = NULL; \
    } \
} while (0)

#define RBORADB_RAISE_ERROR(var_) do { \
    rboradb_raise_error((var_)->dconn->ctxt); \
} while (0)

#define rb_define_method_nodoc rb_define_method

typedef struct {
    rb_atomic_t refcnt;
    dpiContext *handle;
} rbOraDBContext;

typedef struct {
    rb_atomic_t refcnt;
    dpiConn *handle;
    rbOraDBContext *ctxt;
} rbOraDBConn;

#define ExportString(s) do { \
    SafeStringValue(s); \
    s = rb_str_export_to_enc(s, rb_utf8_encoding());	\
} while (0)
#define OptExportString(s) do { \
    if (!NIL_P(s)) { \
        SafeStringValue(s); \
        s = rb_str_export_to_enc(s, rb_utf8_encoding()); \
    } \
} while (0)
#define OPT_RSTRING_PTR(s) ((NIL_P(s)) ? NULL : RSTRING_PTR(s))
#define OPT_RSTRING_LEN(s) ((NIL_P(s)) ? 0 : RSTRING_LEN(s))

#define GET_VAL(Type, Name, ValType, Val2Ruby) do { \
    Type##_t *var = To_##Type(self); \
    ValType val; \
    if (dpi##Type##_get##Name(var->handle, &val) != DPI_SUCCESS) { \
        RBORADB_RAISE_ERROR(var); \
    } \
    return Val2Ruby; \
} while (0)

#define GET_VAL2(Type, Name, VarType1, VarType2, Val2Ruby) do { \
    Type##_t *ptr = To_##Type(self); \
    VarType1 val1; \
    VarType2 val2; \
    if (dpi##Type##_get##Name(ptr->handle, &val1, &val2) != DPI_SUCCESS) { \
        RBORADB_RAISE_ERROR(ptr); \
    } \
    return Val2Ruby; \
} while (0)

#define GET_ENUM(Type, Name, Enum) GET_VAL(Type, Name, Enum, rboradb_from_##Enum(val))
#define GET_STRUCT(Type, Name, Struct) GET_VAL(Type, Name, Struct, rboradb_from_##Struct(&val))
#define GET_UINT32(Type, Name) GET_VAL(Type, Name, uint32_t, UINT2NUM(val))
#define GET_INT32(Type, Name) GET_VAL(Type, Name, int32_t, INT2NUM(val))
#define GET_UINT64(Type, Name) GET_VAL(Type, Name, uint64_t, ULL2NUM(val))
#define GET_BOOL(Type, Name) GET_VAL(Type, Name, int, val ? Qtrue : Qfalse)
#define GET_STR(Type, Name) GET_VAL2(Type, Name, const char*, uint32_t, rb_enc_str_new(val1, val2, rb_utf8_encoding()))

#define SET_VAL(Type, Name, Ruby2Val) do { \
    Type##_t *ptr = To_##Type(self); \
    if (dpi##Type##_set##Name(ptr->handle, Ruby2Val) != DPI_SUCCESS) { \
        RBORADB_RAISE_ERROR(ptr); \
    } \
    return Qnil; \
} while (0)

#define SET_INT32(Type, Name) SET_VAL(Type, Name, NUM2INT(obj))
#define SET_UINT32(Type, Name) SET_VAL(Type, Name, NUM2UINT(obj))
#define SET_ENUM(Type, Name, Enum) SET_VAL(Type, Name, rboradb_to_##Enum(obj))
#define SET_STR(Type, Name) do { \
    Type##_t *ptr = To_##Type(self); \
    ExportString(obj); \
    if (dpi##Type##_set##Name(ptr->handle, RSTRING_PTR(obj), RSTRING_LEN(obj)) != DPI_SUCCESS) { \
        RBORADB_RAISE_ERROR(ptr); \
    } \
    return Qnil; \
} while (0)

#define IVAR_SET(obj, name, value) do { \
    ID id__; \
    CONST_ID(id__, name); \
    rb_ivar_set(obj, id__, value); \
} while (0)

static inline void rbOraDBContext_addRef(rbOraDBContext *ctxt)
{
    RUBY_ATOMIC_INC(ctxt->refcnt);
}

static inline void rbOraDBContext_release(rbOraDBContext *ctxt)
{
    if (RUBY_ATOMIC_FETCH_SUB(ctxt->refcnt, 1) == 1) {
        dpiContext_destroy(ctxt->handle);
        xfree(ctxt);
    }
}

static inline void rbOraDBConn_addRef(rbOraDBConn *dconn)
{
    RUBY_ATOMIC_INC(dconn->refcnt);
}

static inline void rbOraDBConn_release(rbOraDBConn *dconn)
{
    if (RUBY_ATOMIC_FETCH_SUB(dconn->refcnt, 1) == 1) {
        dpiConn_release(dconn->handle);
        rbOraDBContext_release(dconn->ctxt);
        xfree(dconn);
    }
}

// rboradb.c
rbOraDBContext *rboradb_get_rbOraDBContext(VALUE obj);
rbOraDBConn *rboradb_get_dconn(VALUE obj);
VALUE rboradb_from_dpiErrorInfo(const dpiErrorInfo *error);
NORETURN(void rboradb_raise_error(rbOraDBContext *ctxt));
VALUE rboradb_notimplement(int argc, VALUE *argv, VALUE self);

// rboradb_aq.c
void rboradb_aq_init(VALUE mOracleDB);

// rboradb_conn.c
void rboradb_conn_init(VALUE mOracleDB);
rbOraDBConn *rboradb_get_dconn_in_conn(VALUE obj);
VALUE rboradb_to_conn(rbOraDBContext *ctxt, dpiConn* dpi_conn, const dpiConnCreateParams *params);

// rboradb_data.c
void rboradb_data_init(void);
VALUE rboradb_from_data(const dpiData *data, dpiNativeTypeNum native_type_num, dpiOracleTypeNum oracle_type_num, dpiObjectType *objtype, VALUE filter, rbOraDBConn *dconn);
VALUE rboradb_from_data_buffer(const dpiDataBuffer *value, dpiNativeTypeNum native_type_num, dpiOracleTypeNum oracle_type_num, dpiObjectType *objtype, VALUE *filter, rbOraDBConn *dconn);
VALUE rboradb_set_data(VALUE obj, dpiData *data, dpiNativeTypeNum native_type_num, dpiOracleTypeNum oracle_type_num, rbOraDBConn *dconn, dpiVar *var, uint32_t pos);

// rboradb_datetime.c
void rboradb_datetime_init(VALUE mOracleDB);
VALUE rboradb_from_dpiTimestamp(const dpiTimestamp *val);
VALUE rboradb_from_dpiIntervalDS(const dpiIntervalDS *val);
VALUE rboradb_from_dpiIntervalYM(const dpiIntervalYM *val);
dpiTimestamp *rboradb_to_dpiTimestamp(VALUE obj);
dpiIntervalDS *rboradb_to_dpiIntervalDS(VALUE obj);
dpiIntervalYM *rboradb_to_dpiIntervalYM(VALUE obj);
int rboradb_is_Timestamp(VALUE obj);
int rboradb_is_IntervalDS(VALUE obj);
int rboradb_is_IntervalYM(VALUE obj);

// rboradb_info_types.c
void rboradb_info_types_init(VALUE mOracleDB);
VALUE rboradb_from_dpiDataTypeInfo(const dpiDataTypeInfo *info, rbOraDBConn *dconn);
VALUE rboradb_from_dpiObjectAttrInfo(const dpiObjectAttrInfo *info, rbOraDBConn *dconn);
VALUE rboradb_from_dpiObjectTypeInfo(const dpiObjectTypeInfo *info, rbOraDBConn *dconn);
VALUE rboradb_from_dpiQueryInfo(const dpiQueryInfo *info, rbOraDBConn *dconn);
VALUE rboradb_from_dpiStmtInfo(const dpiStmtInfo *info);
VALUE rboradb_from_dpiVersionInfo(const dpiVersionInfo *info);

// rboradb_json.c
void rboradb_json_init(VALUE mOracleDB);
VALUE rboradb_dpiJson2ruby(dpiJson *handle, rbOraDBConn *dconn);
void rboradb_ruby2dpiJson(VALUE obj, dpiJson *handle, rbOraDBConn *dconn);

// rboradb_lob.c
void rboradb_lob_init(VALUE mOracleDB);
VALUE rboradb_from_dpiLob(dpiLob *handle, rbOraDBConn *dconn, int ref);
dpiLob *rboradb_to_dpiLob(VALUE obj);

// rboradb_object.c
void rboradb_object_init(VALUE mOracleDB);
VALUE rboradb_from_dpiObject(dpiObject *handle, dpiObjectType *objtype, rbOraDBConn *dconn, int ref);
VALUE rboradb_from_dpiObjectType(dpiObjectType *handle, rbOraDBConn *dconn, int ref);
dpiObjectType *rboradb_get_ObjectType_or_null(VALUE obj);
dpiObject *rboradb_to_dpiObject(VALUE obj);
dpiObjectType *rboradb_to_dpiObjectType(VALUE obj);

// rboradb_params.c
void rboradb_params_init(void);
VALUE rboradb_set_dpiContextCreateParams(dpiContextCreateParams *context_params, VALUE hash);
VALUE rboradb_set_common_and_dpiConnCreateParams(dpiCommonCreateParams *common_params, dpiConnCreateParams *conn_params, VALUE hash);
VALUE rboradb_set_common_and_dpiPoolCreateParams(dpiCommonCreateParams *common_params, dpiPoolCreateParams *pool_params, VALUE hash);
VALUE rboradb_set_dpiConnCreateParams(dpiConnCreateParams *conn_params, VALUE hash);
VALUE rboradb_set_dpiSodaOperOptions(dpiSodaOperOptions *opts, VALUE hash);
VALUE rboradb_set_dpiSubscrCreateParams(dpiSubscrCreateParams *params, VALUE hash);

// rboradb_pool.c
void rboradb_pool_init(VALUE mOracleDB);

// rboradb_rowid.c
void rboradb_rowid_init(VALUE mOracleDB);
VALUE rboradb_from_dpiRowid(dpiRowid *handle, rbOraDBConn *dconn, int ref);
dpiRowid *rboradb_to_dpiRowid(VALUE obj);

// rboradb_soda.c
void rboradb_soda_init(VALUE mOracleDB);
VALUE rboradb_soda_db_new(dpiSodaDb *handle, rbOraDBConn *dconn);

// rboradb_stmt.c
void rboradb_stmt_init(VALUE mOracleDB);
dpiStmt *rboradb_to_dpiStmt(VALUE obj);
VALUE rboradb_from_dpiStmt(dpiStmt *handle, rbOraDBConn *dconn, int ref, uint32_t fetch_array_size);
rbOraDBConn *rboradb_get_dconn_in_stmt(VALUE obj);

// rboradb_subscr.c
void rboradb_subscr_init(VALUE mOracleDB);

// rboradb_var.c
void rboradb_var_init(VALUE mOracleDB);
dpiVar *rboradb_to_dpiVar(VALUE obj);

#endif
