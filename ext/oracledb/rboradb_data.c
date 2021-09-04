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

static VALUE sym_to_i;
static VALUE sym_to_f;

void rboradb_data_init(void)
{
    sym_to_i = ID2SYM(rb_intern("to_i"));
    sym_to_f = ID2SYM(rb_intern("to_f"));
}

VALUE rboradb_from_data(const dpiData *data, dpiNativeTypeNum native_type_num, dpiOracleTypeNum oracle_type_num, dpiObjectType *objtype, VALUE filter, rbOraDBConn *dconn)
{
    VALUE obj;

    if (data->isNull) {
        return Qnil;
    }
    obj = rboradb_from_data_buffer(&data->value, native_type_num, oracle_type_num, objtype, &filter, dconn);
    if (!NIL_P(filter)) {
        obj = rb_proc_call_with_block(filter, 1, &obj, Qnil);
    }
    return obj;
}

VALUE rboradb_from_data_buffer(const dpiDataBuffer *value, dpiNativeTypeNum native_type_num, dpiOracleTypeNum oracle_type_num, dpiObjectType *objtype, VALUE *filter, rbOraDBConn *dconn)
{
    switch (native_type_num) {
    case DPI_NATIVE_TYPE_INT64:
        return LL2NUM(value->asInt64);
    case DPI_NATIVE_TYPE_UINT64:
        return ULL2NUM(value->asUint64);
    case DPI_NATIVE_TYPE_FLOAT:
        return DBL2NUM(value->asFloat);
    case DPI_NATIVE_TYPE_DOUBLE:
        return DBL2NUM(value->asDouble);
    case DPI_NATIVE_TYPE_BYTES:
        if (*filter == sym_to_i || *filter == sym_to_f || oracle_type_num == DPI_ORACLE_TYPE_NVARCHAR) {
            VALUE tmp, obj;
            char *buf = RB_ALLOCV_N(char, tmp, value->asBytes.length + 1);

            memcpy(buf, value->asBytes.ptr, value->asBytes.length);
            buf[value->asBytes.length] = '\0';
            if (*filter == sym_to_i) {
                obj = rb_cstr2inum(buf, 10);
                *filter = Qnil;
            } else if (*filter == sym_to_f) {
                obj = DBL2NUM(rb_cstr_to_dbl(buf, 0));
                *filter = Qnil;
            } else if (strchr(buf, '.') == NULL) {
                obj = rb_cstr2inum(buf, 10);
            } else {
                obj = DBL2NUM(rb_cstr_to_dbl(buf, 0));
            }
            RB_ALLOCV_END(tmp);
            return obj;
        }
        switch (oracle_type_num) {
        case DPI_ORACLE_TYPE_VARCHAR:
        case DPI_ORACLE_TYPE_CHAR:
            return rb_enc_str_new(value->asBytes.ptr, value->asBytes.length, rb_utf8_encoding());
        case DPI_ORACLE_TYPE_NVARCHAR:
        case DPI_ORACLE_TYPE_NCHAR:
            return rb_enc_str_new(value->asBytes.ptr, value->asBytes.length, rb_utf8_encoding());
        default:
            return rb_str_new(value->asBytes.ptr, value->asBytes.length);
        }
    case DPI_NATIVE_TYPE_TIMESTAMP:
        return rboradb_from_dpiTimestamp(&value->asTimestamp);
    case DPI_NATIVE_TYPE_INTERVAL_DS:
        return rboradb_from_dpiIntervalDS(&value->asIntervalDS);
    case DPI_NATIVE_TYPE_INTERVAL_YM:
        return rboradb_from_dpiIntervalYM(&value->asIntervalYM);
    case DPI_NATIVE_TYPE_LOB:
        return rboradb_from_dpiLob(value->asLOB, dconn, 1);
    case DPI_NATIVE_TYPE_OBJECT:
        return rboradb_from_dpiObject(value->asObject, objtype, dconn, 1);
    case DPI_NATIVE_TYPE_STMT:
        return rboradb_from_dpiStmt(value->asStmt, dconn, 1, 0);
    case DPI_NATIVE_TYPE_BOOLEAN:
        return value->asBoolean ? Qtrue : Qfalse;
    case DPI_NATIVE_TYPE_ROWID:
        return rboradb_from_dpiRowid(value->asRowid, dconn, 1);
    case DPI_NATIVE_TYPE_JSON:
        return rboradb_dpiJson2ruby(value->asJson, dconn);
    }
    rb_raise(rb_eRuntimeError, "unsupported native type %u", native_type_num);
}

VALUE rboradb_set_data(VALUE obj, dpiData *data, dpiNativeTypeNum native_type_num, dpiOracleTypeNum oracle_type_num, rbOraDBConn *dconn, dpiVar *var, uint32_t pos)
{
    int err = DPI_SUCCESS;
    VALUE gc_guard = Qnil;

    switch (native_type_num) {
    case DPI_NATIVE_TYPE_INT64:
        data->value.asInt64 = NUM2LL(obj);
        data->isNull = 0;
        break;
    case DPI_NATIVE_TYPE_UINT64:
        data->value.asUint64 = NUM2ULL(obj);
        data->isNull = 0;
        break;
    case DPI_NATIVE_TYPE_FLOAT:
        data->value.asFloat = NUM2DBL(obj);
        data->isNull = 0;
        break;
    case DPI_NATIVE_TYPE_DOUBLE:
        data->value.asDouble = NUM2DBL(obj);
        data->isNull = 0;
        break;
    case DPI_NATIVE_TYPE_BYTES:
        StringValue(obj);
        switch (oracle_type_num) {
        case DPI_ORACLE_TYPE_VARCHAR:
        case DPI_ORACLE_TYPE_CHAR:
            obj = rb_str_export_to_enc(obj, rb_utf8_encoding());
            break;
        case DPI_ORACLE_TYPE_NVARCHAR:
        case DPI_ORACLE_TYPE_NCHAR:
            obj = rb_str_export_to_enc(obj, rb_utf8_encoding());
            break;
        case DPI_ORACLE_TYPE_NUMBER:
            obj = rb_str_export_to_enc(obj, rb_usascii_encoding());
            break;
        }
        gc_guard = obj;
        if (var) {
            err = dpiVar_setFromBytes(var, pos, RSTRING_PTR(obj), RSTRING_LEN(obj));
        } else {
            data->value.asBytes.ptr = RSTRING_PTR(obj);
            data->value.asBytes.length = RSTRING_LEN(obj);
            data->isNull = 0;
        }
        break;
    case DPI_NATIVE_TYPE_TIMESTAMP:
        data->value.asTimestamp = *rboradb_to_dpiTimestamp(obj);
        data->isNull = 0;
        break;
    case DPI_NATIVE_TYPE_INTERVAL_DS:
        data->value.asIntervalDS = *rboradb_to_dpiIntervalDS(obj);
        data->isNull = 0;
        break;
    case DPI_NATIVE_TYPE_INTERVAL_YM:
        data->value.asIntervalYM = *rboradb_to_dpiIntervalYM(obj);
        data->isNull = 0;
        break;
    case DPI_NATIVE_TYPE_LOB:
        if (var) {
            err = dpiVar_setFromLob(var, pos, rboradb_to_dpiLob(obj));
        } else {
            data->value.asLOB = rboradb_to_dpiLob(obj);
            data->isNull = 0;
        }
        break;
    case DPI_NATIVE_TYPE_OBJECT:
        if (var) {
            err = dpiVar_setFromObject(var, pos, rboradb_to_dpiObject(obj));
        } else {
            data->value.asObject = rboradb_to_dpiObject(obj);
            data->isNull = 0;
        }
        break;
    case DPI_NATIVE_TYPE_STMT:
        if (var) {
            err = dpiVar_setFromStmt(var, pos, rboradb_to_dpiStmt(obj));
        } else {
            data->value.asStmt = rboradb_to_dpiStmt(obj);
            data->isNull = 0;
        }
        break;
    case DPI_NATIVE_TYPE_BOOLEAN:
        data->value.asBoolean = RTEST(obj);
        data->isNull = 0;
        break;
    case DPI_NATIVE_TYPE_ROWID:
        if (var) {
            err = dpiVar_setFromRowid(var, pos, rboradb_to_dpiRowid(obj));
        } else {
            data->value.asRowid = rboradb_to_dpiRowid(obj);
            data->isNull = 0;
        }
        break;
    case DPI_NATIVE_TYPE_JSON:
        rboradb_ruby2dpiJson(obj, data->value.asJson, dconn);
        data->isNull = 0;
        break;
    default:
        rb_raise(rb_eRuntimeError, "unsupported native type %u", native_type_num);
    }
    if (err != DPI_SUCCESS) {
        rboradb_raise_error(dconn->ctxt);
    }
    return gc_guard;
}
