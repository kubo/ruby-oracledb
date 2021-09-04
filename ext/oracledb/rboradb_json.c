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

#define To_Json(obj) ((Json_t *)rb_check_typeddata((obj), &json_data_type))

#define ALLOC_TMP_BUF(type, v, n) ((type*)rb_alloc_tmp_buffer2(&(v), (n), sizeof(type)))

static VALUE cJson;

typedef struct {
    RBORADB_COMMON_HEADER(dpiJson);
} Json_t;

struct hash_to_json_arg {
    long idx, size;
    dpiJsonObject *json_obj;
    VALUE gc_guard;
};

static VALUE json_alloc(VALUE klass);
static void json_free(void *arg);
static void ruby_to_json(dpiJsonNode *node, VALUE value, VALUE gc_guard);
static void array_to_json(dpiJsonNode *node, VALUE value, VALUE gc_guard);
static void hash_to_json(dpiJsonNode *node, VALUE value, VALUE gc_guard);
static int hash_to_json_arg_i(VALUE key, VALUE value, VALUE arg);

static inline void fixnum_to_json(dpiJsonNode *node, VALUE value)
{
    node->oracleTypeNum = DPI_ORACLE_TYPE_NUMBER;
    node->nativeTypeNum = DPI_NATIVE_TYPE_INT64;
    node->value->asInt64 = FIX2LONG(value);
}

static inline void bignum_to_json(dpiJsonNode *node, VALUE value, VALUE gc_guard)
{
    StringValue(value);
    node->oracleTypeNum = DPI_ORACLE_TYPE_NUMBER;
    node->nativeTypeNum = DPI_NATIVE_TYPE_BYTES;
    node->value->asBytes.ptr = RSTRING_PTR(value);
    node->value->asBytes.length = RSTRING_LEN(value);
    rb_ary_push(gc_guard, value);
}

static inline void timestamp_to_json(dpiJsonNode *node, VALUE value)
{
    node->oracleTypeNum = DPI_ORACLE_TYPE_TIMESTAMP;
    node->nativeTypeNum = DPI_NATIVE_TYPE_TIMESTAMP;
    node->value->asTimestamp = *rboradb_to_dpiTimestamp(value);
}

static inline void interval_ds_to_json(dpiJsonNode *node, VALUE value)
{
    node->oracleTypeNum = DPI_ORACLE_TYPE_INTERVAL_DS;
    node->nativeTypeNum = DPI_NATIVE_TYPE_INTERVAL_DS;
    node->value->asIntervalDS = *rboradb_to_dpiIntervalDS(value);
}

static inline void interval_ym_to_json(dpiJsonNode *node, VALUE value)
{
    node->oracleTypeNum = DPI_ORACLE_TYPE_INTERVAL_YM;
    node->nativeTypeNum = DPI_NATIVE_TYPE_INTERVAL_YM;
    node->value->asIntervalYM = *rboradb_to_dpiIntervalYM(value);
}

static const struct rb_data_type_struct json_data_type = {
    "OracleDB::Json",
    {NULL, json_free,},
    NULL, NULL,
};

static VALUE json_alloc(VALUE klass)
{
    Json_t *json;
    return TypedData_Make_Struct(klass, Json_t, &json_data_type, json);
}

static void json_free(void *arg)
{
    Json_t *json = (Json_t *)arg;
    RBORADB_RELEASE(json, dpiJson);
    xfree(arg);
}

static VALUE json_to_ruby(const dpiJsonNode *node)
{
    VALUE obj, tmp;
    char *buf;
    uint32_t idx;
    dpiDataBuffer *value = node->value;

    switch (node->nativeTypeNum) {
    case DPI_NATIVE_TYPE_JSON_OBJECT:
        obj = rb_hash_new();
        for (idx = 0; idx < value->asJsonObject.numFields; idx++) {
            VALUE key = rb_enc_str_new(value->asJsonObject.fieldNames[idx], value->asJsonObject.fieldNameLengths[idx], rb_utf8_encoding());
            VALUE val = json_to_ruby(&value->asJsonObject.fields[idx]);
            rb_hash_aset(obj, key, val);
        }
        return obj;
    case DPI_NATIVE_TYPE_JSON_ARRAY:
        obj = rb_ary_new_capa(node->value->asJsonArray.numElements);
        for (idx = 0; idx < value->asJsonArray.numElements; idx++) {
            rb_ary_push(obj, json_to_ruby(&value->asJsonArray.elements[idx]));
        }
        return obj;
    case DPI_NATIVE_TYPE_BYTES:
        switch (node->oracleTypeNum) {
        case DPI_ORACLE_TYPE_VARCHAR:
            return rb_enc_str_new(value->asBytes.ptr, value->asBytes.length, rb_utf8_encoding());
        case DPI_ORACLE_TYPE_RAW:
            return rb_str_new(value->asBytes.ptr, value->asBytes.length);
        case DPI_ORACLE_TYPE_NUMBER:
            buf = RB_ALLOCV_N(char, tmp, value->asBytes.length + 1);
            memcpy(buf, value->asBytes.ptr, value->asBytes.length);
            buf[value->asBytes.length] = '\0';
            if (strchr(buf, '.') == NULL) {
                obj = rb_cstr2inum(buf, 10);
            } else {
                obj = DBL2NUM(rb_cstr_to_dbl(buf, 0));
            }
            RB_ALLOCV_END(tmp);
            return obj;
        }
        break;
    case DPI_NATIVE_TYPE_DOUBLE:
        return rb_float_new(value->asDouble);
    case DPI_NATIVE_TYPE_TIMESTAMP:
        return rboradb_from_dpiTimestamp(&value->asTimestamp);
    case DPI_NATIVE_TYPE_INTERVAL_DS:
        return rboradb_from_dpiIntervalDS(&value->asIntervalDS);
    case DPI_NATIVE_TYPE_INTERVAL_YM:
        return rboradb_from_dpiIntervalYM(&value->asIntervalYM);
    case DPI_NATIVE_TYPE_BOOLEAN:
        return value->asBoolean ? Qtrue : Qfalse;
    case DPI_NATIVE_TYPE_NULL:
        return Qnil;
    }
    rb_raise(rb_eRuntimeError, "unsupported native type num %d", node->nativeTypeNum);
}

static VALUE json_value(VALUE self)
{
    Json_t *json = To_Json(self);

    return rboradb_dpiJson2ruby(json->handle, json->dconn);
}

static void ruby_to_json(dpiJsonNode *node, VALUE value, VALUE gc_guard)
{
    enum ruby_value_type value_type = rb_type(value);
    ID id;
    VALUE tmp;

    switch (value_type) {
    case T_NIL:
        node->oracleTypeNum = DPI_ORACLE_TYPE_NONE;
        node->nativeTypeNum = DPI_NATIVE_TYPE_NULL;
        return;
    case T_TRUE:
        node->oracleTypeNum = DPI_ORACLE_TYPE_BOOLEAN;
        node->nativeTypeNum = DPI_NATIVE_TYPE_BOOLEAN;
        node->value->asBoolean = 1;
        return;
    case T_FALSE:
        node->oracleTypeNum = DPI_ORACLE_TYPE_BOOLEAN;
        node->nativeTypeNum = DPI_NATIVE_TYPE_BOOLEAN;
        node->value->asBoolean = 0;
        return;
    case T_FLOAT:
        node->oracleTypeNum = DPI_ORACLE_TYPE_NUMBER;
        node->nativeTypeNum = DPI_NATIVE_TYPE_DOUBLE;
        node->value->asDouble = RFLOAT_VALUE(value);
        return;
    case T_FIXNUM:
        fixnum_to_json(node, value);
        return;
    case T_BIGNUM:
        bignum_to_json(node, value, gc_guard);
        return;
    case T_ARRAY:
        array_to_json(node, value, gc_guard);
        return;
    case T_HASH:
        hash_to_json(node, value, gc_guard);
        return;
    default:
        ;
    }
    if (rboradb_is_Timestamp(value)) {
        timestamp_to_json(node, value);
        return;
    }
    if (rboradb_is_IntervalDS(value)) {
        interval_ds_to_json(node, value);
        return;
    }
    if (rboradb_is_IntervalYM(value)) {
        interval_ym_to_json(node, value);
        return;
    }
    CONST_ID(id, "to_int");
    if (rb_respond_to(value, id)) {
        tmp = rb_funcall(value, id, 0);
        switch (rb_type(tmp)) {
        case T_FIXNUM:
            fixnum_to_json(node, tmp);
            return;
        case T_BIGNUM:
            bignum_to_json(node, tmp, gc_guard);
            return;
        default:
            break;
        }
    }
    CONST_ID(id, "to_ary");
    if (rb_respond_to(value, id)) {
        tmp = rb_funcall(value, id, 0);
        if (rb_type(tmp) == T_ARRAY) {
            rb_ary_push(gc_guard, tmp);
            array_to_json(node, tmp, gc_guard);
        }
    }
    CONST_ID(id, "to_hash");
    if (rb_respond_to(value, id)) {
        tmp = rb_funcall(value, id, 0);
        if (rb_type(tmp) == T_HASH) {
            rb_ary_push(gc_guard, tmp);
            array_to_json(node, tmp, gc_guard);
        }
    }
    CONST_ID(id, "to_oracle_timestamp");
    if (rb_respond_to(value, id)) {
        tmp = rb_funcall(value, id, 0);
        if (rboradb_is_Timestamp(tmp)) {
            rb_ary_push(gc_guard, tmp);
            timestamp_to_json(node, tmp);
            return;
        }
    }
    CONST_ID(id, "to_oracle_interval_ds");
    if (rb_respond_to(value, id)) {
        tmp = rb_funcall(value, id, 0);
        if (rboradb_is_IntervalDS(tmp)) {
            rb_ary_push(gc_guard, tmp);
            interval_ds_to_json(node, tmp);
            return;
        }
    }
    CONST_ID(id, "to_oracle_interval_ym");
    if (rb_respond_to(value, id)) {
        tmp = rb_funcall(value, id, 0);
        if (rboradb_is_IntervalYM(tmp)) {
            rb_ary_push(gc_guard, tmp);
            interval_ym_to_json(node, tmp);
            return;
        }
    }
    StringValue(value);
    if (!ENCODING_IS_ASCII8BIT(value)) {
        value = rb_str_export_to_enc(value, rb_utf8_encoding());
        node->oracleTypeNum = DPI_ORACLE_TYPE_VARCHAR;
    } else {
        node->oracleTypeNum = DPI_ORACLE_TYPE_RAW;
    }
    node->nativeTypeNum = DPI_NATIVE_TYPE_BYTES;
    node->value->asBytes.ptr = RSTRING_PTR(value);
    node->value->asBytes.length = RSTRING_LEN(value);
    rb_ary_push(gc_guard, value);
}

static void array_to_json(dpiJsonNode *node, VALUE value, VALUE gc_guard)
{
    VALUE tmp;
    long idx, size;

    node->oracleTypeNum = DPI_ORACLE_TYPE_JSON_ARRAY;
    node->nativeTypeNum = DPI_NATIVE_TYPE_JSON_ARRAY;
    size = RARRAY_LEN(value);
    node->value->asJsonArray.numElements = size;
    node->value->asJsonArray.elements = ALLOC_TMP_BUF(dpiJsonNode, tmp, size);
    rb_ary_push(gc_guard, tmp);
    node->value->asJsonArray.elementValues = ALLOC_TMP_BUF(dpiDataBuffer, tmp, size);
    rb_ary_push(gc_guard, tmp);
    for (idx = 0; idx < size; idx++) {
        node->value->asJsonArray.elements[idx].value = &node->value->asJsonArray.elementValues[idx];
        ruby_to_json(&node->value->asJsonArray.elements[idx], RARRAY_AREF(value, idx), gc_guard);
    }
}

static void hash_to_json(dpiJsonNode *node, VALUE value, VALUE gc_guard)
{
    VALUE tmp;
    long size;
    struct hash_to_json_arg hash_to_json_arg;

    node->oracleTypeNum = DPI_ORACLE_TYPE_JSON_OBJECT;
    node->nativeTypeNum = DPI_NATIVE_TYPE_JSON_OBJECT;
    size = RHASH_SIZE(value);
    node->value->asJsonObject.numFields = size;
    node->value->asJsonObject.fieldNames = ALLOC_TMP_BUF(char *, tmp, size);
    rb_ary_push(gc_guard, tmp);
    node->value->asJsonObject.fieldNameLengths = ALLOC_TMP_BUF(uint32_t, tmp, size);
    rb_ary_push(gc_guard, tmp);
    node->value->asJsonObject.fields = ALLOC_TMP_BUF(dpiJsonNode, tmp, size);
    rb_ary_push(gc_guard, tmp);
    node->value->asJsonObject.fieldValues = ALLOC_TMP_BUF(dpiDataBuffer, tmp, size);
    rb_ary_push(gc_guard, tmp);
    hash_to_json_arg.idx = 0;
    hash_to_json_arg.size = size;
    hash_to_json_arg.json_obj = &node->value->asJsonObject;
    hash_to_json_arg.gc_guard = gc_guard;
    rb_hash_foreach(value, hash_to_json_arg_i, (VALUE)&hash_to_json_arg);
}

static int hash_to_json_arg_i(VALUE key, VALUE value, VALUE arg)
{
    struct hash_to_json_arg *args = (struct hash_to_json_arg*)arg;

    if (args->idx == args->size) {
        rb_raise(rb_eRuntimeError, "unexpected out of range while iterating hash");
    }

    ExportString(key);
    rb_ary_push(args->gc_guard, key);
    args->json_obj->fieldNames[args->idx] = RSTRING_PTR(key);
    args->json_obj->fieldNameLengths[args->idx] = RSTRING_LEN(key);
    ruby_to_json(&args->json_obj->fields[args->idx], value, args->gc_guard);
    args->idx++;
    return ST_CONTINUE;
}

static VALUE json_set_value(VALUE self, VALUE value)
{
    Json_t *json = To_Json(self);
    rboradb_ruby2dpiJson(value, json->handle, json->dconn);
    return Qnil;
}

void rboradb_json_init(VALUE mOracleDB)
{
    cJson = rb_define_class_under(mOracleDB, "Json", rb_cObject);
    rb_define_alloc_func(cJson, json_alloc);
    rb_define_private_method(cJson, "initialize", rboradb_notimplement, -1);
    rb_define_private_method(cJson, "initialize_copy", rboradb_notimplement, -1);
    rb_define_method(cJson, "value", json_value, 0);
    rb_define_method(cJson, "value=", json_set_value, 1);
}

VALUE rboradb_dpiJson2ruby(dpiJson *handle, rbOraDBConn *dconn)
{
    dpiJsonNode *top_node;

    if (dpiJson_getValue(handle, DPI_JSON_OPT_NUMBER_AS_STRING, &top_node) != DPI_SUCCESS) {
        rboradb_raise_error(dconn->ctxt);
    }
    return json_to_ruby(top_node);
}

void rboradb_ruby2dpiJson(VALUE obj, dpiJson *handle, rbOraDBConn *dconn)
{
    VALUE gc_guard = rb_ary_new();
    dpiJsonNode top_node;
    dpiDataBuffer buf;
    top_node.value = &buf;

    ruby_to_json(&top_node, obj, gc_guard);
    if (dpiJson_setValue(handle, &top_node) != DPI_SUCCESS) {
        rboradb_raise_error(dconn->ctxt);
    }
    RB_GC_GUARD(gc_guard);
}
