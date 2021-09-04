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

#define To_Lob(obj) ((Lob_t *)rb_check_typeddata((obj), &lob_data_type))

#define CHAR_TYPE(type) ((type) == DPI_ORACLE_TYPE_CLOB || (type == DPI_ORACLE_TYPE_NCLOB))

static ID id_at_pos;
static VALUE cLob;

typedef struct {
    RBORADB_COMMON_HEADER(dpiLob);
    dpiOracleTypeNum type;
} Lob_t;

static VALUE lob_alloc(VALUE klass);
static void lob_free(void *arg);

static size_t get_size_in_chars(VALUE s)
{
    const uint8_t *ptr = (const uint8_t *)RSTRING_PTR(s);
    const uint8_t *end = (const uint8_t *)RSTRING_END(s);
    size_t size = 0;

    while (ptr < end) {
        if (*ptr < 0x80) {
            size++;
            ptr++;
        } else if (*ptr < 0xC2) {
            rb_raise(rb_eEncodingError, "Invalid UTF-8 encoding");
        } else if (*ptr < 0xE0) {
            size++;
            ptr += 2;
        } else if (*ptr < 0xF0) {
            size++;
            ptr += 3;
        } else if (*ptr < 0xF5) {
            // CLOBs store their data in a form of UTF-16 encoding but the
            // length calculations are based on UCS-2 code points.
            size += 2;
            ptr += 4;
        } else {
            rb_raise(rb_eEncodingError, "Invalid UTF-8 encoding");
        }
    }
    RB_GC_GUARD(s);
    return size;
}

static const struct rb_data_type_struct lob_data_type = {
    "OracleDB::Lob",
    {NULL, lob_free,},
    NULL, NULL,
};

static VALUE lob_alloc(VALUE klass)
{
    Lob_t *lob;
    return TypedData_Make_Struct(klass, Lob_t, &lob_data_type, lob);
}

static void lob_free(void *arg)
{
    Lob_t *lob = (Lob_t *)arg;
    RBORADB_RELEASE(lob, dpiLob);
    xfree(arg);
}

static VALUE lob___initialize(VALUE self, VALUE conn, VALUE type)
{
    Lob_t *lob = To_Lob(self);
    rbOraDBConn *dconn = rboradb_get_dconn_in_conn(conn);
    dpiOracleTypeNum lobtype = rboradb_from_dpiOracleTypeNum(type);

    if (rbOraDBConn_newTempLob(dconn->handle, lobtype, &lob->handle) != DPI_SUCCESS) {
        rboradb_raise_error(dconn->ctxt);
    }
    RBORADB_INIT(lob, dconn);
    lob->type = lobtype;
    rb_ivar_set(self, id_at_pos, INT2FIX(0));
    return Qnil;
}

static VALUE lob_initialize_copy(VALUE self, VALUE obj)
{
    Lob_t *lob = To_Lob(self);
    Lob_t *lob_obj = To_Lob(obj);

    RBORADB_INIT(lob, lob_obj->dconn);
    if (dpiLob_copy(lob_obj->handle, &lob->handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(lob);
    }
    if (dpiLob_getType(lob->handle, &lob->type) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(lob);
    }
    return Qnil;
}

static VALUE lob_close(VALUE self)
{
    Lob_t *lob = To_Lob(self);

    if (dpiLob_close(lob->handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(lob);
    }
    return Qnil;
}

static VALUE lob_close_resource(VALUE self)
{
    Lob_t *lob = To_Lob(self);

    if (dpiLob_closeResource(lob->handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(lob);
    }
    return Qnil;
}

static VALUE lob_chunk_size(VALUE self)
{
    GET_UINT32(Lob, ChunkSize);
}

static VALUE lob_directory_and_file_name(VALUE self)
{
    Lob_t *lob = To_Lob(self);
    const char *dir, *file;
    uint32_t dlen, flen;

    if (dpiLob_getDirectoryAndFileName(lob->handle, &dir, &dlen, &file, &flen) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(lob);
    }
    return rb_ary_new_from_args(2, rb_enc_str_new(dir, dlen, rb_utf8_encoding()), rb_enc_str_new(file, flen, rb_utf8_encoding()));
}

static VALUE lob_file_exists(VALUE self)
{
    GET_BOOL(Lob, FileExists);
}

static VALUE lob_is_resource_open(VALUE self)
{
    GET_BOOL(Lob, IsResourceOpen);
}

static VALUE lob_size(VALUE self)
{
    GET_UINT64(Lob, Size);
}

static VALUE lob_type(VALUE self)
{
    GET_ENUM(Lob, Type, dpiOracleTypeNum);
}

static VALUE lob_open_resource(VALUE self)
{
    Lob_t *lob = To_Lob(self);

    if (dpiLob_openResource(lob->handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(lob);
    }
    return Qnil;
}

static VALUE lob___read_bytes(VALUE self, VALUE offset, VALUE amount)
{
    Lob_t *lob = To_Lob(self);
    uint64_t off = NUM2ULL(offset);
    uint64_t char_size = NUM2ULL(amount);
    uint64_t byte_size;
    size_t size;
    VALUE str;

    if (dpiLob_getBufferSize(lob->handle, char_size, &byte_size) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(lob);
    }
    if (byte_size > (uint64_t)LONG_MAX) {
        rb_raise(rb_eArgError, "size too big");
    }
    str = rb_str_buf_new(byte_size);
    if (dpiLob_readBytes(lob->handle, off, char_size, RSTRING_PTR(str), &byte_size) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(lob);
    }
    rb_str_set_len(str, byte_size);
    if (CHAR_TYPE(lob->type)) {
        rb_enc_associate(str, rb_utf8_encoding());
        size = get_size_in_chars(str);
    } else {
        size = RSTRING_LEN(str);
    }
    return rb_ary_new_from_args(2, str, SIZET2NUM(size));
}

static VALUE lob_set_directory_and_file_name(VALUE self, VALUE directory_alias, VALUE file_name)
{
    Lob_t *lob = To_Lob(self);

    ExportString(directory_alias);
    ExportString(file_name);
    if (dpiLob_setDirectoryAndFileName(lob->handle, RSTRING_PTR(directory_alias), RSTRING_LEN(directory_alias), RSTRING_PTR(file_name), RSTRING_LEN(file_name)) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(lob);
    }
    return Qnil;
}

static VALUE lob_trim(VALUE self, VALUE new_size)
{
    Lob_t *lob = To_Lob(self);

    if (dpiLob_trim(lob->handle, NUM2ULL(new_size)) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(lob);
    }
    return Qnil;
}

static VALUE lob___set_from_bytes(VALUE self, VALUE value)
{
    Lob_t *lob = To_Lob(self);
    size_t size;

    SafeStringValue(value);
    if (CHAR_TYPE(lob->type)) {
        value = rb_str_export_to_enc(value, rb_utf8_encoding());
        size = get_size_in_chars(value);
    } else {
        size = RSTRING_LEN(value);
    }
    if (dpiLob_setFromBytes(lob->handle, RSTRING_PTR(value), RSTRING_LEN(value)) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(lob);
    }
    return SIZET2NUM(size);
}

static VALUE lob___write_bytes(VALUE self, VALUE offset, VALUE value)
{
    Lob_t *lob = To_Lob(self);
    uint64_t off = NUM2ULL(offset);
    size_t size;

    SafeStringValue(value);
    if (CHAR_TYPE(lob->type)) {
        value = rb_str_export_to_enc(value, rb_utf8_encoding());
        size = get_size_in_chars(value);
    } else {
        size = RSTRING_LEN(value);
    }
    if (dpiLob_writeBytes(lob->handle, off, RSTRING_PTR(value), RSTRING_LEN(value)) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(lob);
    }
    return SIZET2NUM(size);
}

void rboradb_lob_init(VALUE mOracleDB)
{
    id_at_pos = rb_intern("@pos");

    cLob = rb_define_class_under(mOracleDB, "Lob", rb_cObject);
    rb_define_alloc_func(cLob, lob_alloc);
    rb_define_private_method(cLob, "__initialize", lob___initialize, 2);
    rb_define_private_method(cLob, "initialize_copy", lob_initialize_copy, 1);
    rb_define_method(cLob, "close", lob_close, 0);
    rb_define_method(cLob, "close_resource", lob_close_resource, 0);
    rb_define_method(cLob, "chunk_size", lob_chunk_size, 0);
    rb_define_method(cLob, "directory_and_file_name", lob_directory_and_file_name, 0);
    rb_define_method(cLob, "file_exists", lob_file_exists, 0);
    rb_define_method(cLob, "is_resource_open", lob_is_resource_open, 0);
    rb_define_method(cLob, "size", lob_size, 0);
    rb_define_method(cLob, "type", lob_type, 0);
    rb_define_method(cLob, "open_resource", lob_open_resource, 0);
    rb_define_private_method(cLob, "__read_bytes", lob___read_bytes, 2);
    rb_define_method(cLob, "set_directory_and_file_name", lob_set_directory_and_file_name, 2);
    rb_define_method(cLob, "trim", lob_trim, 1);
    rb_define_private_method(cLob, "__set_from_bytes", lob___set_from_bytes, 1);
    rb_define_private_method(cLob, "__write_bytes", lob___write_bytes, 2);
}

VALUE rboradb_from_dpiLob(dpiLob *handle, rbOraDBConn *dconn, int ref)
{
    Lob_t *lob;
    VALUE obj = TypedData_Make_Struct(cLob, Lob_t, &lob_data_type, lob);

    if (ref) {
        RBORADB_SET(lob, dpiLob, handle, dconn);
    } else {
        RBORADB_INIT(lob, dconn);
        lob->handle = handle;
    }
    if (dpiLob_getType(lob->handle, &lob->type) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(lob);
    }
    rb_ivar_set(obj, id_at_pos, INT2FIX(0));
    return obj;
}

dpiLob *rboradb_to_dpiLob(VALUE obj)
{
    return To_Lob(obj)->handle;
}
