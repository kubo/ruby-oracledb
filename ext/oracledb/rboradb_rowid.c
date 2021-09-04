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

#define To_Rowid(obj) ((Rowid_t *)rb_check_typeddata((obj), &rowid_data_type))

typedef struct {
    dpiRowid *handle;
    rbOraDBContext *ctxt;
} Rowid_t;

static VALUE cRowid;

static VALUE rowid_alloc(VALUE klass);
static void rowid_free(void *arg);
static VALUE rowid_to_s(VALUE self);

static const struct rb_data_type_struct rowid_data_type = {
    "OracleDB::Rowid",
    {NULL, rowid_free,},
    NULL, NULL,
};

static VALUE rowid_alloc(VALUE klass)
{
    Rowid_t *rid;
    return TypedData_Make_Struct(klass, Rowid_t, &rowid_data_type, rid);
}

static void rowid_free(void *arg)
{
    Rowid_t *rid = (Rowid_t *)arg;
    if (rid) {
        if (rid->handle) {
            dpiRowid_release(rid->handle);
        }
        if (rid->ctxt) {
            rbOraDBContext_release(rid->ctxt);
        }
    }
    xfree(arg);
}

static VALUE rowid_to_s(VALUE self)
{
    Rowid_t *rid = To_Rowid(self);
    const char *val;
    uint32_t len;

    if (rid == NULL || rid->handle == NULL) {
        rb_raise(rb_eRuntimeError, "uinitialized %s", rb_obj_classname(self));
    }
    if (dpiRowid_getStringValue(rid->handle, &val, &len) != DPI_SUCCESS) {
        rboradb_raise_error(rid->ctxt);
    }
    return rb_str_new(val, len);
}

void rboradb_rowid_init(VALUE mOracleDB)
{
    cRowid = rb_define_class_under(mOracleDB, "Rowid", rb_cObject);
    rb_define_alloc_func(cRowid, rowid_alloc);
    rb_define_method(cRowid, "initialize", rboradb_notimplement, -1);
    rb_define_private_method(cRowid, "initialize_copy", rboradb_notimplement, -1);
    rb_define_method(cRowid, "to_s", rowid_to_s, 0);
}

VALUE rboradb_from_dpiRowid(dpiRowid *handle, rbOraDBConn *dconn, int ref)
{
    Rowid_t *rid;
    VALUE obj = TypedData_Make_Struct(cRowid, Rowid_t, &rowid_data_type, rid);

    rid->handle = handle;
    if (ref) {
        dpiRowid_addRef(rid->handle);
    }
    rid->ctxt = dconn->ctxt;
    rbOraDBContext_addRef(rid->ctxt);
    return obj;
}

dpiRowid *rboradb_to_dpiRowid(VALUE obj)
{
    return To_Rowid(obj)->handle;
}
