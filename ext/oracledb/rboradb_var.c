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

#define To_Var(obj) ((Var_t *)rb_check_typeddata((obj), &var_data_type))

typedef struct {
    RBORADB_COMMON_HEADER(dpiVar);
    dpiData *data;
    uint32_t array_size;
    dpiNativeTypeNum native_type_num;
    dpiOracleTypeNum oracle_type_num;
    dpiObjectType *objtype;
    VALUE out_filter;
    VALUE in_filter;
} Var_t;

static VALUE cVar;
static VALUE sym_to_i;
static VALUE sym_to_f;

static void var_mark(void *arg)
{
    Var_t *obj = (Var_t *)arg;
    rb_gc_mark(obj->out_filter);
    rb_gc_mark(obj->in_filter);
}

static void var_free(void *arg)
{
    Var_t *var = (Var_t *)arg;
    if (var->objtype) {
        dpiObjectType_release(var->objtype);
    }
    RBORADB_RELEASE(var, dpiVar);
    xfree(arg);
}

static const struct rb_data_type_struct var_data_type = {
    "OracleDB::Var",
    {var_mark, var_free,},
    NULL, NULL,
};

static VALUE var_alloc(VALUE klass)
{
    Var_t *coll;
    return TypedData_Make_Struct(klass, Var_t, &var_data_type, coll);
}

static VALUE to_proc(VALUE proc, const char *name)
{
    if (NIL_P(proc) || rb_obj_is_proc(proc)) {
        return proc;
    }
    if (RB_SYMBOL_P(proc)) {
        static ID id;
        CONST_ID(id, "to_proc");
        return rb_funcall(proc, id, 0);
    }
    rb_raise(rb_eArgError, "wrong %s type (given %s, expected nil, symbol, proc or lambda)", name, rb_obj_classname(proc));
}

static VALUE var_initialize(VALUE self, VALUE conn, VALUE oracle_type, VALUE native_type, VALUE max_array_size, VALUE size, VALUE size_is_bytes, VALUE is_array, VALUE objtype, VALUE out_filter, VALUE in_filter)
{
    Var_t *var = To_Var(self);
    rbOraDBConn *dconn = rboradb_get_dconn(conn);
    dpiOracleTypeNum oracle_type_num = rboradb_to_dpiOracleTypeNum(oracle_type);
    dpiNativeTypeNum native_type_num = rboradb_to_dpiNativeTypeNum(native_type);
    dpiObjectType *dpiobjtype = NIL_P(objtype) ? NULL : rboradb_to_dpiObjectType(objtype);
    uint32_t array_size = NUM2UINT(max_array_size);

    RBORADB_INIT(var, dconn);
    if (dpiConn_newVar(dconn->handle, oracle_type_num, native_type_num, array_size, NIL_P(size) ? 0 : NUM2UINT(size), RTEST(size_is_bytes),
        RTEST(is_array), dpiobjtype, &var->handle, &var->data) != DPI_SUCCESS) {
        rboradb_raise_error(dconn->ctxt);
    }
    var->array_size = array_size;
    var->native_type_num = native_type_num;
    var->oracle_type_num = oracle_type_num;
    var->objtype = dpiobjtype;
    if (var->objtype) {
        dpiObjectType_addRef(var->objtype);
    }
    if (native_type_num == DPI_NATIVE_TYPE_BYTES && (out_filter == sym_to_i || out_filter == sym_to_f)) {
        var->out_filter = out_filter;
    } else {
        var->out_filter = to_proc(out_filter, "out_filter");
    }
    var->in_filter = to_proc(in_filter, "in_filter");
    return Qnil;
}

static VALUE var_get(VALUE self, VALUE index)
{
    Var_t *var = To_Var(self);
    uint32_t idx = NUM2UINT(index);

    if (idx >= var->array_size) {
        rb_raise(rb_eArgError, "wrong row index (given %u, expected between 0 and %u)",
            idx, var->array_size - 1);
    }
    return rboradb_from_data(var->data + idx, var->native_type_num, var->oracle_type_num, var->objtype, var->out_filter, var->dconn);
}

static VALUE var_returned_data(VALUE self, VALUE pos)
{
    Var_t *var = To_Var(self);
    uint32_t idx, num;
    dpiData *data;
    VALUE ary;

    if (dpiVar_getReturnedData(var->handle, NUM2UINT(pos), &num, &data) != DPI_SUCCESS) {
        rboradb_raise_error(var->dconn->ctxt);
    }

    ary = rb_ary_new_capa(num);
    for (idx = 0; idx < num; idx++) {
        VALUE obj = rboradb_from_data(data + idx, var->native_type_num, var->oracle_type_num, var->objtype, var->out_filter, var->dconn);
        rb_ary_push(ary, obj);
    }
    return ary;
}

static VALUE var_set(VALUE self, VALUE index, VALUE obj)
{
    Var_t *var = To_Var(self);
    uint32_t pos = NUM2UINT(index);
    dpiData *data = var->data + pos;

    if (pos >= var->array_size) {
        rb_raise(rb_eArgError, "wrong row index (given %u, expected between 0 and %u)",
            pos, var->array_size - 1);
    }

    if (NIL_P(obj)) {
        data->isNull = 1;
        return Qnil;
    }

    if (!NIL_P(var->in_filter)) {
        obj = rb_proc_call_with_block(var->in_filter, 1, &obj, Qnil);
    }
    rboradb_set_data(obj, data, var->native_type_num, var->oracle_type_num, var->dconn, var->handle, pos);
    return Qnil;
}

static VALUE var_copy_data(VALUE self, VALUE pos, VALUE source_var, VALUE source_pos)
{
    Var_t *var = To_Var(self);

    if (dpiVar_copyData(var->handle, NUM2UINT(pos), To_Var(source_var)->handle, NUM2UINT(source_pos)) != DPI_SUCCESS) {
        rboradb_raise_error(var->dconn->ctxt);
    }
    return Qnil;
}

static VALUE var_num_elements_in_array(VALUE self)
{
    GET_UINT32(Var, NumElementsInArray);
}

static VALUE var_set_num_elements_in_array(VALUE self, VALUE obj)
{
    SET_UINT32(Var, NumElementsInArray);
}

static VALUE var_size_in_bytes(VALUE self)
{
    GET_UINT32(Var, SizeInBytes);
}

void rboradb_var_init(VALUE mOracleDB)
{
    sym_to_i = ID2SYM(rb_intern("to_i"));
    sym_to_f = ID2SYM(rb_intern("to_f"));

    cVar = rb_define_class_under(mOracleDB, "Var", rb_cObject);
    rb_define_alloc_func(cVar, var_alloc);
    rb_define_private_method(cVar, "__initialize", var_initialize, 10);
    rb_define_private_method(cVar, "initialize_copy", rboradb_notimplement, -1);
    rb_define_method(cVar, "get", var_get, 1);
    rb_define_method(cVar, "returned_data", var_returned_data, 1);
    rb_define_method(cVar, "set", var_set, 2);
    rb_define_method(cVar, "copy_data", var_copy_data, 3);
    rb_define_method(cVar, "num_elements_in_array", var_num_elements_in_array, 0);
    rb_define_method(cVar, "num_elements_in_array=", var_set_num_elements_in_array, 1);
    rb_define_method(cVar, "size_in_bytes", var_size_in_bytes, 0);
}

dpiVar *rboradb_to_dpiVar(VALUE obj)
{
    return To_Var(obj)->handle;
}
