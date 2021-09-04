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

#define To_Object(obj) ((Object_t *)rb_check_typeddata((obj), &object_data_type))
#define To_ObjectType(obj) ((ObjectType_t *)rb_check_typeddata((obj), &object_type_data_type))
#define To_ObjectAttr(obj) ((ObjectAttr_t *)rb_check_typeddata((obj), &object_attr_data_type))

typedef struct {
    RBORADB_COMMON_HEADER(dpiObject);
    dpiObjectType *objtype;
} Object_t;

typedef struct {
    RBORADB_COMMON_HEADER(dpiObjectType);
} ObjectType_t;

typedef struct {
    RBORADB_COMMON_HEADER(dpiObjectAttr);
} ObjectAttr_t;

static VALUE cObject;
static VALUE cObjectType;
static VALUE cObjectAttr;

static void object_free(void *arg)
{
    Object_t *obj = (Object_t *)arg;

    if (obj->objtype) {
        dpiObjectType_release(obj->objtype);
    }
    RBORADB_RELEASE(obj, dpiObject);
    xfree(arg);
}

static const struct rb_data_type_struct object_data_type = {
    "OracleDB::Object",
    {NULL, object_free,},
    NULL, NULL,
};

static void object_type_free(void *arg)
{
    ObjectType_t *objtype = (ObjectType_t *)arg;

    RBORADB_RELEASE(objtype, dpiObjectType);
    xfree(arg);
}

static const struct rb_data_type_struct object_type_data_type = {
    "OracleDB::ObjectType",
    {NULL, object_type_free,},
    NULL, NULL,
};

static void object_attr_free(void *arg)
{
    ObjectAttr_t *objattr = (ObjectAttr_t *)arg;

    RBORADB_RELEASE(objattr, dpiObjectAttr);
    xfree(arg);
}

static const struct rb_data_type_struct object_attr_data_type = {
    "OracleDB::ObjectAttr",
    {NULL, object_attr_free,},
    NULL, NULL,
};

static VALUE set_element_value(dpiData *data, dpiNativeTypeNum *native_type, dpiObjectType *objtype, rbOraDBConn *dconn, VALUE value, _Bool expect_collection)
{
    dpiObjectTypeInfo info;
    dpiOracleTypeNum oracle_type;

    if (dpiObjectType_getInfo(objtype, &info) != DPI_SUCCESS) {
        rboradb_raise_error(dconn->ctxt);
    }
    if (expect_collection) {
        if (!info.isCollection) {
            rb_raise(rb_eArgError, "self isn't a collection.");
        }
    } else {
        if (info.isCollection) {
            rb_raise(rb_eArgError, "self is a collection.");
        }
    }
    if (NIL_P(value)) {
        data->isNull = 1;
        return Qnil;
    }
    oracle_type = info.elementTypeInfo.oracleTypeNum;
    *native_type = info.elementTypeInfo.defaultNativeTypeNum;
    if (oracle_type == DPI_ORACLE_TYPE_NUMBER) {
        *native_type = DPI_NATIVE_TYPE_BYTES;
    }
    return rboradb_set_data(value, data, *native_type, oracle_type, dconn, NULL, 0);
}

static VALUE object_alloc(VALUE klass)
{
    Object_t *obj;
    VALUE val = TypedData_Make_Struct(klass, Object_t, &object_data_type, obj);
    return val;
}

static VALUE object_initialize(VALUE self, VALUE objtype_obj)
{
    Object_t *obj = To_Object(self);
    ObjectType_t *objtype = To_ObjectType(objtype_obj);

    RBORADB_INIT(obj, objtype->dconn);
    if (dpiObjectType_createObject(objtype->handle, &obj->handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(objtype);
    }
    obj->objtype = objtype->handle;
    dpiObjectType_addRef(obj->objtype);
    return Qnil;
}

static VALUE object_initialize_copy(VALUE self, VALUE obj)
{
    Object_t *obj_self = To_Object(self);
    Object_t *obj_obj = To_Object(obj);

    RBORADB_RELEASE(obj_self, dpiObject);
    RBORADB_INIT(obj_self, obj_obj->dconn);
    if (dpiObject_copy(obj_obj->handle, &obj_self->handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(obj_obj);
    }
    obj_self->objtype = obj_obj->objtype;
    return Qnil;
}

static VALUE object_append_element(VALUE self, VALUE value)
{
    Object_t *obj = To_Object(self);
    dpiData data = {1,};
    dpiNativeTypeNum native_type;
    VALUE gc_guard = set_element_value(&data, &native_type, obj->objtype, obj->dconn, value, true);

    if (dpiObject_appendElement(obj->handle, native_type, &data) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(obj);
    }
    RB_GC_GUARD(gc_guard);
    return Qnil;
}

static VALUE object_attribute_value(VALUE self, VALUE attr_type)
{
    Object_t *obj = To_Object(self);
    ObjectAttr_t *attr = To_ObjectAttr(attr_type);
    dpiObjectAttrInfo info;
    dpiOracleTypeNum oracle_type;
    dpiNativeTypeNum native_type;
    dpiObjectType *objtype;
    dpiData value;

    if (dpiObjectAttr_getInfo(attr->handle, &info) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(obj);
    }
    oracle_type = info.typeInfo.oracleTypeNum;
    native_type = info.typeInfo.defaultNativeTypeNum;
    objtype = info.typeInfo.objectType;

    if (oracle_type == DPI_ORACLE_TYPE_NUMBER) {
        native_type = DPI_NATIVE_TYPE_BYTES;
    }
    if (dpiObject_getAttributeValue(obj->handle, attr->handle, native_type, &value) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(obj);
    }
    return rboradb_from_data(&value, native_type, oracle_type, objtype, Qnil, obj->dconn);
}

static VALUE object_delete_element_by_index(VALUE self, VALUE index)
{
    Object_t *obj = To_Object(self);

    if (dpiObject_deleteElementByIndex(obj->handle, NUM2INT(index)) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(obj);
    }
    return Qnil;
}

static VALUE object_element_exists_by_index(VALUE self, VALUE index)
{
    Object_t *obj = To_Object(self);
    int exists;

    if (dpiObject_getElementExistsByIndex(obj->handle, NUM2INT(index), &exists) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(obj);
    }
    return exists ? Qtrue : Qfalse;
}

static VALUE object_element_value_by_index(VALUE self, VALUE index)
{
    Object_t *obj = To_Object(self);
    dpiObjectTypeInfo info;
    dpiOracleTypeNum oracle_type;
    dpiNativeTypeNum native_type;
    dpiObjectType *objtype;
    dpiData value;

    if (dpiObjectType_getInfo(obj->objtype, &info) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(obj);
    }
    if (!info.isCollection) {
        rb_raise(rb_eArgError, "self isn't a collection.");
    }
    oracle_type = info.elementTypeInfo.oracleTypeNum;
    native_type = info.elementTypeInfo.defaultNativeTypeNum;
    objtype = info.elementTypeInfo.objectType;
    if (oracle_type == DPI_ORACLE_TYPE_NUMBER) {
        native_type = DPI_NATIVE_TYPE_BYTES;
    }
    if (dpiObject_getElementValueByIndex(obj->handle, NUM2INT(index), native_type, &value) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(obj);
    }
    return rboradb_from_data(&value, native_type, oracle_type, objtype, Qnil, obj->dconn);
}

static VALUE object_first_index(VALUE self)
{
    Object_t *obj = To_Object(self);
    int32_t index;
    int exists;

    if (dpiObject_getFirstIndex(obj->handle, &index, &exists) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(obj);
    }
    return exists ? INT2NUM(index) : Qnil;
}

static VALUE object_last_index(VALUE self)
{
    Object_t *obj = To_Object(self);
    int32_t index;
    int exists;

    if (dpiObject_getLastIndex(obj->handle, &index, &exists) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(obj);
    }
    return exists ? INT2NUM(index) : Qnil;
}

static VALUE object_next_index(VALUE self, VALUE index)
{
    Object_t *obj = To_Object(self);
    int32_t next_index;
    int exists;

    if (dpiObject_getNextIndex(obj->handle, NUM2INT(index), &next_index, &exists) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(obj);
    }
    return exists ? INT2NUM(next_index) : Qnil;
}

static VALUE object_prev_index(VALUE self, VALUE index)
{
    Object_t *obj = To_Object(self);
    int32_t prev_index;
    int exists;

    if (dpiObject_getPrevIndex(obj->handle, NUM2INT(index), &prev_index, &exists) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(obj);
    }
    return exists ? INT2NUM(prev_index) : Qnil;
}

static VALUE object_set_attribute_value(VALUE self, VALUE attr_type, VALUE value)
{
    Object_t *obj = To_Object(self);
    ObjectAttr_t *attr = To_ObjectAttr(attr_type);
    dpiObjectAttrInfo info;
    dpiData data = {1,};
    dpiNativeTypeNum native_type;
    VALUE gc_guard;

    if (dpiObjectAttr_getInfo(attr->handle, &info) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(attr);
    }
    gc_guard = set_element_value(&data, &native_type, info.typeInfo.objectType, obj->dconn, value, false);
    if (dpiObject_setAttributeValue(obj->handle, attr->handle, native_type, &data) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(obj);
    }
    RB_GC_GUARD(gc_guard);
    return Qnil;
}

static VALUE object_set_element_value_by_index(VALUE self, VALUE index, VALUE value)
{
    Object_t *obj = To_Object(self);
    int32_t idx = NUM2INT(index);
    dpiData data = {1,};
    dpiNativeTypeNum native_type;
    VALUE gc_guard = set_element_value(&data, &native_type, obj->objtype, obj->dconn, value, true);

    if (dpiObject_setElementValueByIndex(obj->handle, idx, native_type, &data) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(obj);
    }
    RB_GC_GUARD(gc_guard);
    return Qnil;
}

static VALUE object_size(VALUE self)
{
    GET_INT32(Object, Size);
}

static VALUE object_trim(VALUE self, VALUE num_to_trim)
{
    Object_t *obj = To_Object(self);

    if (dpiObject_trim(obj->handle, NUM2UINT(num_to_trim)) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(obj);
    }
    return Qnil;
}

static VALUE object_type_alloc(VALUE klass)
{
    ObjectType_t *objtype;
    return TypedData_Make_Struct(klass, ObjectType_t, &object_type_data_type, objtype);
}

static VALUE object_type_initialize(VALUE self, VALUE vconn, VALUE name)
{
    ObjectType_t *objtype = To_ObjectType(self);
    rbOraDBConn *dconn = rboradb_get_dconn(vconn);

    ExportString(name);
    RBORADB_INIT(objtype, dconn);
    if (rbOraDBConn_getObjectType(dconn->handle, RSTRING_PTR(name), RSTRING_LEN(name), &objtype->handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(objtype);
    }
    return Qnil;
}

static VALUE object_type_info(VALUE self)
{
    ObjectType_t *objtype = To_ObjectType(self);
    dpiObjectTypeInfo info;

    if (dpiObjectType_getInfo(objtype->handle, &info) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(objtype);
    }
    return rboradb_from_dpiObjectTypeInfo(&info, objtype->dconn);
}

static VALUE object_type_attributes(VALUE self)
{
    ObjectType_t *objtype = To_ObjectType(self);
    dpiObjectTypeInfo info;
    dpiObjectAttr **attrs;
    uint16_t idx;
    VALUE ary;

    if (dpiObjectType_getInfo(objtype->handle, &info) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(objtype);
    }

    if (info.isCollection) {
        return Qnil;
    }
    attrs = ALLOCA_N(dpiObjectAttr*, info.numAttributes);
    if (dpiObjectType_getAttributes(objtype->handle, info.numAttributes, attrs) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(objtype);
    }
    ary = rb_ary_new_capa(info.numAttributes);
    for (idx = 0; idx < info.numAttributes; idx++) {
        ObjectAttr_t *objattr;
        VALUE obj = TypedData_Make_Struct(cObjectAttr, ObjectAttr_t, &object_attr_data_type, objattr);

        RBORADB_SET(objattr, dpiObjectAttr, attrs[idx], objtype->dconn);
        rb_ary_push(ary, obj);
    }
    return ary;
}

static VALUE object_attr_alloc(VALUE klass)
{
    ObjectAttr_t *objattr;
    return TypedData_Make_Struct(klass, ObjectAttr_t, &object_attr_data_type, objattr);
}

static VALUE object_attr_info(VALUE self)
{
    ObjectAttr_t *objattr = To_ObjectAttr(self);
    dpiObjectAttrInfo info;

    if (dpiObjectAttr_getInfo(objattr->handle, &info) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(objattr);
    }
    return rboradb_from_dpiObjectAttrInfo(&info, objattr->dconn);
}

void rboradb_object_init(VALUE mOracleDB)
{
    cObject = rb_define_class_under(mOracleDB, "Object", rb_cObject);
    rb_define_alloc_func(cObject, object_alloc);
    rb_define_method(cObject, "initialize", object_initialize, 1);
    rb_define_private_method(cObject, "initialize_copy", object_initialize_copy, 1);
    rb_define_method(cObject, "append_element", object_append_element, 1);
    rb_define_method(cObject, "attribute_value", object_attribute_value, 1);
    rb_define_method(cObject, "delete_element_by_index", object_delete_element_by_index, 1);
    rb_define_method(cObject, "element_exists_by_index", object_element_exists_by_index, 1);
    rb_define_method(cObject, "element_value_by_index", object_element_value_by_index, 1);
    rb_define_method(cObject, "first_index", object_first_index, 0);
    rb_define_method(cObject, "last_index", object_last_index, 0);
    rb_define_method(cObject, "next_index", object_next_index, 1);
    rb_define_method(cObject, "prev_index", object_prev_index, 1);
    rb_define_method(cObject, "set_attribute_value", object_set_attribute_value, 2);
    rb_define_method(cObject, "set_element_value_by_index", object_set_element_value_by_index, 2);
    rb_define_method(cObject, "size", object_size, 0);
    rb_define_method(cObject, "trim", object_trim, 1);

    cObjectType = rb_define_class_under(mOracleDB, "ObjectType", rb_cObject);
    rb_define_alloc_func(cObjectType, object_type_alloc);
    rb_define_method(cObjectType, "initialize", object_type_initialize, 2);
    rb_define_private_method(cObjectType, "initialize_copy", rboradb_notimplement, -1);
    rb_define_private_method(cObjectType, "__info", object_type_info, 0);
    rb_define_private_method(cObjectType, "__attributes", object_type_attributes, 0);

    cObjectAttr = rb_define_class_under(mOracleDB, "ObjectAttr", rb_cObject);
    rb_define_alloc_func(cObjectAttr, object_attr_alloc);
    rb_define_method(cObjectAttr, "initialize", rboradb_notimplement, -1);
    rb_define_private_method(cObjectAttr, "initialize_copy", rboradb_notimplement, -1);
    rb_define_private_method(cObjectAttr, "__info", object_attr_info, 0);
}

VALUE rboradb_from_dpiObject(dpiObject *handle, dpiObjectType *objtype, rbOraDBConn *dconn, int ref)
{
    Object_t *obj;
    VALUE val = TypedData_Make_Struct(cObject, Object_t, &object_data_type, obj);

    if (ref) {
        RBORADB_SET(obj, dpiObject, handle, dconn);
    } else {
        RBORADB_INIT(obj, dconn);
        obj->handle = handle;
    }
    obj->objtype = objtype;
    if (obj->objtype) {
        dpiObjectType_addRef(obj->objtype);
    }
    return val;
}

VALUE rboradb_from_dpiObjectType(dpiObjectType *handle, rbOraDBConn *dconn, int ref)
{
    ObjectType_t *objtype;
    VALUE val = TypedData_Make_Struct(cObjectType, ObjectType_t, &object_type_data_type, objtype);

    if (ref) {
        RBORADB_SET(objtype, dpiObjectType, handle, dconn);
    } else {
        RBORADB_INIT(objtype, dconn);
        objtype->handle = handle;
    }
    return val;
}

dpiObjectType *rboradb_get_ObjectType_or_null(VALUE obj)
{
    if (rb_typeddata_is_kind_of(obj, &object_data_type)) {
        return To_ObjectType(obj)->handle;
    } else {
        return NULL;
    }
}

dpiObject *rboradb_to_dpiObject(VALUE obj)
{
    return To_Object(obj)->handle;
}

dpiObjectType *rboradb_to_dpiObjectType(VALUE obj)
{
    return To_ObjectType(obj)->handle;
}
