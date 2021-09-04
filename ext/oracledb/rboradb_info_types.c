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

static VALUE cDataTypeInfo;
static VALUE cObjectAttrInfo;
static VALUE cObjectTypeInfo;
static VALUE cQueryInfo;
static VALUE cStmtInfo;
static VALUE cVersionInfo;

void rboradb_info_types_init(VALUE mOracleDB)
{
    cDataTypeInfo = rb_define_class_under(mOracleDB, "DataTypeInfo", rb_cObject);
    cObjectAttrInfo = rb_define_class_under(mOracleDB, "ObjectAttrInfo", rb_cObject);
    cObjectTypeInfo = rb_define_class_under(mOracleDB, "ObjectTypeInfo", rb_cObject);
    cQueryInfo = rb_define_class_under(mOracleDB, "QueryInfo", rb_cObject);
    cStmtInfo = rb_define_class_under(mOracleDB, "StmtInfo", rb_cObject);
    cVersionInfo = rb_define_class_under(mOracleDB, "VersionInfo", rb_cObject);
}

VALUE rboradb_from_dpiDataTypeInfo(const dpiDataTypeInfo *info, rbOraDBConn *dconn)
{
    VALUE obj = rb_obj_alloc(cDataTypeInfo);
    IVAR_SET(obj, "@oracle_type", rboradb_from_dpiOracleTypeNum(info->oracleTypeNum));
    IVAR_SET(obj, "@default_native_type", rboradb_from_dpiNativeTypeNum(info->defaultNativeTypeNum));
    IVAR_SET(obj, "@oci_type_code", INT2FIX(info->ociTypeCode));
    IVAR_SET(obj, "@db_size_in_bytes", UINT2NUM(info->dbSizeInBytes));
    IVAR_SET(obj, "@client_size_in_bytes", UINT2NUM(info->clientSizeInBytes));
    IVAR_SET(obj, "@size_in_chars", UINT2NUM(info->sizeInChars));
    IVAR_SET(obj, "@precision", INT2FIX(info->precision));
    IVAR_SET(obj, "@scale", INT2FIX(info->scale));
    IVAR_SET(obj, "@fs_precision", INT2FIX(info->fsPrecision));
    IVAR_SET(obj, "@object_type", info->objectType ? rboradb_from_dpiObjectType(info->objectType, dconn, 1) : Qnil);
    return obj;
}

VALUE rboradb_from_dpiObjectAttrInfo(const dpiObjectAttrInfo *info, rbOraDBConn *dconn)
{
    VALUE obj = rb_obj_alloc(cObjectAttrInfo);
    IVAR_SET(obj, "@name", rb_enc_str_new(info->name, info->nameLength, rb_utf8_encoding()));
    IVAR_SET(obj, "@type_info", rboradb_from_dpiDataTypeInfo(&info->typeInfo, dconn));
    return obj;
}

VALUE rboradb_from_dpiObjectTypeInfo(const dpiObjectTypeInfo *info, rbOraDBConn *dconn)
{
    VALUE obj = rb_obj_alloc(cObjectTypeInfo);
    IVAR_SET(obj, "@schema", rb_enc_str_new(info->schema, info->schemaLength, rb_utf8_encoding()));
    IVAR_SET(obj, "@name", rb_enc_str_new(info->name, info->nameLength, rb_utf8_encoding()));
    IVAR_SET(obj, "@is_collection", info->isCollection ? Qtrue : Qfalse);
    IVAR_SET(obj, "@element_type_info", info->isCollection ? rboradb_from_dpiDataTypeInfo(&info->elementTypeInfo, dconn) : Qnil);
    IVAR_SET(obj, "@num_attributes", INT2FIX(info->numAttributes));
    return obj;
}

VALUE rboradb_from_dpiQueryInfo(const dpiQueryInfo *info, rbOraDBConn *dconn)
{
    VALUE obj = rb_obj_alloc(cQueryInfo);
    IVAR_SET(obj, "@name", rb_enc_str_new(info->name, info->nameLength, rb_utf8_encoding()));
    IVAR_SET(obj, "@type_info", rboradb_from_dpiDataTypeInfo(&info->typeInfo, dconn));
    IVAR_SET(obj, "@null_ok", info->nullOk ? Qtrue : Qfalse);
    return obj;
}

VALUE rboradb_from_dpiStmtInfo(const dpiStmtInfo *info)
{
    VALUE obj = rb_obj_alloc(cStmtInfo);
    IVAR_SET(obj, "@is_query", info->isQuery ? Qtrue : Qfalse);
    IVAR_SET(obj, "@is_plsql", info->isPLSQL ? Qtrue : Qfalse);
    IVAR_SET(obj, "@is_ddl", info->isDDL ? Qtrue : Qfalse);
    IVAR_SET(obj, "@is_dml", info->isDML ? Qtrue : Qfalse);
    IVAR_SET(obj, "@statement_type", rboradb_from_dpiStatementType(info->statementType));
    IVAR_SET(obj, "@is_returning", info->isReturning ? Qtrue : Qfalse);
    return obj;
}

VALUE rboradb_from_dpiVersionInfo(const dpiVersionInfo *info)
{
    VALUE obj = rb_obj_alloc(cVersionInfo);
    IVAR_SET(obj, "@version", INT2NUM(info->versionNum));
    IVAR_SET(obj, "@release", INT2NUM(info->releaseNum));
    IVAR_SET(obj, "@update", INT2NUM(info->updateNum));
    IVAR_SET(obj, "@port_release", INT2NUM(info->portReleaseNum));
    IVAR_SET(obj, "@port_update", INT2NUM(info->portUpdateNum));
    return obj;
}
