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

#define To_SodaColl(obj) ((SodaColl_t *)rb_check_typeddata((obj), &soda_coll_data_type))
#define To_SodaDb(obj) ((SodaDb_t *)rb_check_typeddata((obj), &soda_db_data_type))
#define To_SodaDoc(obj) ((SodaDoc_t *)rb_check_typeddata((obj), &soda_doc_data_type))

typedef struct {
    RBORADB_COMMON_HEADER(dpiSodaColl);
} SodaColl_t;

typedef struct {
    RBORADB_COMMON_HEADER(dpiSodaDb);
} SodaDb_t;

typedef struct {
    RBORADB_COMMON_HEADER(dpiSodaDoc);
} SodaDoc_t;

typedef struct {
    RBORADB_COMMON_HEADER(dpiSodaDocCursor);
    VALUE proc;
} SodaDocCursor_t;

typedef struct {
    RBORADB_COMMON_HEADER(dpiSodaCollCursor);
    VALUE proc;
} SodaCollCursor_t;

static VALUE cColl;
static VALUE cDb;
static VALUE cDoc;

static VALUE soda_coll_alloc(VALUE klass);
static VALUE soda_coll_new(dpiSodaColl *handle, rbOraDBConn *dconn);
static void soda_coll_free(void *arg);

static VALUE soda_db_alloc(VALUE klass);
static VALUE soda_db_new(dpiSodaDb *handle, rbOraDBConn *dconn);
static void soda_db_free(void *arg);

static VALUE soda_doc_alloc(VALUE klass);
static VALUE soda_doc_new(dpiSodaDoc *handle, rbOraDBConn *dconn);
static void soda_doc_free(void *arg);

static VALUE init_oper_options(dpiSodaOperOptions *dpi_opts, VALUE opts, rbOraDBConn *dconn)
{
    if (dpiContext_initSodaOperOptions(dconn->ctxt->handle, dpi_opts) != DPI_SUCCESS) {
        rboradb_raise_error(dconn->ctxt);
    }
    return rboradb_set_dpiSodaOperOptions(dpi_opts, opts);
}

static const struct rb_data_type_struct soda_coll_data_type = {
    "OracleDB::Soda::Coll",
    {NULL, soda_coll_free,},
    NULL, NULL,
};

static const struct rb_data_type_struct soda_db_data_type = {
    "OracleDB::Soda::Db",
    {NULL, soda_db_free,},
    NULL, NULL,
};

static const struct rb_data_type_struct soda_doc_data_type = {
    "OracleDB::Soda::Doc",
    {NULL, soda_doc_free,},
    NULL, NULL,
};

static VALUE soda_coll_alloc(VALUE klass)
{
    SodaColl_t *coll;
    return TypedData_Make_Struct(klass, SodaColl_t, &soda_coll_data_type, coll);
}

static VALUE soda_coll_new(dpiSodaColl *handle, rbOraDBConn *dconn)
{
    SodaColl_t *coll;
    VALUE obj = TypedData_Make_Struct(cColl, SodaColl_t, &soda_coll_data_type, coll);

    RBORADB_INIT(coll, dconn);
    coll->handle = handle;
    return obj;
}

static void soda_coll_free(void *arg)
{
    SodaColl_t *coll = (SodaColl_t *)arg;
    RBORADB_RELEASE(coll, dpiSodaColl);
    xfree(arg);
}

static VALUE soda_coll_create_index(VALUE self, VALUE index_spec)
{
    SodaColl_t *coll = To_SodaColl(self);

    ExportString(index_spec);
    if (dpiSodaColl_createIndex(coll->handle, RSTRING_PTR(index_spec), RSTRING_LEN(index_spec), 0) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(coll);
    }
    return Qnil;
}

static VALUE soda_coll_drop(VALUE self)
{
    SodaColl_t *coll = To_SodaColl(self);
    int is_dropped;

    if (dpiSodaColl_drop(coll->handle, 0, &is_dropped) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(coll);
    }
    return is_dropped ? Qtrue : Qfalse;
}

static VALUE soda_coll_drop_index(int argc, VALUE *argv, VALUE self)
{
    SodaColl_t *coll = To_SodaColl(self);
    VALUE name, kwopts, force;
    int is_dropped;
    uint32_t flags;
    static ID keywords[1];

    if (!keywords[0]) {
        keywords[0] = rb_intern_const("force");
    }
    rb_scan_args(argc, argv, "10:", &name, &kwopts);
    rb_get_kwargs(kwopts, keywords, 0, 1, &force);

    ExportString(name);
    flags = ((force != Qundef) && RTEST(force)) ? DPI_SODA_FLAGS_INDEX_DROP_FORCE : 0;
    if (dpiSodaColl_dropIndex(coll->handle, RSTRING_PTR(name), RSTRING_LEN(name), flags, &is_dropped) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(coll);
    }
    return is_dropped ? Qtrue : Qfalse;
}

static VALUE iter_doc_cursor(VALUE arg)
{
    SodaDocCursor_t *cursor = (SodaDocCursor_t *)arg;
    while (1) {
        dpiSodaDoc *handle;
        VALUE obj;

        if (dpiSodaDocCursor_getNext(cursor->handle, 0, &handle) != DPI_SUCCESS) {
            RBORADB_RAISE_ERROR(cursor);
        }
        if (handle == NULL) {
            break;
        }
        obj = soda_doc_new(handle, cursor->dconn);
        rb_proc_call_with_block(cursor->proc, 1, &obj, Qnil);
    }
    return Qnil;
}

static VALUE soda_coll_find(int argc, VALUE *argv, VALUE self)
{
    SodaColl_t *coll = To_SodaColl(self);
    VALUE kwopts;
    dpiSodaOperOptions dpi_opts;
    SodaDocCursor_t cursor;
    VALUE gc_guard;
    int state;

    RETURN_ENUMERATOR(self, 0, 0);
    rb_scan_args(argc, argv, "00:", &kwopts);

    gc_guard = init_oper_options(&dpi_opts, kwopts, coll->dconn);
    if (dpiSodaColl_find(coll->handle, &dpi_opts, 0, &cursor.handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(coll);
    }
    RB_GC_GUARD(gc_guard);
    cursor.dconn = coll->dconn;
    cursor.proc = rb_block_proc();
    rb_protect(iter_doc_cursor, (VALUE)&cursor, &state);
    RB_GC_GUARD(cursor.proc);
    dpiSodaDocCursor_release(cursor.handle);
    if (state) {
        rb_jump_tag(state);
    }
    return Qnil;
}

static VALUE soda_coll_find_one(int argc, VALUE *argv, VALUE self)
{
    SodaColl_t *coll = To_SodaColl(self);
    VALUE kwopts;
    dpiSodaOperOptions dpi_opts;
    VALUE gc_guard;
    dpiSodaDoc *handle;

    rb_scan_args(argc, argv, "00:", &kwopts);

    gc_guard = init_oper_options(&dpi_opts, kwopts, coll->dconn);
    if (dpiSodaColl_findOne(coll->handle, &dpi_opts, 0, &handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(coll);
    }
    RB_GC_GUARD(gc_guard);
    return handle ? soda_doc_new(handle, coll->dconn) : Qnil;
}

static VALUE soda_coll_data_guide(VALUE self)
{
    SodaColl_t *coll = To_SodaColl(self);
    dpiSodaDoc *handle;

    if (dpiSodaColl_getDataGuide(coll->handle, 0, &handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(coll);
    }
    return handle ? soda_doc_new(handle, coll->dconn) : Qnil;
}

static VALUE soda_coll_doc_count(int argc, VALUE *argv, VALUE self)
{
    SodaColl_t *coll = To_SodaColl(self);
    VALUE kwopts;
    dpiSodaOperOptions dpi_opts;
    VALUE gc_guard;
    uint64_t count;

    rb_scan_args(argc, argv, "00:", &kwopts);

    gc_guard = init_oper_options(&dpi_opts, kwopts, coll->dconn);
    if (dpiSodaColl_getDocCount(coll->handle, &dpi_opts, 0, &count) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(coll);
    }
    RB_GC_GUARD(gc_guard);
    return ULL2NUM(count);
}

static VALUE soda_coll_metadata(VALUE self)
{
    GET_STR(SodaColl, Metadata);
}

static VALUE soda_coll_name(VALUE self)
{
    GET_STR(SodaColl, Name);
}

static VALUE soda_coll_insert_many(int argc, VALUE *argv, VALUE self)
{
    SodaColl_t *coll = To_SodaColl(self);
    VALUE kwopts, docs;
    dpiSodaDoc **handles;
    dpiSodaOperOptions dpi_opts;
    VALUE gc_guard;
    size_t i, size;
    VALUE tmp;
    VALUE ary;

    rb_scan_args(argc, argv, "10:", &docs, &kwopts);

    gc_guard = init_oper_options(&dpi_opts, kwopts, coll->dconn);
    Check_Type(docs, T_ARRAY);
    size = RARRAY_LEN(docs);
    handles = ALLOCV_N(dpiSodaDoc *, tmp, size * 2);
    for (i = 0; i < size; i++) {
        handles[i] = To_SodaDoc(RARRAY_AREF(docs, i))->handle;
    }
    if (dpiSodaColl_insertManyWithOptions(coll->handle, size, handles, &dpi_opts, 0, handles + size) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(coll);
    }
    RB_GC_GUARD(gc_guard);
    ary = rb_ary_new_capa(size);
    for (i = 0; i < size; i++) {
        rb_ary_push(ary, soda_doc_new(handles[size + i], coll->dconn));
    }
    return ary;
}

static VALUE soda_coll_insert_one(int argc, VALUE *argv, VALUE self)
{
    SodaColl_t *coll = To_SodaColl(self);
    VALUE doc, kwopts, and_get;
    dpiSodaDoc *handle = NULL;
    dpiSodaOperOptions dpi_opts;
    VALUE gc_guard;
    static ID keywords[1];

    if (!keywords[0]) {
        keywords[0] = rb_intern_const("and_get");
    }
    rb_scan_args(argc, argv, "10:", &doc, &kwopts);
    rb_get_kwargs(kwopts, keywords, 0, -2, &and_get);

    gc_guard = init_oper_options(&dpi_opts, kwopts, coll->dconn);
    if (dpiSodaColl_insertOneWithOptions(coll->handle, To_SodaDoc(doc)->handle, &dpi_opts, 0, (and_get != Qundef && RTEST(and_get)) ? &handle : NULL) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(coll);
    }
    RB_GC_GUARD(gc_guard);
    return handle ? soda_doc_new(handle, coll->dconn) : Qnil;
}

static VALUE soda_coll_remove(int argc, VALUE *argv, VALUE self)
{
    SodaColl_t *coll = To_SodaColl(self);
    VALUE kwopts;
    dpiSodaOperOptions dpi_opts;
    VALUE gc_guard;
    uint64_t count;

    rb_scan_args(argc, argv, "00:", &kwopts);

    gc_guard = init_oper_options(&dpi_opts, kwopts, coll->dconn);
    if (dpiSodaColl_remove(coll->handle, &dpi_opts, 0, &count) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(coll);
    }
    RB_GC_GUARD(gc_guard);
    return ULL2NUM(count);
}

static VALUE soda_coll_replace_one(int argc, VALUE *argv, VALUE self)
{
    SodaColl_t *coll = To_SodaColl(self);
    VALUE doc, kwopts, and_get;
    dpiSodaOperOptions dpi_opts;
    VALUE gc_guard;
    int replaced;
    dpiSodaDoc *handle = NULL;
    static ID keywords[1];

    if (!keywords[0]) {
        keywords[0] = rb_intern_const("and_get");
    }
    rb_scan_args(argc, argv, "10:", &doc, &kwopts);
    rb_get_kwargs(kwopts, keywords, 0, -2, &and_get);

    gc_guard = init_oper_options(&dpi_opts, kwopts, coll->dconn);
    if (dpiSodaColl_replaceOne(coll->handle, &dpi_opts, To_SodaDoc(doc)->handle, 0, &replaced, RTEST(and_get) ? &handle : NULL) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(coll);
    }
    RB_GC_GUARD(gc_guard);
    if (and_get != Qundef && RTEST(and_get)) {
        return replaced ? soda_doc_new(handle, coll->dconn) : Qnil;
    } else {
        return replaced ? Qtrue : Qfalse;
    }
}

static VALUE soda_coll_save(int argc, VALUE *argv, VALUE self)
{
    SodaColl_t *coll = To_SodaColl(self);
    VALUE doc, kwopts, and_get;
    dpiSodaOperOptions dpi_opts;
    VALUE gc_guard;
    dpiSodaDoc *handle = NULL;
    static ID keywords[1];

    if (!keywords[0]) {
        keywords[0] = rb_intern_const("and_get");
    }
    rb_scan_args(argc, argv, "10:", &doc, &kwopts);
    rb_get_kwargs(kwopts, keywords, 0, -2, &and_get);

    gc_guard = init_oper_options(&dpi_opts, kwopts, coll->dconn);
    if (dpiSodaColl_saveWithOptions(coll->handle, To_SodaDoc(doc)->handle, &dpi_opts, 0, (and_get != Qundef && RTEST(and_get)) ? &handle : NULL) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(coll);
    }
    RB_GC_GUARD(gc_guard);
    return handle ? soda_doc_new(handle, coll->dconn) : Qnil;
}

static VALUE soda_coll_truncate(VALUE self)
{
    SodaColl_t *coll = To_SodaColl(self);

    if (dpiSodaColl_truncate(coll->handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(coll);
    }
    return Qnil;
}

static VALUE soda_db_alloc(VALUE klass)
{
    SodaDb_t *db;
    return TypedData_Make_Struct(klass, SodaDb_t, &soda_db_data_type, db);
}

static VALUE soda_db_new(dpiSodaDb *handle, rbOraDBConn *dconn)
{
    SodaDb_t *db;
    VALUE obj = TypedData_Make_Struct(cDb, SodaDb_t, &soda_db_data_type, db);

    RBORADB_INIT(db, dconn);
    db->handle = handle;
    return obj;
}

static void soda_db_free(void *arg)
{
    SodaDb_t *db = (SodaDb_t *)arg;
    RBORADB_RELEASE(db, dpiSodaDb);
    xfree(arg);
}

static VALUE soda_db_create_collection(int argc, VALUE *argv, VALUE self)
{
    SodaDb_t *db = To_SodaDb(self);
    VALUE name, kwopts, metadata, map;
    uint32_t flags;
    dpiSodaColl *handle;
    static ID keywords[1];

    if (!keywords[0]) {
        keywords[0] = rb_intern_const("map");
    }
    rb_scan_args(argc, argv, "20:", &name, &metadata, &kwopts);
    rb_get_kwargs(kwopts, keywords, 0, 1, &map);

    ExportString(name);
    ExportString(metadata);
    flags = (map != Qundef && RTEST(map)) ? DPI_SODA_FLAGS_CREATE_COLL_MAP : 0;
    if (dpiSodaDb_createCollection(db->handle, RSTRING_PTR(name), RSTRING_LEN(name),
        RSTRING_PTR(metadata), RSTRING_LEN(metadata), flags, &handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(db);
    }
    return soda_coll_new(handle, db->dconn);
}

static VALUE soda_db_create_document(VALUE self, VALUE key, VALUE content, VALUE media_type)
{
    SodaDb_t *db = To_SodaDb(self);
    dpiSodaDoc *handle;
    static const char *application_json = "application/json";
    const size_t application_json_len = strlen(application_json);

    OptExportString(key);
    SafeStringValue(content);
    OptExportString(media_type);
    if (NIL_P(media_type) || (RSTRING_LEN(media_type) == application_json_len && memcmp(RSTRING_PTR(media_type), application_json, application_json_len) == 0)) {
        content = rb_str_export_to_enc(content, rb_utf8_encoding());
    }
    if (dpiSodaDb_createDocument(db->handle, OPT_RSTRING_PTR(key), OPT_RSTRING_LEN(key),
        RSTRING_PTR(content), RSTRING_LEN(content), OPT_RSTRING_PTR(media_type), OPT_RSTRING_LEN(media_type), 0, &handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(db);
    }
    return soda_doc_new(handle, db->dconn);
}

static VALUE iter_coll_cursor(VALUE arg)
{
    SodaCollCursor_t *cursor = (SodaCollCursor_t *)arg;
    while (1) {
        dpiSodaColl *handle;
        VALUE obj;

        if (dpiSodaCollCursor_getNext(cursor->handle, 0, &handle) != DPI_SUCCESS) {
            RBORADB_RAISE_ERROR(cursor);
        }
        if (handle == NULL) {
            break;
        }
        obj = soda_coll_new(handle, cursor->dconn);
        rb_proc_call_with_block(cursor->proc, 1, &obj, Qnil);
    }
    return Qnil;
}

static VALUE soda_db_collections(int argc, VALUE *argv, VALUE self)
{
    SodaDb_t *db = To_SodaDb(self);
    VALUE start_name;
    SodaCollCursor_t cursor;
    int state;

    rb_scan_args(argc, argv, "01", &start_name);

    RETURN_ENUMERATOR(self, 0, 0);
    OptExportString(start_name);
    if (dpiSodaDb_getCollections(db->handle, OPT_RSTRING_PTR(start_name), OPT_RSTRING_LEN(start_name), 0, &cursor.handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(db);
    }
    cursor.dconn = db->dconn;
    cursor.proc = rb_block_proc();
    rb_protect(iter_coll_cursor, (VALUE)&cursor, &state);
    dpiSodaCollCursor_release(cursor.handle);
    if (state) {
        rb_jump_tag(state);
    }
    return Qnil;
}

static VALUE soda_db_collection_names(int argc, VALUE *argv, VALUE self)
{
    SodaDb_t *db = To_SodaDb(self);
    dpiSodaCollNames names;
    VALUE start_name, kwopts, limit;
    VALUE ary;
    uint32_t idx;
    static ID keywords[1];

    if (!keywords[0]) {
        keywords[0] = rb_intern_const("limit");
    }
    rb_scan_args(argc, argv, "01:", &start_name, &kwopts);
    rb_get_kwargs(kwopts, keywords, 0, 1, &limit);

    OptExportString(start_name);
    if (limit == Qundef) {
        limit = INT2FIX(0);
    }
    if (dpiSodaDb_getCollectionNames(db->handle, OPT_RSTRING_PTR(start_name), OPT_RSTRING_LEN(start_name), NUM2UINT(limit), 0, &names) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(db);
    }
    ary = rb_ary_new_capa(names.numNames);
    for (idx = 0; idx < names.numNames; idx++) {
        rb_ary_push(ary, rb_enc_str_new(names.names[idx], names.nameLengths[idx], rb_utf8_encoding()));
    }
    dpiSodaDb_freeCollectionNames(db->handle, &names);
    return ary;
}

static VALUE soda_db_open_collection(VALUE self, VALUE name)
{
    SodaDb_t *db = To_SodaDb(self);
    dpiSodaColl *handle;

    ExportString(name);
    if (dpiSodaDb_openCollection(db->handle, RSTRING_PTR(name), RSTRING_LEN(name), 0, &handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(db);
    }
    return soda_coll_new(handle, db->dconn);
}

static VALUE soda_doc_alloc(VALUE klass)
{
    SodaDoc_t *doc;
    return TypedData_Make_Struct(klass, SodaDoc_t, &soda_doc_data_type, doc);
}

static VALUE soda_doc_new(dpiSodaDoc *handle, rbOraDBConn *dconn)
{
    SodaDoc_t *doc;
    VALUE obj = TypedData_Make_Struct(cDoc, SodaDoc_t, &soda_doc_data_type, doc);

    RBORADB_INIT(doc, dconn);
    doc->handle = handle;
    return obj;
}

static void soda_doc_free(void *arg)
{
    SodaDoc_t *doc = (SodaDoc_t *)arg;
    RBORADB_RELEASE(doc, dpiSodaDoc);
    xfree(arg);
}

static VALUE soda_doc_content(VALUE self)
{
    SodaDoc_t *doc = To_SodaDoc(self);
    const char *val;
    uint32_t len;
    const char *enc;

    if (dpiSodaDoc_getContent(doc->handle, &val, &len, &enc) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(doc);
    }
    if (enc == NULL) {
        return rb_str_new(val, len);
    } else {
        return rb_enc_str_new(val, len, rb_enc_find(enc));
    }
}

static VALUE soda_doc_created_on(VALUE self)
{
    GET_STR(SodaDoc, CreatedOn);
}

static VALUE soda_doc_key(VALUE self)
{
    GET_STR(SodaDoc, Key);
}

static VALUE soda_doc_last_modified(VALUE self)
{
    GET_STR(SodaDoc, LastModified);
}

static VALUE soda_doc_media_type(VALUE self)
{
    GET_STR(SodaDoc, MediaType);
}

static VALUE soda_doc_version(VALUE self)
{
    GET_STR(SodaDoc, Version);
}

void rboradb_soda_init(VALUE mOracleDB)
{
    VALUE mSoda = rb_define_module_under(mOracleDB, "Soda");

    cColl = rb_define_class_under(mSoda, "Coll", rb_cObject);
    rb_define_alloc_func(cColl, soda_coll_alloc);
    rb_define_method(cColl, "initialize", rboradb_notimplement, -1);
    rb_define_private_method(cColl, "initialize_copy", rboradb_notimplement, -1);
    rb_define_method(cColl, "create_index", soda_coll_create_index, 1);
    rb_define_method(cColl, "drop", soda_coll_drop, 0);
    rb_define_method(cColl, "drop_index", soda_coll_drop_index, -1);
    rb_define_method(cColl, "find", soda_coll_find, -1);
    rb_define_method(cColl, "find_one", soda_coll_find_one, -1);
    rb_define_method(cColl, "data_guide", soda_coll_data_guide, 0);
    rb_define_method(cColl, "doc_count", soda_coll_doc_count, -1);
    rb_define_method(cColl, "metadata", soda_coll_metadata, 0);
    rb_define_method(cColl, "name", soda_coll_name, 0);
    rb_define_method(cColl, "insert_many", soda_coll_insert_many, -1);
    rb_define_method(cColl, "insert_one", soda_coll_insert_one, -1);
    rb_define_method(cColl, "remove", soda_coll_remove, -1);
    rb_define_method(cColl, "replace_one", soda_coll_replace_one, -1);
    rb_define_method(cColl, "save", soda_coll_save, -1);
    rb_define_method(cColl, "truncate", soda_coll_truncate, 0);

    cDb = rb_define_class_under(mSoda, "Db", rb_cObject);
    rb_define_alloc_func(cDb, soda_db_alloc);
    rb_define_method(cDb, "initialize", rboradb_notimplement, -1);
    rb_define_private_method(cDb, "initialize_copy", rboradb_notimplement, -1);
    rb_define_method(cDb, "create_collection", soda_db_create_collection, -1);
    rb_define_method(cDb, "create_document", soda_db_create_document, 3);
    rb_define_method(cDb, "collections", soda_db_collections, -1);
    rb_define_method(cDb, "collection_names", soda_db_collection_names, -1);
    rb_define_method(cDb, "open_collection", soda_db_open_collection, 1);

    cDoc = rb_define_class_under(mSoda, "Doc", rb_cObject);
    rb_define_alloc_func(cDoc, soda_doc_alloc);
    rb_define_method(cDoc, "initialize", rboradb_notimplement, -1);
    rb_define_private_method(cDoc, "initialize_copy", rboradb_notimplement, -1);
    rb_define_method(cDoc, "content", soda_doc_content, 0);
    rb_define_method(cDoc, "created_on", soda_doc_created_on, 0);
    rb_define_method(cDoc, "key", soda_doc_key, 0);
    rb_define_method(cDoc, "last_modified", soda_doc_last_modified, 0);
    rb_define_method(cDoc, "media_type", soda_doc_media_type, 0);
    rb_define_method(cDoc, "version", soda_doc_version, 0);
}

VALUE rboradb_soda_db_new(dpiSodaDb *handle, rbOraDBConn *dconn)
{
    return soda_db_new(handle, dconn);
}
