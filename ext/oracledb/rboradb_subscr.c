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
#include "ruby/thread_native.h"
#ifndef WIN32
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#endif

#define To_Subscr(obj) ((Subscr_t *)rb_check_typeddata((obj), &subscr_data_type))
#define MEM_DUP(dest, src, type) do { \
    dest = (type*)malloc(sizeof(type)); \
    *dest = *src; \
} while (0)

#define MEM_DUP_N(dest, src, type, num) do { \
    const type *src__ = src; \
    type *dest__ = (type*)malloc(sizeof(type) * num); \
    memcpy(dest__, src__, sizeof(type) * num); \
    dest = dest__; \
} while (0)

typedef struct {
    RBORADB_COMMON_HEADER(dpiSubscr);
    VALUE callback;
} Subscr_t;

/* This must be allocated and freed by malloc and free, not by xmalloc and xfree */
typedef struct subscr_msg {
    struct subscr_msg *next;
    dpiSubscrMessage message;
    VALUE callback;
} subscr_msg_t;

static VALUE cSubscr;
static VALUE cSubscrMessage;
static VALUE cSubscrMessageQuery;
static VALUE cSubscrMessageTable;
static VALUE cSubscrMessageRow;
static VALUE thread = Qnil;

static ID id_at_consumer_name;
static ID id_at_db_name;
static ID id_at_error_info;
static ID id_at_event_type;
static ID id_at_id;
static ID id_at_name;
static ID id_at_operation;
static ID id_at_queries;
static ID id_at_queue_name;
static ID id_at_registered;
static ID id_at_rowid;
static ID id_at_rows;
static ID id_at_tables;
static ID id_at_tx_id;

#ifdef WIN32
static HANDLE hEvent;
#else
static int read_fd; /* non-blocking read-side pipe descriptor  */
static int write_fd; /* blocking write-side pipe descriptor */
#endif
static subscr_msg_t *head = NULL;
static subscr_msg_t **tail = &head;
static rb_nativethread_lock_t lock;

static VALUE subscr_thread(void *dummy);
static VALUE subscr_alloc(VALUE klass);
static void subscr_mark(void *arg);
static void subscr_free(void *arg);
static void subscr_callback(void *context, dpiSubscrMessage *message);

static subscr_msg_t *subscr_msg_alloc(dpiSubscrMessage *message, VALUE callback);
static void subscr_msg_free(subscr_msg_t *msg);
static dpiSubscrMessageTable *subscr_message_table_alloc(dpiSubscrMessageTable *tables, uint32_t num_tables);
static void subscr_message_table_free(dpiSubscrMessageTable *tables, uint32_t num_tables);

static VALUE from_dpiSubscrMessage(const dpiSubscrMessage *msg);
static VALUE from_dpiSubscrMessageQuery(const dpiSubscrMessageQuery *query);
static VALUE from_dpiSubscrMessageTable(const dpiSubscrMessageTable *tbl);
static VALUE from_dpiSubscrMessageRow(const dpiSubscrMessageRow *row);

static const struct rb_data_type_struct subscr_data_type = {
    "OracleDB::Subscr",
    {subscr_mark, subscr_free,},
    NULL, NULL,
};

static VALUE to_ruby_msg(VALUE args)
{
    subscr_msg_t *msg = (subscr_msg_t *)args;
    VALUE ary = rb_ary_new_capa(2);
    VALUE arg = from_dpiSubscrMessage(&msg->message);
    rb_ary_push(ary, msg->callback);
    rb_ary_push(ary, rb_ary_new_from_args(1, arg));
    return ary;
}

static VALUE run_callback_proc(VALUE args)
{
    const VALUE *ary = RARRAY_CONST_PTR_TRANSIENT(args);
    return rb_proc_call(ary[0], ary[1]);
}

static VALUE subscr_thread(void *dummy)
{
    while (1) {
#ifdef WIN32
        rb_w32_wait_events_blocking(hEvent, 1, INFINITE);
        ResetEvent(hEvent);
#else
        char dummy[256];
        int rv;

        rb_thread_wait_fd(read_fd);
        do {
            rv = read(read_fd, dummy, sizeof(dummy));
        } while (rv > 0 || (rv == -1 && errno == EINTR));
#endif
        rb_native_mutex_lock(&lock);
        while (head != NULL) {
            subscr_msg_t *msg = head;
            VALUE args;
            int state = 0;

            if (tail == &head->next) {
                tail = &head;
            }
            head = head->next;
            rb_native_mutex_unlock(&lock);

            args = rb_protect(to_ruby_msg, (VALUE)msg, &state);
            subscr_msg_free(msg);
            if (!state) {
                rb_protect(run_callback_proc, args, &state);
            }
            if (state) {
                /* ignore errors */
                rb_set_errinfo(Qnil);
            }
            rb_native_mutex_lock(&lock);
        }
        rb_native_mutex_unlock(&lock);
    }
    return Qnil;
}

static subscr_msg_t *subscr_msg_alloc(dpiSubscrMessage *message, VALUE callback)
{
    subscr_msg_t *msg = calloc(1, sizeof(*msg));

    msg->message = *message;
    if (message->dbName != NULL) {
        MEM_DUP_N(msg->message.dbName, message->dbName, char, message->dbNameLength);
    }
    msg->message.tables = subscr_message_table_alloc(message->tables, message->numTables);
    if (message->queries != NULL) {
        uint32_t idx;

        MEM_DUP_N(msg->message.queries, message->queries, dpiSubscrMessageQuery, message->numQueries);
        for (idx = 0; idx < message->numQueries; idx++) {
            msg->message.queries[idx].tables = subscr_message_table_alloc(message->queries[idx].tables, message->queries[idx].numTables);
        }
    }
    //msg->message.errorInfo = errinfo_dup(message->errorInfo);
    if (message->txId != NULL) {
        MEM_DUP_N(msg->message.txId, message->txId, char, message->txIdLength);
    }
    if (message->queueName != NULL) {
        MEM_DUP_N(msg->message.queueName, message->queueName, char, message->queueNameLength);
    }
    if (message->consumerName != NULL) {
        MEM_DUP_N(msg->message.consumerName, message->consumerName, char, message->consumerNameLength);
    }
    msg->callback = callback;
    return msg;
}

static void subscr_msg_free(subscr_msg_t *msg)
{
    free((void*)msg->message.dbName);
    subscr_message_table_free(msg->message.tables, msg->message.numTables);
    if (msg->message.queries != NULL) {
        uint32_t idx;
        for (idx = 0; idx < msg->message.numQueries; idx++) {
            subscr_message_table_free(msg->message.queries[idx].tables, msg->message.queries[idx].numTables);
        }
        free(msg->message.queries);
    }
    free((void*)msg->message.txId);
    free((void*)msg->message.queueName);
    free((void*)msg->message.consumerName);
    free(msg);
}

static dpiSubscrMessageTable *subscr_message_table_alloc(dpiSubscrMessageTable *tables, uint32_t num_tables)
{
    dpiSubscrMessageTable *new_tables;
    uint32_t i, j;

    MEM_DUP_N(new_tables, tables, dpiSubscrMessageTable, num_tables);
    for (i = 0; i < num_tables; i++) {
        MEM_DUP_N(new_tables[i].name, tables[i].name, char, tables[i].nameLength);
        if (tables[i].rows != NULL) {
            MEM_DUP_N(new_tables[i].rows, tables[i].rows, dpiSubscrMessageRow, tables[i].numRows);
            for (j = 0; j < tables[i].numRows; j++) {
                MEM_DUP_N(new_tables[i].rows[j].rowid, tables[i].rows[j].rowid, char, tables[i].rows[j].rowidLength);
            }
        }
    }
    return new_tables;
}

static void subscr_message_table_free(dpiSubscrMessageTable *tables, uint32_t num_tables)
{
    uint32_t i, j;

    for (i = 0; i < num_tables; i++) {
        if (tables[i].rows != NULL) {
            for (j = 0; j < tables[i].numRows; j++) {
	        free((void*)tables[i].rows[j].rowid);
            }
            free(tables[i].rows);
        }
        free((void*)tables[i].name);
    }
    free(tables);
}

static VALUE from_dpiSubscrMessage(const dpiSubscrMessage *msg)
{
    VALUE obj = rb_obj_alloc(cSubscrMessage);
    VALUE ary;
    uint32_t idx;

    rb_ivar_set(obj, id_at_event_type, rboradb_from_dpiEventType(msg->eventType));
    rb_ivar_set(obj, id_at_db_name, rb_enc_str_new(msg->dbName, msg->dbNameLength, rb_utf8_encoding()));

    ary = rb_ary_new_capa(msg->numTables);
    for (idx = 0; idx < msg->numTables; idx++) {
        rb_ary_push(ary, from_dpiSubscrMessageTable(&msg->tables[idx]));
    }
    rb_ivar_set(obj, id_at_tables, ary);

    ary = rb_ary_new_capa(msg->numQueries);
    for (idx = 0; idx < msg->numQueries; idx++) {
        rb_ary_push(ary, from_dpiSubscrMessageQuery(&msg->queries[idx]));
    }
    rb_ivar_set(obj, id_at_queries, ary);

    rb_ivar_set(obj, id_at_error_info, msg->errorInfo ? rboradb_from_dpiErrorInfo(msg->errorInfo) : Qnil);

    rb_ivar_set(obj, id_at_tx_id, msg->txId ? rb_str_new(msg->txId, msg->txIdLength) : Qnil);
    rb_ivar_set(obj, id_at_registered, msg->registered ? Qtrue : Qfalse);
    rb_ivar_set(obj, id_at_queue_name, msg->queueName ? rb_enc_str_new(msg->queueName, msg->queueNameLength, rb_utf8_encoding()) : Qnil);
    rb_ivar_set(obj, id_at_consumer_name, msg->consumerName ? rb_enc_str_new(msg->consumerName, msg->consumerNameLength, rb_utf8_encoding()) : Qnil);

    return obj;
}

static VALUE from_dpiSubscrMessageQuery(const dpiSubscrMessageQuery *query)
{
    VALUE obj = rb_obj_alloc(cSubscrMessageQuery);
    VALUE ary;
    uint32_t idx;

    rb_ivar_set(obj, id_at_id, ULL2NUM(query->id));
    rb_ivar_set(obj, id_at_operation, rboradb_from_dpiOpCode(query->operation));

    ary = rb_ary_new_capa(query->numTables);
    for (idx = 0; idx < query->numTables; idx++) {
        rb_ary_push(ary, from_dpiSubscrMessageTable(&query->tables[idx]));
    }
    rb_ivar_set(obj, id_at_tables, ary);
    return obj;
}

static VALUE from_dpiSubscrMessageTable(const dpiSubscrMessageTable *tbl)
{
    VALUE obj = rb_obj_alloc(cSubscrMessageTable);
    VALUE ary;
    uint32_t idx;

    rb_ivar_set(obj, id_at_operation, rboradb_from_dpiOpCode(tbl->operation));
    rb_ivar_set(obj, id_at_name, rb_enc_str_new(tbl->name, tbl->nameLength, rb_utf8_encoding()));

    ary = rb_ary_new_capa(tbl->numRows);
    for (idx = 0; idx < tbl->numRows; idx++) {
        rb_ary_push(ary, from_dpiSubscrMessageRow(&tbl->rows[idx]));
    }
    rb_ivar_set(obj, id_at_rows, ary);
    return obj;
}

static VALUE from_dpiSubscrMessageRow(const dpiSubscrMessageRow *row)
{
    VALUE obj = rb_obj_alloc(cSubscrMessageRow);

    rb_ivar_set(obj, id_at_operation, rboradb_from_dpiOpCode(row->operation));
    rb_ivar_set(obj, id_at_rowid, rb_usascii_str_new(row->rowid, row->rowidLength));
    return obj;
}

static VALUE subscr_alloc(VALUE klass)
{
    Subscr_t *subscr;
    VALUE obj = TypedData_Make_Struct(klass, Subscr_t, &subscr_data_type, subscr);
    subscr->callback = Qnil;
    return obj;
}

static void subscr_mark(void *arg)
{
    Subscr_t *subscr = (Subscr_t *)arg;
    rb_gc_mark(subscr->callback);
}

static void subscr_free(void *arg)
{
    Subscr_t *subscr = (Subscr_t *)arg;
    RBORADB_RELEASE(subscr, dpiSubscr);
    xfree(arg);
}

static void subscr_callback(void *context, dpiSubscrMessage *message)
{
    Subscr_t *subscr = (Subscr_t *)context;
    subscr_msg_t *msg = subscr_msg_alloc(message, subscr->callback);
#ifndef WIN32
    char dummy = 0;
    int rv;
#endif

    rb_native_mutex_lock(&lock);
    *tail = msg;
    tail = &msg->next;
#ifdef WIN32
    SetEvent(hEvent);
#else
    do {
        rv = write(write_fd, &dummy, 1);
    } while (rv == -1 && errno == EINTR);
#endif
    rb_native_mutex_unlock(&lock);
}

static VALUE subscr_initialize(VALUE self, VALUE conn, VALUE params)
{
    Subscr_t *subscr = To_Subscr(self);
    rbOraDBConn *dconn = rboradb_get_dconn(conn);
    dpiSubscrCreateParams prms;
    VALUE tmp;
    VALUE callback = Qnil;
    dpiSubscr *handle;

    if (dpiContext_initSubscrCreateParams(dconn->ctxt->handle, &prms) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(subscr);
    }
    tmp = rboradb_set_dpiSubscrCreateParams(&prms, params);
    if (prms.callbackContext) {
        callback = (VALUE)prms.callbackContext;
        prms.callback = subscr_callback;
        prms.callbackContext = subscr;
    }
    if (rbOraDBConn_subscribe(dconn->handle, &prms, &handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(subscr);
    }
    RBORADB_INIT(subscr, dconn);
    subscr->handle = handle;
    subscr->callback = callback;
    RB_GC_GUARD(tmp);

    if (NIL_P(thread)) {
#ifdef WIN32
        hEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
#else
        int fds[2];
        int fd_flags;

        if (pipe(fds) != 0) {
            rb_sys_fail("pipe");
        }
        read_fd = fds[0];
        write_fd = fds[1];
        fd_flags = fcntl(read_fd, F_GETFL, 0);
        fcntl(read_fd, F_SETFL, fd_flags | O_NONBLOCK);
#endif
        rb_native_mutex_initialize(&lock);
        thread = rb_thread_create(subscr_thread, NULL);
    }
    return Qnil;
}

static VALUE subscr_prepare_stmt(VALUE self, VALUE sql)
{
    Subscr_t *subscr = To_Subscr(self);
    dpiStmt *handle;

    ExportString(sql);
    if (dpiSubscr_prepareStmt(subscr->handle, RSTRING_PTR(sql), RSTRING_LEN(sql), &handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(subscr);
    }
    return rboradb_from_dpiStmt(handle, subscr->dconn, 0, 0);
}

static VALUE subscr_unsubscribe(VALUE self)
{
    Subscr_t *subscr = To_Subscr(self);

    if (rbOraDBConn_unsubscribe(subscr->dconn->handle, subscr->handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(subscr);
    }
    return Qnil;
}

void rboradb_subscr_init(VALUE mOracleDB)
{
    id_at_consumer_name = rb_intern("@consumer_name");
    id_at_db_name = rb_intern("@db_name");
    id_at_error_info = rb_intern("@error_info");
    id_at_event_type = rb_intern("@event_type");
    id_at_id = rb_intern("@id");
    id_at_name = rb_intern("@name");
    id_at_operation = rb_intern("@operation");
    id_at_queries = rb_intern("@queries");
    id_at_queue_name = rb_intern("@queue_name");
    id_at_registered = rb_intern("@registered");
    id_at_rowid = rb_intern("@rowid");
    id_at_rows = rb_intern("@rows");
    id_at_tables = rb_intern("@tables");
    id_at_tx_id = rb_intern("@tx_id");

    cSubscr = rb_define_class_under(mOracleDB, "Subscr", rb_cObject);
    rb_define_alloc_func(cSubscr, subscr_alloc);
    rb_define_private_method(cSubscr, "initialize", subscr_initialize, 2);
    rb_define_private_method(cSubscr, "initialize_copy", rboradb_notimplement, -1);
    rb_define_method(cSubscr, "prepare_stmt", subscr_prepare_stmt, 1);
    rb_define_method(cSubscr, "unsubscribe", subscr_unsubscribe, 0);

    cSubscrMessage = rb_define_class_under(cSubscr, "Message", rb_cObject);
    rb_define_attr(cSubscrMessage, "event_type", 1, 0);
    rb_define_attr(cSubscrMessage, "db_name", 1, 0);
    rb_define_attr(cSubscrMessage, "tables", 1, 0);
    rb_define_attr(cSubscrMessage, "queries", 1, 0);
    rb_define_attr(cSubscrMessage, "error_info", 1, 0);
    rb_define_attr(cSubscrMessage, "tx_id", 1, 0);
    rb_define_attr(cSubscrMessage, "registered", 1, 0);
    rb_define_attr(cSubscrMessage, "queue_name", 1, 0);
    rb_define_attr(cSubscrMessage, "consumer_name", 1, 0);

    cSubscrMessageQuery = rb_define_class_under(cSubscrMessage, "Query", rb_cObject);
    rb_define_attr(cSubscrMessageQuery, "id", 1, 0);
    rb_define_attr(cSubscrMessageQuery, "operation", 1, 0);
    rb_define_attr(cSubscrMessageQuery, "tables", 1, 0);

    cSubscrMessageTable = rb_define_class_under(cSubscrMessage, "Table", rb_cObject);
    rb_define_attr(cSubscrMessageTable, "operation", 1, 0);
    rb_define_attr(cSubscrMessageTable, "name", 1, 0);
    rb_define_attr(cSubscrMessageTable, "rows", 1, 0);

    cSubscrMessageRow = rb_define_class_under(cSubscrMessage, "Row", rb_cObject);
    rb_define_attr(cSubscrMessageRow, "operation", 1, 0);
    rb_define_attr(cSubscrMessageRow, "rowid", 1, 0);
}
