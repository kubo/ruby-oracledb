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

#define To_DeqOptions(obj) ((DeqOptions_t *)rb_check_typeddata((obj), &deqopts_data_type))
#define To_EnqOptions(obj) ((EnqOptions_t *)rb_check_typeddata((obj), &enq_options_data_type))
#define To_MsgProps(obj) ((MsgProps_t *)rb_check_typeddata((obj), &msg_props_data_type))
#define To_Queue(obj) ((Queue_t *)rb_check_typeddata((obj), &queue_data_type))

typedef struct {
    RBORADB_COMMON_HEADER(dpiDeqOptions);
} DeqOptions_t;

typedef struct {
    RBORADB_COMMON_HEADER(dpiEnqOptions);
} EnqOptions_t;

typedef struct {
    RBORADB_COMMON_HEADER(dpiMsgProps);
    dpiObjectType *payload_objtype;
} MsgProps_t;

typedef struct {
    RBORADB_COMMON_HEADER(dpiQueue);
    dpiObjectType *payload_objtype;
} Queue_t;

static VALUE cDeqOptions;
static VALUE cEnqOptions;
static VALUE cMsgProps;
static VALUE cQueue;

static void deqopts_free(void *arg)
{
    DeqOptions_t *opts = (DeqOptions_t *)arg;
    RBORADB_RELEASE(opts, dpiDeqOptions);
    xfree(arg);
}

static const struct rb_data_type_struct deqopts_data_type = {
    "OracleDB::DeqOptions",
    {NULL, deqopts_free,},
    NULL, NULL,
};

static void enq_options_free(void *arg)
{
    EnqOptions_t *opts = (EnqOptions_t *)arg;
    RBORADB_RELEASE(opts, dpiEnqOptions);
    xfree(arg);
}

static const struct rb_data_type_struct enq_options_data_type = {
    "OracleDB::EnqOptions",
    {NULL, enq_options_free,},
    NULL, NULL,
};

static void msg_props_free(void *arg)
{
    MsgProps_t *props = (MsgProps_t *)arg;
    if (props->payload_objtype) {
        dpiObjectType_release(props->payload_objtype);
    }
    RBORADB_RELEASE(props, dpiMsgProps);
    xfree(arg);
}

static const struct rb_data_type_struct msg_props_data_type = {
    "OracleDB::MsgProps",
    {NULL, msg_props_free,},
    NULL, NULL,
};

static void queue_free(void *arg)
{
    Queue_t *queue = (Queue_t *)arg;
    if (queue->payload_objtype) {
        dpiObjectType_release(queue->payload_objtype);
    }
    RBORADB_RELEASE(queue, dpiQueue);
    xfree(arg);
}

static const struct rb_data_type_struct queue_data_type = {
    "OracleDB::Queue",
    {NULL, queue_free,},
    NULL, NULL,
};

static VALUE deqopts_alloc(VALUE klass)
{
    DeqOptions_t *opts;
    return TypedData_Make_Struct(klass, DeqOptions_t, &deqopts_data_type, opts);
}

static VALUE deqopts_initialize(VALUE self, VALUE conn)
{
    DeqOptions_t *opts = To_DeqOptions(self);
    rbOraDBConn *dconn = rboradb_get_dconn(conn);

    RBORADB_INIT(opts, dconn);
    if (dpiConn_newDeqOptions(dconn->handle, &opts->handle) != DPI_SUCCESS) {
        rboradb_raise_error(dconn->ctxt);
    }
    return Qnil;
}

static VALUE deqopts_condition(VALUE self)
{
    GET_STR(DeqOptions, Condition);
}

static VALUE deqopts_set_condition(VALUE self, VALUE obj)
{
    SET_STR(DeqOptions, Condition);
}

static VALUE deqopts_consumer_name(VALUE self)
{
    GET_STR(DeqOptions, ConsumerName);
}

static VALUE deqopts_set_consumer_name(VALUE self, VALUE obj)
{
    SET_STR(DeqOptions, ConsumerName);
}

static VALUE deqopts_correlation(VALUE self)
{
    GET_STR(DeqOptions, Correlation);
}

static VALUE deqopts_set_correlation(VALUE self, VALUE obj)
{
    SET_STR(DeqOptions, Correlation);
}

static VALUE deqopts_set_delivery_mode(VALUE self, VALUE obj)
{
    SET_ENUM(DeqOptions, DeliveryMode, dpiMessageDeliveryMode);
}

static VALUE deqopts_mode(VALUE self)
{
    GET_ENUM(DeqOptions, Mode, dpiDeqMode);
}

static VALUE deqopts_set_mode(VALUE self, VALUE obj)
{
    SET_ENUM(DeqOptions, Mode, dpiDeqMode);
}

static VALUE deqopts_msg_id(VALUE self)
{
    GET_STR(DeqOptions, MsgId);
}

static VALUE deqopts_set_msg_id(VALUE self, VALUE obj)
{
    SET_STR(DeqOptions, MsgId);
}

static VALUE deqopts_navigation(VALUE self)
{
    GET_ENUM(DeqOptions, Navigation, dpiDeqNavigation);
}

static VALUE deqopts_set_navigation(VALUE self, VALUE obj)
{
    SET_ENUM(DeqOptions, Navigation, dpiDeqNavigation);
}

static VALUE deqopts_transformation(VALUE self)
{
    GET_STR(DeqOptions, Transformation);
}

static VALUE deqopts_set_transformation(VALUE self, VALUE obj)
{
    SET_STR(DeqOptions, Transformation);
}

static VALUE deqopts_visibility(VALUE self)
{
    GET_ENUM(DeqOptions, Visibility, dpiVisibility);
}

static VALUE deqopts_set_visibility(VALUE self, VALUE obj)
{
    SET_ENUM(DeqOptions, Visibility, dpiVisibility);
}

static VALUE deqopts_wait(VALUE self)
{
    GET_UINT32(DeqOptions, Wait);
}

static VALUE deqopts_set_wait(VALUE self, VALUE obj)
{
    SET_UINT32(DeqOptions, Wait);
}

static VALUE enq_options_alloc(VALUE klass)
{
    EnqOptions_t *opts;
    return TypedData_Make_Struct(klass, EnqOptions_t, &enq_options_data_type, opts);
}

static VALUE enq_options_initialize(VALUE self, VALUE conn)
{
    EnqOptions_t *opts = To_EnqOptions(self);
    rbOraDBConn *dconn = rboradb_get_dconn(conn);

    RBORADB_INIT(opts, dconn);
    if (dpiConn_newEnqOptions(dconn->handle, &opts->handle) != DPI_SUCCESS) {
        rboradb_raise_error(dconn->ctxt);
    }
    return Qnil;
}

static VALUE enqopts_set_delivery_mode(VALUE self, VALUE obj)
{
    SET_ENUM(EnqOptions, DeliveryMode, dpiMessageDeliveryMode);
}

static VALUE enqopts_transformation(VALUE self)
{
    GET_STR(EnqOptions, Transformation);
}

static VALUE enqopts_set_transformation(VALUE self, VALUE obj)
{
    SET_STR(EnqOptions, Transformation);
}

static VALUE enqopts_visibility(VALUE self)
{
    GET_ENUM(EnqOptions, Visibility, dpiVisibility);
}

static VALUE enqopts_set_visibility(VALUE self, VALUE obj)
{
    SET_ENUM(EnqOptions, Visibility, dpiVisibility);
}

static VALUE msg_props_alloc(VALUE klass)
{
    MsgProps_t *props;
    VALUE val = TypedData_Make_Struct(klass, MsgProps_t, &msg_props_data_type, props);
    return val;
}

static VALUE msg_props_initialize(VALUE self, VALUE conn)
{
    MsgProps_t *props = To_MsgProps(self);
    rbOraDBConn *dconn = rboradb_get_dconn(conn);

    RBORADB_INIT(props, dconn);
    if (dpiConn_newMsgProps(dconn->handle, &props->handle) != DPI_SUCCESS) {
        rboradb_raise_error(dconn->ctxt);
    }
    return Qnil;
}

static VALUE msgprops_num_attempts(VALUE self)
{
    GET_INT32(MsgProps, NumAttempts);
}

static VALUE msgprops_correlation(VALUE self)
{
    GET_STR(MsgProps, Correlation);
}

static VALUE msgprops_set_correlation(VALUE self, VALUE obj)
{
    SET_STR(MsgProps, Correlation);
}

static VALUE msgprops_delay(VALUE self)
{
    GET_INT32(MsgProps, Delay);
}

static VALUE msgprops_set_delay(VALUE self, VALUE obj)
{
    SET_INT32(MsgProps, Delay);
}

static VALUE msgprops_delivery_mode(VALUE self)
{
    GET_ENUM(MsgProps, DeliveryMode, dpiMessageDeliveryMode);
}

static VALUE msgprops_enq_time(VALUE self)
{
    GET_STRUCT(MsgProps, EnqTime, dpiTimestamp);
}

static VALUE msgprops_exception_q(VALUE self)
{
    GET_STR(MsgProps, ExceptionQ);
}

static VALUE msgprops_set_exception_q(VALUE self, VALUE obj)
{
    SET_STR(MsgProps, ExceptionQ);
}

static VALUE msgprops_expiration(VALUE self)
{
    GET_INT32(MsgProps, Expiration);
}

static VALUE msgprops_set_expiration(VALUE self, VALUE obj)
{
    SET_INT32(MsgProps, Expiration);
}

static VALUE msgprops_msg_id(VALUE self)
{
    GET_STR(MsgProps, MsgId);
}

static VALUE msgprops_original_msg_id(VALUE self)
{
    GET_STR(MsgProps, OriginalMsgId);
}

static VALUE msgprops_set_original_msg_id(VALUE self, VALUE obj)
{
    SET_STR(MsgProps, OriginalMsgId);
}

static VALUE msgprops_payload(VALUE self)
{
    MsgProps_t *props = To_MsgProps(self);

    if (props->payload_objtype) {
        dpiObject *obj;

        if (dpiMsgProps_getPayload(props->handle, &obj, NULL, NULL) != DPI_SUCCESS) {
            RBORADB_RAISE_ERROR(props);
        }
        return obj ? rboradb_from_dpiObject(obj, props->payload_objtype, props->dconn, 1) : Qnil;
    } else {
        const char *val;
        uint32_t len;

        if (dpiMsgProps_getPayload(props->handle, NULL, &val, &len) != DPI_SUCCESS) {
            RBORADB_RAISE_ERROR(props);
        }
        return val ? rb_str_new(val, len) : Qnil;
    }
}

static VALUE msgprops_set_payload(VALUE self, VALUE obj)
{
    MsgProps_t *props = To_MsgProps(self);
    dpiObjectType *objtype = rboradb_get_ObjectType_or_null(obj);

    if (objtype) {
        if (dpiMsgProps_setPayloadObject(props->handle, rboradb_to_dpiObject(obj)) != DPI_SUCCESS) {
            RBORADB_RAISE_ERROR(props);
        }
    } else {
        rb_string_value(&obj);
        if (dpiMsgProps_setPayloadBytes(props->handle, RSTRING_PTR(obj), RSTRING_LEN(obj)) != DPI_SUCCESS) {
            RBORADB_RAISE_ERROR(props);
        }
    }
    if (props->payload_objtype) {
        dpiObjectType_release(props->payload_objtype);
    }
    props->payload_objtype = objtype;
    if (props->payload_objtype) {
        dpiObjectType_addRef(props->payload_objtype);
    }
    return Qnil;
}

static VALUE msgprops_priority(VALUE self)
{
    GET_INT32(MsgProps, Priority);
}

static VALUE msgprops_set_priority(VALUE self, VALUE obj)
{
    SET_INT32(MsgProps, Priority);
}

static VALUE msgprops_state(VALUE self)
{
    GET_ENUM(MsgProps, State, dpiMessageState);
}

static VALUE queue_alloc(VALUE klass)
{
    Queue_t *queue;
    return TypedData_Make_Struct(klass, Queue_t, &queue_data_type, queue);
}

static VALUE queue_initialize(int argc, VALUE *argv, VALUE self)
{
    Queue_t *queue = To_Queue(self);
    VALUE conn, name, payload_type;
    rbOraDBConn *dconn;
    dpiObjectType *objtype = NULL;

    rb_scan_args(argc, argv, "21", &conn, &name, &payload_type);
    dconn = rboradb_get_dconn(conn);
    ExportString(name);
    if (!NIL_P(payload_type)) {
        objtype = rboradb_to_dpiObjectType(payload_type);
    }
    RBORADB_INIT(queue, dconn);
    queue->payload_objtype = objtype;
    if (queue->payload_objtype) {
        dpiObjectType_addRef(queue->payload_objtype);
    }
    if (dpiConn_newQueue(dconn->handle, RSTRING_PTR(name), RSTRING_LEN(name), objtype, &queue->handle) != DPI_SUCCESS) {
        rboradb_raise_error(dconn->ctxt);
    }
    return Qnil;
}

static VALUE msg_props_new(dpiMsgProps *handle, rbOraDBConn *dconn, dpiObjectType *objtype)
{
    MsgProps_t *props;
    VALUE obj = TypedData_Make_Struct(cMsgProps, MsgProps_t, &enq_options_data_type, props);

    RBORADB_INIT(props, dconn);
    props->handle = handle;
    props->payload_objtype = objtype;
    if (props->payload_objtype) {
        dpiObjectType_addRef(props->payload_objtype);
    }
    return obj;
}

static VALUE queue_deq_many(VALUE self, VALUE maxnum)
{
    Queue_t *queue = To_Queue(self);
    uint32_t idx, num_props = NUM2UINT(maxnum);
    VALUE tmp_buf;
    dpiMsgProps **handles = RB_ALLOCV_N(dpiMsgProps *, tmp_buf, num_props);
    VALUE ary;

    if (rbOraDBQueue_deqMany(queue->dconn->handle, queue->handle, &num_props, handles) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(queue);
    }
    ary = rb_ary_new_capa(num_props);
    for (idx = 0; idx < num_props; idx++) {
        rb_ary_push(ary, msg_props_new(handles[idx], queue->dconn, queue->payload_objtype));
    }
    RB_ALLOCV_END(tmp_buf);
    return ary;
}

static VALUE queue_deq_one(VALUE self)
{
    Queue_t *queue = To_Queue(self);
    dpiMsgProps *handle;

    if (rbOraDBQueue_deqOne(queue->dconn->handle, queue->handle, &handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(queue);
    }
    return msg_props_new(handle, queue->dconn, queue->payload_objtype);
}

static VALUE queue_enq_many(VALUE self, VALUE props)
{
    Queue_t *queue = To_Queue(self);
    uint32_t idx, num_props;
    VALUE tmp_buf;
    dpiMsgProps **handles;

    Check_Type(props, T_ARRAY);
    num_props = RARRAY_LEN(props);
    handles = RB_ALLOCV_N(dpiMsgProps *, tmp_buf, num_props);
    for (idx = 0; idx < num_props; idx++) {
        handles[idx] = To_MsgProps(props)->handle;
    }
    if (rbOraDBQueue_enqMany(queue->dconn->handle, queue->handle, num_props, handles) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(queue);
    }
    RB_ALLOCV_END(tmp_buf);
    return Qnil;
}

static VALUE queue_enq_one(VALUE self, VALUE props)
{
    Queue_t *queue = To_Queue(self);
    MsgProps_t *mp = To_MsgProps(props);

    if (rbOraDBQueue_enqOne(queue->dconn->handle, queue->handle, mp->handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(queue);
    }
    return Qnil;
}

static VALUE deqopts_new(dpiDeqOptions *handle, rbOraDBConn *dconn)
{
    DeqOptions_t *opts;
    VALUE obj = TypedData_Make_Struct(cDeqOptions, DeqOptions_t, &deqopts_data_type, opts);

    RBORADB_SET(opts, dpiDeqOptions, handle, dconn);
    return obj;
}

static VALUE queue_deq_options(VALUE self)
{
    Queue_t *queue = To_Queue(self);
    dpiDeqOptions *handle;

    if (dpiQueue_getDeqOptions(queue->handle, &handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(queue);
    }
    return deqopts_new(handle, queue->dconn);
}

static VALUE enq_options_new(dpiEnqOptions *handle, rbOraDBConn *dconn)
{
    EnqOptions_t *opts;
    VALUE obj = TypedData_Make_Struct(cEnqOptions, EnqOptions_t, &enq_options_data_type, opts);

    RBORADB_SET(opts, dpiEnqOptions, handle, dconn);
    return obj;
}

static VALUE queue_enq_options(VALUE self)
{
    Queue_t *queue = To_Queue(self);
    dpiEnqOptions *handle;

    if (dpiQueue_getEnqOptions(queue->handle, &handle) != DPI_SUCCESS) {
        RBORADB_RAISE_ERROR(queue);
    }
    return enq_options_new(handle, queue->dconn);
}

void rboradb_aq_init(VALUE mOracleDB)
{
    cDeqOptions = rb_define_class_under(mOracleDB, "DeqOptions", rb_cObject);
    rb_define_alloc_func(cDeqOptions, deqopts_alloc);
    rb_define_method(cDeqOptions, "initialize", deqopts_initialize, 1);
    rb_define_private_method(cDeqOptions, "initialize_copy", rboradb_notimplement, -1);

    rb_define_method(cDeqOptions, "condition", deqopts_condition, 0);
    rb_define_method(cDeqOptions, "condition=", deqopts_set_condition, 1);
    rb_define_method(cDeqOptions, "consumer_name", deqopts_consumer_name, 0);
    rb_define_method(cDeqOptions, "consumer_name=", deqopts_set_consumer_name, 1);
    rb_define_method(cDeqOptions, "correlation", deqopts_correlation, 0);
    rb_define_method(cDeqOptions, "correlation=", deqopts_set_correlation, 1);
    rb_define_method(cDeqOptions, "delivery_mode=", deqopts_set_delivery_mode, 1);
    rb_define_method(cDeqOptions, "mode", deqopts_mode, 0);
    rb_define_method(cDeqOptions, "mode=", deqopts_set_mode, 1);
    rb_define_method(cDeqOptions, "msg_id", deqopts_msg_id, 0);
    rb_define_method(cDeqOptions, "msg_id=", deqopts_set_msg_id, 1);
    rb_define_method(cDeqOptions, "navigation", deqopts_navigation, 0);
    rb_define_method(cDeqOptions, "navigation=", deqopts_set_navigation, 1);
    rb_define_method(cDeqOptions, "transformation", deqopts_transformation, 0);
    rb_define_method(cDeqOptions, "transformation=", deqopts_set_transformation, 1);
    rb_define_method(cDeqOptions, "visibility", deqopts_visibility, 0);
    rb_define_method(cDeqOptions, "visibility=", deqopts_set_visibility, 1);
    rb_define_method(cDeqOptions, "wait", deqopts_wait, 0);
    rb_define_method(cDeqOptions, "wait=", deqopts_set_wait, 1);

    cEnqOptions = rb_define_class_under(mOracleDB, "EnqOptions", rb_cObject);
    rb_define_alloc_func(cEnqOptions, enq_options_alloc);
    rb_define_method(cEnqOptions, "initialize", enq_options_initialize, 1);
    rb_define_private_method(cEnqOptions, "initialize_copy", rboradb_notimplement, -1);
    rb_define_method(cEnqOptions, "delivery_mode=", enqopts_set_delivery_mode, 1);
    rb_define_method(cEnqOptions, "transformation", enqopts_transformation, 0);
    rb_define_method(cEnqOptions, "transformation=", enqopts_set_transformation, 1);
    rb_define_method(cEnqOptions, "visibility", enqopts_visibility, 0);
    rb_define_method(cEnqOptions, "visibility=", enqopts_set_visibility, 1);

    cMsgProps = rb_define_class_under(mOracleDB, "MsgProps", rb_cObject);
    rb_define_alloc_func(cMsgProps, msg_props_alloc);
    rb_define_method(cMsgProps, "initialize", msg_props_initialize, 1);
    rb_define_private_method(cMsgProps, "initialize_copy", rboradb_notimplement, -1);
    rb_define_method(cMsgProps, "num_attempts", msgprops_num_attempts, 0);
    rb_define_method(cMsgProps, "correlation", msgprops_correlation, 0);
    rb_define_method(cMsgProps, "correlation=", msgprops_set_correlation, 1);
    rb_define_method(cMsgProps, "delay", msgprops_delay, 0);
    rb_define_method(cMsgProps, "delay=", msgprops_set_delay, 1);
    rb_define_method(cMsgProps, "delivery_mode", msgprops_delivery_mode, 0);
    rb_define_method(cMsgProps, "enq_time", msgprops_enq_time, 0);
    rb_define_method(cMsgProps, "exception_q", msgprops_exception_q, 0);
    rb_define_method(cMsgProps, "exception_q=", msgprops_set_exception_q, 1);
    rb_define_method(cMsgProps, "expiration", msgprops_expiration, 0);
    rb_define_method(cMsgProps, "expiration=", msgprops_set_expiration, 1);
    rb_define_method(cMsgProps, "msg_id", msgprops_msg_id, 0);
    rb_define_method(cMsgProps, "original_msg_id", msgprops_original_msg_id, 0);
    rb_define_method(cMsgProps, "original_msg_id=", msgprops_set_original_msg_id, 1);
    rb_define_method(cMsgProps, "payload", msgprops_payload, 0);
    rb_define_method(cMsgProps, "payload=", msgprops_set_payload, 1);
    rb_define_method(cMsgProps, "priority", msgprops_priority, 0);
    rb_define_method(cMsgProps, "priority=", msgprops_set_priority, 1);
    rb_define_method(cMsgProps, "state", msgprops_state, 0);

    cQueue = rb_define_class_under(mOracleDB, "Queue", rb_cObject);
    rb_define_alloc_func(cQueue, queue_alloc);
    rb_define_method(cQueue, "initialize", queue_initialize, -1);
    rb_define_private_method(cQueue, "initialize_copy", rboradb_notimplement, -1);
    rb_define_method(cQueue, "deq_many", queue_deq_many, 1);
    rb_define_method(cQueue, "deq_one", queue_deq_one, 0);
    rb_define_method(cQueue, "enq_many", queue_enq_many, 1);
    rb_define_method(cQueue, "enq_one", queue_enq_one, 1);
    rb_define_method(cQueue, "deq_options", queue_deq_options, 0);
    rb_define_method(cQueue, "enq_options", queue_enq_options, 0);
}
