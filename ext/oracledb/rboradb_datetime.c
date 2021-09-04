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
#include <rboradb.h>

#define To_Timestamp(obj) ((dpiTimestamp *)rb_check_typeddata((obj), &timestamp_data_type))
#define To_IntervalDS(obj) ((dpiIntervalDS *)rb_check_typeddata((obj), &interval_ds_data_type))
#define To_IntervalYM(obj) ((dpiIntervalYM *)rb_check_typeddata((obj), &interval_ym_data_type))

static VALUE cTimestamp;
static VALUE cIntervalDS;
static VALUE cIntervalYM;

static const struct rb_data_type_struct timestamp_data_type = {
    "OracleDB::Timestamp",
    {NULL, NULL,},
    NULL, NULL,
};

static const struct rb_data_type_struct interval_ds_data_type = {
    "OracleDB::IntervalDS",
    {NULL, NULL,},
    NULL, NULL,
};

static const struct rb_data_type_struct interval_ym_data_type = {
    "OracleDB::IntervalYM",
    {NULL, NULL,},
    NULL, NULL,
};

static VALUE timestamp_alloc(VALUE klass)
{
    dpiTimestamp *val;
    return TypedData_Make_Struct(klass, dpiTimestamp, &timestamp_data_type, val);
}

static VALUE timestamp_initialize(int argc, VALUE *argv, VALUE self)
{
    dpiTimestamp *val = To_Timestamp(self);
    VALUE year, month, day, hour, minute, second, fsecond, tz_hour_offset, tz_minute_offset;

    rb_scan_args(argc, argv, "09", &year, &month, &day, &hour, &minute, &second, &fsecond, &tz_hour_offset, &tz_minute_offset);
    val->year = NIL_P(year) ? 1 : NUM2INT(year);
    val->month = NIL_P(month) ? 1 : NUM2UINT(month);
    val->day = NIL_P(day) ? 1 : NUM2UINT(day);
    val->hour = NIL_P(hour) ? 0 : NUM2UINT(hour);
    val->minute = NIL_P(minute) ? 0 : NUM2UINT(minute);
    val->second = NIL_P(second) ? 0 : NUM2UINT(second);
    val->fsecond = NIL_P(fsecond) ? 0 : NUM2UINT(fsecond);
    val->tzHourOffset = NIL_P(tz_hour_offset) ? 0 : NUM2INT(tz_hour_offset);
    val->tzMinuteOffset = NIL_P(tz_minute_offset) ? 0 : NUM2INT(tz_minute_offset);
    return Qnil;
}

static VALUE timestamp_to_a(VALUE self)
{
    dpiTimestamp *val = To_Timestamp(self);
    VALUE ary = rb_ary_new_capa(9);
    rb_ary_push(ary, INT2FIX(val->year));
    rb_ary_push(ary, INT2FIX(val->month));
    rb_ary_push(ary, INT2FIX(val->day));
    rb_ary_push(ary, INT2FIX(val->hour));
    rb_ary_push(ary, INT2FIX(val->minute));
    rb_ary_push(ary, INT2FIX(val->second));
    rb_ary_push(ary, UINT2NUM(val->fsecond));
    rb_ary_push(ary, INT2FIX(val->tzHourOffset));
    rb_ary_push(ary, INT2FIX(val->tzMinuteOffset));
    return ary;
}

static VALUE timestamp_to_s(VALUE self)
{
    dpiTimestamp *val = To_Timestamp(self);
    char sign = (val->tzHourOffset >= 0 && val->tzMinuteOffset >= 0) ? '+' : '-';
    return rb_sprintf("%04d-%02u-%02u %02u:%02u:%02u.%09u %c%02d:%02d",
                      val->year, val->month, val->day,
                      val->hour, val->minute, val->second,
                      val->fsecond, sign, abs(val->tzHourOffset), abs(val->tzMinuteOffset));
}

static VALUE timestamp_year(VALUE self)
{
    dpiTimestamp *val = To_Timestamp(self);
    return INT2FIX(val->year);
}

static VALUE timestamp_month(VALUE self)
{
    dpiTimestamp *val = To_Timestamp(self);
    return INT2FIX(val->month);
}

static VALUE timestamp_day(VALUE self)
{
    dpiTimestamp *val = To_Timestamp(self);
    return INT2FIX(val->day);
}

static VALUE timestamp_hour(VALUE self)
{
    dpiTimestamp *val = To_Timestamp(self);
    return INT2FIX(val->hour);
}

static VALUE timestamp_minute(VALUE self)
{
    dpiTimestamp *val = To_Timestamp(self);
    return INT2FIX(val->minute);
}

static VALUE timestamp_second(VALUE self)
{
    dpiTimestamp *val = To_Timestamp(self);
    return INT2FIX(val->second);
}

static VALUE timestamp_fsecond(VALUE self)
{
    dpiTimestamp *val = To_Timestamp(self);
    return UINT2NUM(val->fsecond);
}

static VALUE timestamp_tz_hour_offset(VALUE self)
{
    dpiTimestamp *val = To_Timestamp(self);
    return INT2FIX(val->tzHourOffset);
}

static VALUE timestamp_tz_minute_offset(VALUE self)
{
    dpiTimestamp *val = To_Timestamp(self);
    return INT2FIX(val->tzMinuteOffset);
}

static VALUE interval_ds_alloc(VALUE klass)
{
    dpiIntervalDS *val;
    return TypedData_Make_Struct(klass, dpiIntervalDS, &interval_ds_data_type, val);
}

static VALUE interval_ds_initialize(int argc, VALUE *argv, VALUE self)
{
    dpiIntervalDS *val = To_IntervalDS(self);
    VALUE days, hours, minutes, seconds, fseconds;

    rb_scan_args(argc, argv, "05", &days, &hours, &minutes, &seconds, &fseconds);
    val->days = NIL_P(days) ? 0 : NUM2INT(days);
    val->hours = NIL_P(hours) ? 0 : NUM2INT(hours);
    val->minutes = NIL_P(minutes) ? 0 : NUM2INT(minutes);
    val->seconds = NIL_P(seconds) ? 0 : NUM2INT(seconds);
    val->fseconds = NIL_P(fseconds) ? 0 : NUM2INT(fseconds);
    return Qnil;
}

static VALUE interval_ds_to_a(VALUE self)
{
    dpiIntervalDS *val = To_IntervalDS(self);
    VALUE ary = rb_ary_new_capa(5);
    rb_ary_push(ary, INT2NUM(val->days));
    rb_ary_push(ary, INT2NUM(val->hours));
    rb_ary_push(ary, INT2NUM(val->minutes));
    rb_ary_push(ary, INT2NUM(val->seconds));
    rb_ary_push(ary, INT2NUM(val->fseconds));
    return ary;
}

static VALUE interval_ds_to_s(VALUE self)
{
    dpiIntervalDS *val = To_IntervalDS(self);
    char sign = (val->days >= 0 && val->hours >= 0 && val->minutes >= 0 && val->seconds >= 0 && val->fseconds >= 0) ? '+' : '-';
    return rb_sprintf("%c%0d %02d:%02d:%02d.%09d",
                      sign, abs(val->days), abs(val->hours), abs(val->minutes), abs(val->seconds), abs(val->fseconds));
}

static VALUE interval_ds_days(VALUE self)
{
    dpiIntervalDS *val = To_IntervalDS(self);
    return INT2NUM(val->days);
}

static VALUE interval_ds_hours(VALUE self)
{
    dpiIntervalDS *val = To_IntervalDS(self);
    return INT2NUM(val->hours);
}

static VALUE interval_ds_minutes(VALUE self)
{
    dpiIntervalDS *val = To_IntervalDS(self);
    return INT2NUM(val->minutes);
}

static VALUE interval_ds_seconds(VALUE self)
{
    dpiIntervalDS *val = To_IntervalDS(self);
    return INT2NUM(val->seconds);
}

static VALUE interval_ds_fseconds(VALUE self)
{
    dpiIntervalDS *val = To_IntervalDS(self);
    return INT2NUM(val->fseconds);
}

static VALUE interval_ym_alloc(VALUE klass)
{
    dpiIntervalYM *val;
    return TypedData_Make_Struct(klass, dpiIntervalYM, &interval_ym_data_type, val);
}

static VALUE interval_ym_initialize(int argc, VALUE *argv, VALUE self)
{
    dpiIntervalYM *val = To_IntervalYM(self);
    VALUE years, months;

    rb_scan_args(argc, argv, "02", &years, &months);
    val->years = NIL_P(years) ? 0 : NUM2INT(years);
    val->months = NIL_P(months) ? 0 : NUM2INT(months);
    return Qnil;
}

static VALUE interval_ym_to_a(VALUE self)
{
    dpiIntervalYM *val = To_IntervalYM(self);
    VALUE ary = rb_ary_new_capa(2);
    rb_ary_push(ary, INT2NUM(val->years));
    rb_ary_push(ary, INT2NUM(val->months));
    return ary;
}

static VALUE interval_ym_to_s(VALUE self)
{
    dpiIntervalYM *val = To_IntervalYM(self);
    char sign = (val->years >= 0 && val->months >= 0) ? '+' : '-';
    return rb_sprintf("%c%9d-%02d", sign, abs(val->years), abs(val->months));
}

static VALUE interval_ym_years(VALUE self)
{
    dpiIntervalYM *val = To_IntervalYM(self);
    return INT2NUM(val->years);
}

static VALUE interval_ym_months(VALUE self)
{
    dpiIntervalYM *val = To_IntervalYM(self);
    return INT2NUM(val->months);
}

void rboradb_datetime_init(VALUE mOracleDB)
{
    cTimestamp = rb_define_class_under(mOracleDB, "Timestamp", rb_cObject);
    rb_define_alloc_func(cTimestamp, timestamp_alloc);
    rb_define_method(cTimestamp, "initialize", timestamp_initialize, -1);
    rb_define_method(cTimestamp, "to_a", timestamp_to_a, 0);
    rb_define_method(cTimestamp, "to_s", timestamp_to_s, 0);
    rb_define_method(cTimestamp, "year", timestamp_year, 0);
    rb_define_method(cTimestamp, "month", timestamp_month, 0);
    rb_define_method(cTimestamp, "day", timestamp_day, 0);
    rb_define_method(cTimestamp, "hour", timestamp_hour, 0);
    rb_define_method(cTimestamp, "minute", timestamp_minute, 0);
    rb_define_method(cTimestamp, "second", timestamp_second, 0);
    rb_define_method(cTimestamp, "fsecond", timestamp_fsecond, 0);
    rb_define_method(cTimestamp, "tz_hour_offset", timestamp_tz_hour_offset, 0);
    rb_define_method(cTimestamp, "tz_minute_offset", timestamp_tz_minute_offset, 0);

    cIntervalDS = rb_define_class_under(mOracleDB, "IntervalDS", rb_cObject);
    rb_define_alloc_func(cIntervalDS, interval_ds_alloc);
    rb_define_method(cIntervalDS, "initialize", interval_ds_initialize, -1);
    rb_define_method(cIntervalDS, "to_a", interval_ds_to_a, 0);
    rb_define_method(cIntervalDS, "to_s", interval_ds_to_s, 0);
    rb_define_method(cIntervalDS, "days", interval_ds_days, 0);
    rb_define_method(cIntervalDS, "hours", interval_ds_hours, 0);
    rb_define_method(cIntervalDS, "minutes", interval_ds_minutes, 0);
    rb_define_method(cIntervalDS, "seconds", interval_ds_seconds, 0);
    rb_define_method(cIntervalDS, "fseconds", interval_ds_fseconds, 0);

    cIntervalYM = rb_define_class_under(mOracleDB, "IntervalYM", rb_cObject);
    rb_define_alloc_func(cIntervalYM, interval_ym_alloc);
    rb_define_method(cIntervalYM, "initialize", interval_ym_initialize, -1);
    rb_define_method(cIntervalYM, "to_a", interval_ym_to_a, 0);
    rb_define_method(cIntervalYM, "to_s", interval_ym_to_s, 0);
    rb_define_method(cIntervalYM, "years", interval_ym_years, 0);
    rb_define_method(cIntervalYM, "months", interval_ym_months, 0);
}

VALUE rboradb_from_dpiTimestamp(const dpiTimestamp *val)
{
    dpiTimestamp *v;
    VALUE obj = TypedData_Make_Struct(cTimestamp, dpiTimestamp, &timestamp_data_type, v);
    *v = *val;
    return obj;
}

VALUE rboradb_from_dpiIntervalDS(const dpiIntervalDS *val)
{
    dpiIntervalDS *v;
    VALUE obj = TypedData_Make_Struct(cIntervalDS, dpiIntervalDS, &interval_ds_data_type, v);
    *v = *val;
    return obj;
}

VALUE rboradb_from_dpiIntervalYM(const dpiIntervalYM *val)
{
    dpiIntervalYM *v;
    VALUE obj = TypedData_Make_Struct(cIntervalYM, dpiIntervalYM, &interval_ym_data_type, v);
    *v = *val;
    return obj;
}

dpiTimestamp *rboradb_to_dpiTimestamp(VALUE obj)
{
    return To_Timestamp(obj);
}

dpiIntervalDS *rboradb_to_dpiIntervalDS(VALUE obj)
{
    return To_IntervalDS(obj);
}

dpiIntervalYM *rboradb_to_dpiIntervalYM(VALUE obj)
{
    return To_IntervalYM(obj);
}

int rboradb_is_Timestamp(VALUE obj)
{
    return rb_typeddata_is_kind_of(obj, &timestamp_data_type);
}

int rboradb_is_IntervalDS(VALUE obj)
{
    return rb_typeddata_is_kind_of(obj, &interval_ds_data_type);
}

int rboradb_is_IntervalYM(VALUE obj)
{
    return rb_typeddata_is_kind_of(obj, &interval_ym_data_type);
}
