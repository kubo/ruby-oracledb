require 'mkmf'
require 'pathname'
require 'yaml'

ext_src_dir = Pathname(__FILE__).dirname
ext_src_dir.glob("_gen_*") do |file|
  file.delete
end

class FuncDef
  attr_reader :orig_name
  attr_reader :name
  attr_reader :args
  attr_reader :args_dcl
  attr_reader :cancel_cb

  def initialize(key, val)
    @orig_name = key
    @name = key.sub(/^dpi/, 'rbOraDB')
    @args = val["args"].collect do |arg|
      ArgDef.new(arg)
    end
    breakable = (not val.has_key?("break")) || val["break"]
    @args_dcl = if @args[0].dcl == 'dpiConn *conn' || !breakable
                  ""
                else
                  'dpiConn *conn, '
                end + @args.collect {|arg| arg.dcl}.join(', ')
    @cancel_cb = if breakable
                   "(void (*)(void *))dpiConn_breakExecution, conn"
                 else
                   "NULL, NULL"
                 end
  end

  class ArgDef
    attr_reader :dcl
    attr_reader :name

    def initialize(arg)
      /(\w+)\s*$/ =~ arg
      @dcl = arg
      @name = $1
    end
  end
end

class EnumDef
  attr_reader :name
  attr_reader :bitflag
  attr_reader :to_dpi
  attr_reader :from_dpi
  attr_reader :values
  def initialize(key, val)
    @name = key
    @bitflag = val["bitflag"]
    prefix = val["prefix"]
    @to_dpi = @from_dpi = false
    case val["dir"]
    when "to_dpi"
      @to_dpi = true
    when "from_dpi"
      @from_dpi = true
    when "both"
      @to_dpi = true
      @from_dpi = true
    end
    @values = val["values"].map do |val|
      [prefix + val.upcase, val]
    end
  end
end

func_defs = []
YAML.load(open(ext_src_dir / "dpi_funcs.yml")).each do |key, val|
  if val["args"]
    func_defs << FuncDef.new(key, val)
  end
end

enum_defs = []
YAML.load(open(ext_src_dir / "dpi_enums.yml")).each do |key, val|
  enum_defs << EnumDef.new(key, val)
end
enum_defs.sort! { |a, b| a.name <=> b.name }

open("_gen_dpi_funcs.h", 'w') do |f|
  f.print(<<EOS)
#ifndef DPI_FUNCS_H
#define DPI_FUNCS_H 1
/* This file was created by extconf.rb */

EOS
  func_defs.each do |func|
    f.print(<<EOS)
int #{func.name}(#{func.args_dcl});
EOS
  end
  f.print(<<EOS)
#endif
EOS
end

open("_gen_dpi_funcs.c", 'w') do |f|
  f.print(<<EOS)
/* This file was created by extconf.rb */
#include <ruby.h>
#include <ruby/thread.h>
#include <dpi.h>
#include "_gen_dpi_funcs.h"
EOS
  func_defs.each do |func|
    f.print(<<EOS)

/* #{func.orig_name} */
typedef struct {
EOS
    func.args.each do |arg|
      f.print(<<EOS)
    #{arg.dcl};
EOS
    end
    f.print(<<EOS)
} #{func.orig_name}_arg_t;

static void *#{func.orig_name}_cb(void *data)
{
    #{func.orig_name}_arg_t *arg = (#{func.orig_name}_arg_t *)data;
    int rv = #{func.orig_name}(#{func.args.collect {|arg| 'arg->' + arg.name}.join(', ')});
    return (void*)(size_t)rv;
}

int #{func.name}(#{func.args_dcl})
{
    #{func.orig_name}_arg_t arg;
    void *rv;
EOS
    func.args.each do |arg|
      f.print(<<EOS)
    arg.#{arg.name} = #{arg.name};
EOS
    end
    f.print(<<EOS)
    rv = rb_thread_call_without_gvl(#{func.orig_name}_cb, &arg, #{func.cancel_cb});
    return (int)(size_t)rv;
}
EOS
  end
end

open("_gen_dpi_enums.h", 'w') do |f|
  f.print(<<EOS)
#ifndef DPI_ENUMS_H
#define DPI_ENUMS_H 1
/* This file was created by extconf.rb */

typedef enum OciAttrDataType {
    RBORADB_OCI_ATTR_DATA_TYPE_BOOLEAN,
    RBORADB_OCI_ATTR_DATA_TYPE_TEXT,
    RBORADB_OCI_ATTR_DATA_TYPE_UB1,
    RBORADB_OCI_ATTR_DATA_TYPE_UB2,
    RBORADB_OCI_ATTR_DATA_TYPE_UB4,
    RBORADB_OCI_ATTR_DATA_TYPE_UB8,
} OciAttrDataType;

typedef enum OciHandleType {
    RBORADB_OCI_HTYPE_SVCCTX = 3,
    RBORADB_OCI_HTYPE_SERVER = 8,
    RBORADB_OCI_HTYPE_SESSION = 9,
} OciHandleType;

EOS
  enum_defs.each do |enum|
    f.print(<<EOS) if enum.to_dpi
#{enum.name} rboradb_to_#{enum.name}(VALUE obj);
EOS
    f.print(<<EOS) if enum.from_dpi
VALUE rboradb_from_#{enum.name}(#{enum.name} val);
EOS
  end
  f.print(<<EOS)
#endif
EOS
end

open("_gen_dpi_enums.c", 'w') do |f|
  f.print(<<EOS)
/* This file was created by extconf.rb */
#include <ruby.h>
#include <dpi.h>
#include "_gen_dpi_enums.h"

static uint32_t to_bitvalues(VALUE obj, uint32_t (*func)(VALUE))
{
    uint32_t v = 0;
    if (RB_TYPE_P(obj, T_ARRAY)) {
        VALUE *ptr = RARRAY_PTR(obj);
        long i, len = RARRAY_LEN(obj);
        for (i = 0; i < len; i++) {
            v |= func(ptr[i]);
        }
    } else if (!NIL_P(obj)) {
        v = func(obj);
    }
    return v;
}

static VALUE from_bitvalues(uint32_t val, VALUE (*func)(uint32_t))
{
    VALUE ary = rb_ary_new();
    while (val != 0) {
        uint32_t bit = val & -val; // the least significant bit
        rb_ary_push(ary, func(bit));
        val -= bit;
    }
    return ary;
}
EOS
  enum_defs.each do |enum|
    if enum.to_dpi
      if enum.bitflag
        f.print(<<EOS)

static uint32_t to_#{enum.name}(VALUE obj)
EOS
      else
        f.print(<<EOS)

#{enum.name} rboradb_to_#{enum.name}(VALUE obj)
EOS
      end
        f.print(<<EOS)
{
    static VALUE map = Qundef;
    VALUE val;
    if (map == Qundef) {
        map = rb_hash_new();
EOS
        enum.values.each do |val|
        f.print(<<EOS)
        rb_hash_aset(map, ID2SYM(rb_intern("#{val[1]}")), UINT2NUM(#{val[0]}));
EOS
        end
        f.print(<<EOS)
    }
    val = rb_hash_aref(map, obj);
    if (NIL_P(val)) {
        obj = rb_String(rb_inspect(obj));
        rb_raise(rb_eArgError, "unknown #{enum.name}: %.*s", (int)RSTRING_LEN(obj), RSTRING_PTR(obj));
    }
    return NUM2UINT(val);
}
EOS
      if enum.bitflag
        f.print(<<EOS)

#{enum.name} rboradb_to_#{enum.name}(VALUE obj)
{
    return to_bitvalues(obj, to_#{enum.name});
}
EOS
      end
    end
    if enum.from_dpi
      if enum.bitflag
        f.print(<<EOS)

static VALUE from_#{enum.name}(#{enum.name} val)
EOS
      else
        f.print(<<EOS)

VALUE rboradb_from_#{enum.name}(#{enum.name} val)
EOS
      end
      f.print(<<EOS)
{
    ID id;
    switch (val) {
EOS
      enum.values.each do |val|
        f.print(<<EOS)
    case #{val[0]}:
        CONST_ID(id, "#{val[1]}");
        return ID2SYM(id);
EOS
      end
      f.print(<<EOS)
    }
    rb_raise(rb_eArgError, "unknown #{enum.name}: %u", val);
}
EOS
      if enum.bitflag
        f.print(<<EOS)

VALUE rboradb_from_#{enum.name}(#{enum.name} val)
{
    return from_bitvalues(val,from_#{enum.name});
}
EOS
      end
    end
  end
end

$CFLAGS << " -I. -I#{ext_src_dir.parent.parent / "odpi" / "include"}"

$objs = ext_src_dir.glob("*.c").reject do |file|
  file.basename.to_s.start_with?("_gen_")
end.map do |file|
  file.sub_ext(".o").to_s
end

$objs += Pathname(".").glob("_gen_*.c").map do |file|
  file.sub_ext(".o").to_s
end

create_makefile('oracledb/oracledb')
