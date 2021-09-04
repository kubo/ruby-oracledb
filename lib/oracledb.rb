require "oracledb/version"
require "oracledb/oracledb"
require "oracledb/info_types"
require "oracledb/object_types"

module OracleDB
  class Conn
    def prepare_stmt(sql, fetch_array_size: 100, scrollable: false, tag: nil)
      __prepare_stmt(sql, fetch_array_size, scrollable, tag)
    end

    def object_type(name)
      ObjectType.new(self, name)
    end

    def new_deq_options
      DeqOptions.new(self)
    end

    def new_enq_options
      EnqOptions.new(self)
    end

    def new_msg_props
      MsgProps.new(self)
    end

    def new_queue(name, payload=nil)
      Queue.new(self, name, payload)
    end

    def new_tmp_lob(type)
      Lob.new(self, type)
    end

    def subscribe(params)
      Subscr.new(self, params)
    end
  end

  class Lob
    def initialize(conn, type, value = nil)
      __initialize(conn, type)
      set(value)
    end

    def set(value)
      len = __set_from_bytes(value)
      @pos = len
    end

    def read(length = nil)
      return "" if length.zero?
      len = length
      if length.nil?
        len = size - @pos
        return "" if len <= 0
      end
      value, len = __read_bytes(@pos + 1, len)
      if len > 0
        @pos += len
        value
      elsif length.nil?
        ""
      else
        nil
      end
    end

    def seek(offset, whence = IO::SEEK_SET)
      case whence
      when IO::SEEK_SET, :SET
        @pos = offset
      when IO::SEEK_CUR, :CUR
        @pos += offset
      when IO::SEEK_END, :END
        @pos = size + offset
      else
        raise ArgumentError, "Unkown whence value: #{whence}"
      end
      0
    end

    def write(value)
      len = __write_bytes(@pos + 1, value)
      @pos += len
      len
    end
  end

  class Stmt
    def execute(mode: nil)
      @num_query_columns = __execute(mode)
      if @num_query_columns != 0
        @define_vars = Array.new(@num_query_columns)
        if block_given?
          num_fetched = 0
          while row = fetch
            yield(row)
            num_fetched += 1
          end
          return num_fetched
        end
      end
      nil
    end

    def bind(key, var = nil, **kw)
      if !var.is_a?(Var)
        var = Var.new(self, var, array_size: @array_size, **kw)
      end
      if key.is_a? Numeric
        __bind_by_pos(key, var)
      else
        __bind_by_name(key, var)
      end
      @bind_vars ||= {}
      @bind_vars[key] = var
    end

    def define(pos, var = nil, **kw)
      if !var.is_a?(Var)
        var = Var.new(self, var, array_size: @array_size, **kw)
      end
      __define(pos, var)
      @define_vars[pos - 1] = var
    end

    def fetch
      if !@defined && @num_query_columns != 0
        @define_vars.each_index do |idx|
          if @define_vars[idx].nil?
            define(idx + 1, query_info(idx + 1))
          end
        end
      end
      buffer_row_index = __fetch
      buffer_row_index && @define_vars.map do |var|
        var.get(buffer_row_index)
      end
    end

    def info
      @info ||= __info
      @info
    end
  end

  class Var
    def initialize(conn, info = nil, array_size:, oracle_type: nil, native_type: nil, size: nil, size_is_bytes: nil, is_array: nil, object_type: nil, out_filter: nil, in_filter: nil)
      info = info.type_info if info.respond_to? :type_info
      if info
        oracle_type = info.oracle_type if oracle_type.nil?
        native_type = oracle_type != :number ? info.default_native_type : :bytes if native_type.nil?
        size = info.client_size_in_bytes if size.nil?
        size_is_bytes = true if size_is_bytes.nil?
        is_array = false if is_array.nil?
        object_type = info.object_type if object_type.nil?
      end
      __initialize(conn, oracle_type, native_type, array_size, size, size_is_bytes, is_array, object_type, out_filter, in_filter)
    end
  end

  class ObjectType
    def new_object
      Object::new(self)
    end
  end

  class Timestamp
    def inspect
      "#<#{self.class}:#{self.to_s}>"
    rescue
      "#<#{self.class}:ERROR: #{$!.message}>"
    end
  end

  class IntervalDS
    def inspect
      "#<#{self.class}:#{self.to_s}>"
    rescue
      "#<#{self.class}:ERROR: #{$!.message}>"
    end
  end

  class IntervalYM
    def inspect
      "#<#{self.class}:#{self.to_s}>"
    rescue
      "#<#{self.class}:ERROR: #{$!.message}>"
    end
  end

  class Rowid
    def inspect
      "#<#{self.class}:#{self.to_s}>"
    rescue
      "#<#{self.class}:ERROR: #{$!.message}>"
    end
  end
end
