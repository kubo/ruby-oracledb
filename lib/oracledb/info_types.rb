module OracleDB

  class DataTypeInfo
    def oracle_type
      @oracle_type
    end

    def default_native_type
      @default_native_type
    end

    def oci_type_code
      @oci_type_code
    end

    def db_size_in_bytes
      @db_size_in_bytes
    end

    def client_size_in_bytes
      @client_size_in_bytes
    end

    def size_in_chars
      @size_in_chars
    end

    def precision
      @precision
    end

    def scale
      @scale
    end

    def fs_precision
      @fs_precision
    end

    def object_type
      @object_type
    end

    def to_s
      case @oracle_type
      when :varchar
        "VARCHAR2(#{@db_size_in_bytes})"
      when :nvarchar
        "NVARCHAR(#{@size_in_chars})"
      when :char
        "CHAR(#{@db_size_in_bytes})"
      when :nchar
        "NCHAR(#{@size_in_chars})"
      when :rowid
        "ROWID"
      when :raw
        "RAW(#{@db_size_in_bytes})"
      when :native_float
        "BINARY_FLOAT"
      when :native_double
        "BINARY_DOUBLE"
      when :number
        if @precision != 0 && @scale == -127
          if @precision == 126
            "FLOAT"
          else
            "FLOAT(#{@precision})"
          end
        elsif @precision == 0
          "NUMBER"
        elsif @scale == 0
          "NUMBER(#{@precision})"
        else
          "NUMBER(#{@precision}, #{@scale})"
        end
      when :date
        "DATE"
      when :timestamp
        if @fs_precision == 6
          "TIMESTAMP"
        else
          "TIMESTAMP(#{@fs_precision})"
        end
      when :timestamp_tz
        if @fs_precision == 6
          "TIMESTAMP WITH TIME ZONE"
        else
          "TIMESTAMP(#{@fs_precision}) WITH TIME ZONE"
        end
      when :timestamp_ltz
        if @fs_precision == 6
          "TIMESTAMP WITH LOCAL TIME ZONE"
        else
          "TIMESTAMP(#{@fs_precision}) WITH LOCAL TIME ZONE"
        end
      when :interval_ds
        if @precision == 2 && @fs_precision == 6
          "INTERVAL DAY TO SECOND"
        else
          "INTERVAL DAY(#{@precision}) TO SECOND(#{@fs_precision})"
        end
      when :interval_ym
        if @precision == 2
          "INTERVAL YEAR TO MONTH"
        else
          "INTERVAL YEAR(#{@precision}) TO MONTH"
        end
      when :clob
        "CLOB"
      when :nclob
        "NCLOB"
      when :blob
        "BLOB"
      when :bfile
        "BFILE"
      when :stmt
        "REF CURSOR"
      when :boolean
        "BOOLEAN"
      when :object
        if @object_type
          "#{@object_type.schema}.#{@object_type.name}"
        else
          "OBJECT"
        end
      when :long_varchar
        "LONG"
      when :long_raw
        "LONG RAW"
      when :json
        "JSON"
      else
        @oracle_type.to_s
      end
    end
  end

  class ObjectAttrInfo
    def name
      @name
    end

    def type_info
      @type_info
    end

    def to_s
      "#{@name} #{@type_info.to_s}"
    end
  end

  class ObjectTypeInfo
    def schema
      @schema
    end

    def name
      @name
    end

    def is_collection
      @is_collection
    end

    def element_type_info
      @element_type_info
    end

    def num_attributes
      @num_attributes
    end

    def to_s
      "#{@schema}.#{@name}"
    end
  end

  class QueryInfo
    def name
      @name
    end

    def type_info
      @type_info
    end

    def null_ok
      @null_ok
    end

    def to_s
      if @null_ok
        "#{self.name} #{self.type_info}"
      else
        "#{self.name} #{self.type_info} NOT NULL"
      end
    end
  end

  class StmtInfo
    def is_query
      @is_query
    end

    def is_plsql
      @is_plsql
    end

    def is_ddl
      @is_ddl
    end

    def is_dml
      @is_dml
    end

    def statement_type
      @statement_type
    end

    def is_returning
      @is_returning
    end

    def to_s
      @statement_type.to_s
    end
  end

  class VersionInfo
    include Comparable

    def initialize(version, release=0, update=0, port_release=0, port_update=0)
      @version = version
      @release = release
      @update = update
      @port_release = port_release
      @port_update = port_update
    end

    def version
      @version
    end

    def release
      @release
    end

    def update
      @update
    end

    def port_release
      @port_release
    end

    def port_update
      @port_update
    end

    def to_s
      "#{@version}.#{@release}.#{@update}.#{@port_release}.#{@port_update}"
    end

    def inspect
      "#<#{self.class.to_s}: #{self.to_s}>"
    end

    def <=>(other)
      result = self.version <=> other.version
      result = self.release <=> other.release if result == 0
      result = self.update <=> other.update if result == 0
      result = self.port_release <=> other.port_release if result == 0
      result = self.port_update <=> other.port_update if result == 0
      result
    end
  end
end
