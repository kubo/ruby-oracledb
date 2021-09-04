module OracleDB
  class ObjectAttr
    def name
      (@info ||= __info).name
    end

    def type_info
      (@info ||= __info).type_info
    end
  end

  class ObjectType
    def schema
      (@info ||= __info).schema
    end

    def name
      (@info ||= __info).name
    end

    def is_collection
      (@info ||= __info).is_collection
    end

    def element_type_info
      (@info ||= __info).element_type_info
    end

    def num_attributes
      (@info ||= __info).num_attributes
    end

    def attributes
      @attributes ||= __attributes
    end
  end
end
