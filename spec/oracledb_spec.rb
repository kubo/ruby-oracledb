RSpec.describe OracleDB do
  it "has a version number" do
    expect(OracleDB::VERSION).not_to be nil
  end
end

RSpec.describe OracleDB::Context do
  it "initializes without parameters" do
    expect($ctxt).to be_a_kind_of OracleDB::Context
  end

  # it "initializes with parameters" do
  #   expect(OracleDB::Context.new(default_driver_name: "dummy", load_error_url: "dummy")).not_to be nil
  # end

  # it "raises an error when unknown parameters are specified" do
  #   expect{OracleDB::Context.new(dummy: "dummy val")}.to raise_error(ArgumentError, "unknown keyword: :dummy")
  #   expect{OracleDB::Context.new(dummy: "dummy val", dummy2: "dummy2 val")}.to raise_error(ArgumentError, "unknown keywords: :dummy, :dummy2")
  # end

  it "gets Oracle client version" do
    expect($ctxt.client_version).to be_a_kind_of OracleDB::VersionInfo
  end
end

RSpec.describe OracleDB::Conn do
  it "connects to the Oracle server" do
    expect(OracleDB::Conn.new($ctxt, $main_username, $main_password, $connect_string)).to be_a_kind_of OracleDB::Conn
  end

  it "raises an exception when password is invalid" do
    expect{OracleDB::Conn.new($ctxt, $main_username,  "  invalid password  ", $connect_string)}.to raise_error(OracleDB::Error, /^ORA-01017:/)
  end
end

RSpec.describe OracleDB::Conn do
  it "connects to the Oracle server" do
    expect(OracleDB::Conn.new($ctxt, $main_username, $main_password, $connect_string)).to be_a_kind_of OracleDB::Conn
  end

  it "raises an exception when password is invalid" do
    expect{OracleDB::Conn.new($ctxt, $main_username,  "  invalid password  ", $connect_string)}.to raise_error(OracleDB::Error, /^ORA-01017:/)
  end
end

RSpec.describe OracleDB::Stmt do
  it "gets bind count and bind names" do
    conn = connect
    stmt = conn.prepare_stmt("select * from tab where dummy = :dummy")
    expect(stmt).to be_a_kind_of OracleDB::Stmt
    expect(stmt.bind_count).to eq 1
    expect(stmt.bind_names).to eq ["DUMMY"]
    info = stmt.info
    expect(info).to be_a_kind_of OracleDB::StmtInfo
    expect(info.is_query).to be true
    expect(info.is_plsql).to be false
    expect(info.is_ddl).to be false
    expect(info.is_dml).to be false
    expect(info.statement_type).to eq :select
    expect(info.is_returning).to be false
  end

  it "binds values by positions" do
    conn = connect
    stmt = conn.prepare_stmt("select * from tab where dummy = :dummy")
    stmt.bind(1, array_size: 1, oracle_type: :varchar, native_type: :bytes, size: 10)
  end

  it "gets query info" do
    conn = connect
    stmt = conn.prepare_stmt("select * from TestDataTypes")
    stmt.execute
    expected_values = [
      ['STRINGCOL', 'VARCHAR2(100)', false],
      ['UNICODECOL', 'NVARCHAR(100)', false],
      ['FIXEDCHARCOL', 'CHAR(100)', false],
      ['FIXEDUNICODECOL', 'NCHAR(100)', false],
      ['RAWCOL', 'RAW(30)', false],
      ['FLOATCOL', 'FLOAT', false],
      ['DOUBLEPRECCOL', 'FLOAT', false],
      ['INTCOL', 'NUMBER(9)', false],
      ['NUMBERCOL', 'NUMBER(9, 2)', false],
      ['DATECOL', 'DATE', false],
      ['TIMESTAMPCOL', 'TIMESTAMP', false],
      ['TIMESTAMPTZCOL', 'TIMESTAMP WITH TIME ZONE', false],
      ['TIMESTAMPLTZCOL', 'TIMESTAMP WITH LOCAL TIME ZONE', false],
      ['INTERVALDSCOL', 'INTERVAL DAY TO SECOND', false],
      ['INTERVALYMCOL', 'INTERVAL YEAR TO MONTH', false],
      ['BINARYFLTCOL', 'BINARY_FLOAT', false],
      ['BINARYDOUBLECOL', 'BINARY_DOUBLE', false],
      ['CLOBCOL', 'CLOB', true],
      ['NCLOBCOL', 'NCLOB', true],
      ['BLOBCOL', 'BLOB', true],
      ['BFILECOL', 'BFILE', true],
      ['LONGCOL', 'LONG', false],
      ['UNCONSTRAINEDCOL', 'NUMBER', false],
      ['SIGNEDINTCOL', 'NUMBER(38)', true],
      ['SUBOBJECTCOL', 'ODPIC.UDT_SUBOBJECT', true],
    ]
    1.upto(stmt.num_query_columns) do |pos|
      query_info = stmt.query_info(pos)
      expected_value = expected_values[pos - 1]
      expect(query_info).to be_a_kind_of OracleDB::QueryInfo
      expect(query_info.name).to eq expected_value[0]
      expect(query_info.type_info.to_s).to eq expected_value[1]
      expect(query_info.null_ok).to eq expected_value[2]
    end
  end

  it "queries timestamps with out_filters" do
    conn = connect
    stmt = conn.prepare_stmt("select to_timestamp('2021-02-03 04:05:06.789012345', 'YYYY-MM-DD HH24:MI:SS.FF9') from dual")

    stmt.execute
    expect(stmt.fetch[0].to_s).to eq '2021-02-03 04:05:06.789012345 +00:00'

    stmt.execute
    stmt.define(1, array_size: 100, oracle_type: :timestamp, native_type: :timestamp, out_filter: :to_s)
    expect(stmt.fetch[0]).to eq '2021-02-03 04:05:06.789012345 +00:00'

    stmt.execute
    stmt.define(1, array_size: 100, oracle_type: :timestamp, native_type: :timestamp, out_filter: ->(x){x.to_s})
    expect(stmt.fetch[0]).to eq '2021-02-03 04:05:06.789012345 +00:00'

    stmt.execute
    stmt.define(1, array_size: 100, oracle_type: :timestamp, native_type: :timestamp, out_filter: proc {|x| x.to_s})
    expect(stmt.fetch[0]).to eq '2021-02-03 04:05:06.789012345 +00:00'

    stmt.execute
    stmt.define(1, array_size: 100, oracle_type: :timestamp, native_type: :timestamp, out_filter: Proc.new {|x| x.to_s})
    expect(stmt.fetch[0]).to eq '2021-02-03 04:05:06.789012345 +00:00'
  end
end

RSpec.describe OracleDB::ObjectType do
  it "gets object type information" do
    conn = connect
    objtype = conn.object_type("UDT_OBJECTDATATYPES")
    expect(objtype).to be_a_kind_of OracleDB::ObjectType
    expect(objtype.schema).to eq $main_username.upcase
    expect(objtype.name).to eq "UDT_OBJECTDATATYPES"
    expect(objtype.is_collection).to be false
    expect(objtype.element_type_info).to be nil
    expect(objtype.num_attributes).to be 14
    expected_attrs = [
      ['STRINGCOL', 'VARCHAR2(60)'],
      ['UNICODECOL', 'NVARCHAR(60)'],
      ['FIXEDCHARCOL', 'CHAR(30)'],
      ['FIXEDUNICODECOL', 'NCHAR(30)'],
      ['RAWCOL', 'RAW(30)'],
      ['INTCOL', 'NUMBER'],
      ['NUMBERCOL', 'NUMBER(9, 2)'],
      ['DATECOL', 'DATE'],
      ['TIMESTAMPCOL', 'TIMESTAMP'],
      ['TIMESTAMPTZCOL', 'TIMESTAMP WITH TIME ZONE'],
      ['TIMESTAMPLTZCOL', 'TIMESTAMP WITH LOCAL TIME ZONE'],
      ['BINARYFLTCOL', 'BINARY_FLOAT'],
      ['BINARYDOUBLECOL', 'BINARY_DOUBLE'],
      ['SIGNEDINTCOL', 'NUMBER(38)'],
    ]
    attrs = objtype.attributes
    expect(attrs.size).to be expected_attrs.size
    attrs.zip(expected_attrs).each do |attr, expected_attr|
      expect(attr).to be_a_kind_of OracleDB::ObjectAttr
      expect(attr.name).to eq expected_attr[0]
      expect(attr.type_info.to_s).to eq expected_attr[1]
    end
  end
end

RSpec.describe OracleDB::Soda::Db do
  it "gets db" do
    db = connect.soda_db
    coll = db.create_collection("ODPIC_COLL_2503", "")
    expect(coll.name).to eq "ODPIC_COLL_2503"
  end
end
