require "bundler/setup"
require "oracledb"

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end
end

$main_username = ENV["ODPIC_TEST_MAIN_USER"] || "odpic"
$main_password = ENV["ODPIC_TEST_MAIN_PASSWORD"] || "welcome"
$connect_string = ENV["ODPIC_TEST_CONNECT_STRING"] || "localhost/orclpdb"

$ctxt = OracleDB::Context.new

def connect
  OracleDB::Conn.new($ctxt, $main_username, $main_password, $connect_string)
end
