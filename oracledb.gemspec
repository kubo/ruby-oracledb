require_relative 'lib/oracledb/version'

Gem::Specification.new do |spec|
  spec.name          = "oracledb"
  spec.version       = OracleDB::VERSION
  spec.licenses      = ["UPL-1.0", "Apache-2.0"]
  spec.authors       = ["Kubo Takehiro"]
  spec.email         = ["kubo@jiubao.org"]

  spec.summary       = %q{Ruby interface for Oracle based on ODPI-C}
  spec.homepage      = "http://www.rubydoc.info/github/kubo/ruby-oracledb"
  spec.required_ruby_version = Gem::Requirement.new(">= 3.0.0")

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "http://github.com/kubo/ruby-oracledb"
  # spec.metadata["changelog_uri"] = "http://github.com/kubo/ruby-oracledb/blob/master/ChangeLog.md"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files         = Dir.chdir(File.expand_path('..', __FILE__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.files << "odpi/include/dpi.h"
  spec.files << "odpi/embed/dpi.c"
  spec.files += Dir.glob("odpi/src/*.c")
  spec.files += Dir.glob("odpi/src/*.h")
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]
  spec.extensions = %w{ext/oracledb/extconf.rb}
end
