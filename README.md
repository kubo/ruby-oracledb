# Ruby-OracleDB

Ruby-OracleDB is a ruby interface for Oracle Database based on [ODPI-C][].

## Status

Extremely unstable

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'oracledb', git: 'https://github.com/kubo/ruby-oracledb.git', submodules: true
```

And then execute:

    $ bundle install

## Documents

[github pages](https://www.jiubao.org/ruby-oracledb/)

## Supported versions

* Ruby 3.0.0 and later
* Oracle client 11.2 and later
* Oracle server 9.2 and later (depending on Oracle client version)

## License

Ruby-OracleDB is under the terms of:

1. [the Universal Permissive License v 1.0 or at your option, any later version](http://oss.oracle.com/licenses/upl); and/or
2. [the Apache License v 2.0](http://www.apache.org/licenses/LICENSE-2.0). 

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/kubo/oracledb.

[ODPI-C]: https://oracle.github.io/odpi/
