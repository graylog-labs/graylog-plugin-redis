# Redis Plugin for Graylog

[![Github Downloads](https://img.shields.io/github/downloads/graylog-labs/graylog-plugin-redis/total.svg)](https://github.com/graylog-labs/graylog-plugin-redis/releases)
[![GitHub Release](https://img.shields.io/github/release/graylog-labs/graylog-plugin-redis.svg)](https://github.com/graylog-labs/graylog-plugin-redis/releases)
[![Build Status](https://travis-ci.org/graylog-labs/graylog-plugin-redis.svg?branch=master)](https://travis-ci.org/graylog-labs/graylog-plugin-redis)

**Required Graylog version:** 2.2.0 and later

This plugin provides inputs and outputs for the [Redis protocol](http://redis.io/) in Graylog which can be used to consume data from Redis.


## Installation

[Download the plugin](https://github.com/graylog-labs/graylog-plugin-redis/releases) and place the JAR file in your Graylog plugin directory.
By default the plugin directory is the `plugins/` directory relative to your Graylog installation directory and can be configured in your `graylog.conf` file.

Restart Graylog and you are done.


## Build

This project is using Maven and requires Java 8 or higher.

You can build the plugin (JAR) with `mvn package`.

DEB and RPM packages can be build with `mvn jdeb:jdeb` and `mvn rpm:rpm` respectively.


## Plugin Release

In order to release a new version of the plugin, run the following commands:

```
$ mvn release:prepare
$ mvn release:perform
```

This sets the version numbers, creates a tag and pushes to GitHub.

Travis CI will build the release artifacts and upload to GitHub automatically.


## License

Copyright (c) 2016-2017 Graylog, Inc.

This library is licensed under the GNU General Public License, Version 3.0.

See https://www.gnu.org/licenses/gpl-3.0.html or the LICENSE.txt file in this repository for the full license text.
