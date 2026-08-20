
[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Maven Central](https://img.shields.io/maven-central/v/io.debezium/debezium-connector-ingres)](https://search.maven.org/#search|ga|1|g:io.debezium+a:debezium-connector-ingres)
[![Build Status](https://img.shields.io/github/actions/workflow/status/debezium/debezium-connector-ingres/maven.yml?branch=main&logo=github&label=Maven%20CI)](https://github.com/debezium/debezium-connector-ingres/actions/workflows/maven.yml?query=branch:main)
[![User chat](https://img.shields.io/badge/chat-users-brightgreen.svg)](https://gitter.im/debezium/user)
[![Developer chat](https://img.shields.io/badge/chat-devs-brightgreen.svg)](https://gitter.im/debezium/dev)
[![Google Group](https://img.shields.io/:mailing%20list-debezium-brightgreen.svg)](https://groups.google.com/forum/#!forum/debezium)
[![Stack Overflow](http://img.shields.io/:stack%20overflow-debezium-brightgreen.svg)](http://stackoverflow.com/questions/tagged/debezium)

Copyright Debezium Authors.
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

# Debezium Connector for Ingres

Debezium is an open source project that provides a low latency data streaming platform for change data capture (CDC).

This repository contains the connector for Actian Ingres.
You are encouraged to explore this connector and test it.
The connector is currently in **incubation** state and is **not production-ready**.
You can expect missing features and breaking changes between releases.

## Known limitations/Issues
* Snapshots are taken without locking database tables. They may lead to inconsistencies or locking issues if other activity is happening on the tables being snapshotted.

## Using the Ingres connector with Kafka Connect

The Ingres connector is designed to work with [Kafka Connect](http://kafka.apache.org/documentation.html#connect) and to be deployed to a Kafka Connect runtime service. The deployed connector will monitor one or more databases and write all change events to Kafka topics, which can be independently consumed by one or more clients. Kafka Connect can be distributed to provide fault tolerance to ensure the connectors are running and continually keeping up with changes in the database.

Kafka Connect can also be run standalone as a single process, although doing so is not tolerant of failures.

## Embedding the Ingres connector

The Ingres connector can also be used as a library without Kafka or Kafka Connect, enabling applications and services to directly connect to a Ingres database and obtain the ordered change events. This approach requires the application to record the progress of the connector so that upon restart the connect can continue where it left off. Therefore, this may be a useful approach for less critical use cases. For production use cases, we highly recommend using this connector with Kafka and Kafka Connect.

## Building and testing the Ingres connector

Building this connector first requires the main [debezium](https://github.com/debezium/debezium) code repository to be built locally using `mvn clean install`.

### Building just the artifacts, without running tests, CheckStyle, etc.

You can skip all non-essential plug-ins (tests, integration tests, CheckStyle, formatter, API compatibility check, etc.) using the "quick" build profile:

    $ mvn clean verify -Dquick

This can be used for producing connector JARs and/or archives for manual testing in Kafka Connect.

### Running tests (WIP)

Integration tests are not automated and require manual setup and execution against an established Ingres server. To run tests You need to do the following

#### Server-side setup

1. Create or choose two users with security privileges. If pick users that are not 'u1' and 'u2' then change `TestHelper.TEST_SCHEMA` to match
```bash
> sql
CREATE USER u1 WITH PASSWORD='user1', PRIVILEGES = (SECURITY)\g
CREATE USER u2 WITH PASSWORD='user2', PRIVILEGES = (SECURITY)\g
```
2. Create or choose a database to test with. By default dbz is the name of the database coded into the tests. If you change this, then change `TestHelper.TEST_DATABASE` to match
```bash
> createdb dbz
```
3. Create a checkpoint with journaling
```bash
> ckpdb +j dbz
```

#### Test setup

1. Set up an Ingres server reachable from your machine (see "Server-side setup" below).
2. Point the tests at your server. Connection details are read from Java system properties
   (`TestHelper.adminJdbcConfig()` builds them via `Configuration.fromSystemProperties(...)` using the
   `database.` prefix), so most of these can just be passed as `-D` flags on the Maven command line —
   no source changes needed:
   - `-Ddatabase.hostname=<host>` — the Ingres server hostname.
   - `-Ddatabase.port=<port>` — the Ingres server port.
   - `-Ddatabase.user=<user>` — the user to connect with
   - `-Ddatabase.password=<password>` — the password for the user
   - `-Ddatabase.dbname=<database>` — the database to connect to.

   The exceptions are `TEST_DATABASE` / `TEST_SCHEMA` / `TEST_CONNECTOR` in
   `src/test/java/io/debezium/connector/ingres/util/TestHelper.java` — these are compile-time Java
   constants (used directly in topic names, schema prefixes, etc. throughout the tests), not system
   properties, so they do need to be edited in source to match your server's database/schema name, per
   the `FIXME` comment at the top of that file. If you override `-Ddatabase.dbname`, keep it consistent
   with `TEST_DATABASE`.
3. Run the tests, always passing `-Ddocker.skip=true` so the `docker-maven-plugin` doesn't try to start
   a local Docker container (the tests connect to the external server configured above instead):

       $ mvn verify -Ddocker.skip=true -Ddatabase.hostname=<host> -Ddatabase.port=<port> \
             -Ddatabase.user=<user> -Ddatabase.password=<password>

   To run a single test class:

       $ mvn verify -Ddocker.skip=true -Ddatabase.hostname=<host> -Ddatabase.port=<port> \
             -Ddatabase.user=<user> -Ddatabase.password=<password> -Dit.test=IngresConnectorIT
4. A few timeouts around locking, sessions, and Debezium's own record-consumption waits may need
   adjusting for your server:
   - `-Ddatabase.query.timeout.ms=<ms>` — bounds how long an **admin/teardown** JDBC statement (e.g.
     dropping tables between tests) can run before being cancelled. Defaults to 90000 (90s);
   - `-Ddebezium.test.records.waittime=<seconds>` / `-Ddebezium.test.engine.waittime=<seconds>` — how
     long tests wait for expected records/engine state before giving up. Both default to 30

   **Note:** if you see locking errors (e.g. "error occurred in trying to get a lock while opening a
   database") or tests timing out waiting for records, these values may need to be increased for your
   server/network.
