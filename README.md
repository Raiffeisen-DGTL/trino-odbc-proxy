# Zaychik: Arrow Flight SQL Proxy Server for Trino

Implementation of Arrow FlightSQL Server to allow querying Trino using Arrow Flight clients such as
Arrow Flight JDBC/ODBC drivers.

Implementation of this server is inspired by the FlightSQL server example for Apache Derby gently
provided by Apache Arrow community. Example implementation can be found
[here](https://github.com/apache/arrow/blob/main/java/flight/flight-sql/src/test/java/org/apache/arrow/flight/sql/example/FlightSqlExample.java).

> Proxy server is build using:
>
> * Java 17
> * Trino JDBC driver: version 452
> * Arrow Flight SQL Server: version 17.0.0

## General Approach and Requirements

* Proxy server queries Trino using JDBC driver.
* Proxy server requires user-password authorisation.
* User and password are forwarded to Trino connection factory and used to establish connection to Trino.
* Thus, Trino cluster must have user-password authentication type. Other types of authentication as well
  as unauthenticated access to Trino are not supported.
* No connection pooling to Trino is used as connections are user-specific.
* Forwarding user and password prevents impersonation when connecting to Trino and, therefore,
  allows inheritance of user permissions from Trino itself.
* Trino JDBC driver streams data over HTTP under the hood.
* Data is received by Proxy server in batches.
  Each batch is encoded to Apache Arrow format and streamed to the Arrow client.
* Complex data types are not supported. See [table](#supported-trino-data-types) below.
* Server provides custom JMX metrics for better monitoring.

## Build and Run

The project is powered by Maven, thus in order to build project and collect dependencies, use following maven commands:
* `mvn clean package` - to compile and package project to jar-file.
* `mvn dependency:copy-dependencies` - to collect project dependencies.

In order to export JMX metrics in prometheus format, it is required to use `jmx_prometheus_javaagent`
along with jmx configuration file. The prometheus jmx exported agent is defined in `pom.xml` and will be downloaded
along with other dependencies. Copy the jmx configuration file to working directory which should have the following structure:
```
workdir
|-- trino-arrow-flight-proxy-0.1.0.jar
|-- dependency  # folder with collected project dependencies
|-- jmx_config.yaml
```

The following java options need to be provided:
* `-add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED` - required to run Apache Arrow Flight SQL server.
* `-javaagent:./dependency/jmx_prometheus_javaagent-1.0.1.jar=9090:jmx_config.yaml` - required to run prometheus jmx exporter as java-agent. 
  Metrics will be served at port `9090` (change port number if required).
* `-Darrow.memory.debug.allocator=true` - optional to enable Arrow memory allocator debug logs.

Java options can be easily provided via `_JAVA_OPTIONS` environment variable as follows:
```bash
export _JAVA_OPTIONS="-Darrow.memory.debug.allocator=true -javaagent:./dependency/jmx_prometheus_javaagent-1.0.1.jar=9090:jmx_config.yaml --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
```

Finally, to run the proxy server, execute Java command:
```bash
java -cp trino-arrow-flight-proxy-0.1.0.jar:dependency/* ru.raiffeisen.trino.arrow.flight.sql.server.FlightSQLTrinoProxy
```

## Authorization

There are two types of user-password authorization middleware supported by this Proxy server:

* `TRINO` - authorization middleware tries to create JDBC connection to Trino using provided user and password.
  If connection created successfully, then user is authorized to connect to server. Thus, all users who are authorized
  to connect to Trino are able to connect to Proxy server.
* `LDAP` - connects to LDAP server and checks if user is authorized to connect to server. Connection to LDAP server is
  performed using provided user and password, thus no additional credentials are required.
  **Connection to LDAP is performed with SSL. Thus, ldap provider url must have `ldaps` scheme and be accessible on port
  636.**

## Configuration

Server configuration must be specified in `application.properties` file in resource directory. Alternatively,
configuration can be set using environment variables. Server recognizes environment variables started from prefix `TF_`.

All proxy server configurations are listed below. If configuration parameter does not have a default value,
then value of this parameter must be set explicitly either in `application.properties` or in corresponding environment
variable.

Configuration parameters set via environment variables override ones provided in `application.properties` file.

### Flight Server Configuration

* `flight.host` - Arrow flight server host \
  Environment variable: `TF_FLIGHT_HOST` \
  Default value: `localhost`
* `flight.port` - Arrow flight server port \
  Environment variable: `TF_FLIGHT_PORT` \
  Default value: `32010`
* `flight.ssl` - Is connection to arrow flight server should use SSL?
  Environment variable: `TF_FLIGHT_SSL` \
  Default value: `false`
  > Currently, SSL connection to Arrow Flight Server is not supported.
* `flight.auth.type` - Type of authorization middleware: either `trino` or `ldap` types are supported.
  Environment variable: `TF_FLIGHT_AUTH_TYPE` \
  Default value: `trino`
* `flight.batch.size` - Size of the batch (number of rows). Server fetches data from Trino and streams it
  to client in batches. If size of the batch is small the overall data streaming rate will be slower.
  If size of the batch is too large the latency will be large as well.
  Environment variable: `TF_FLIGHT_BATCH_SIZE` \
  Default value: `1024`
* `flight.backpressure.threshold` - The number of bytes that can be queued on an output stream before blocking.
  When blocked listener will wait until data is streamed to the client and block is released and only after that it
  will continue to stream next batch.
  Environment variable: `TF_FLIGHT_BACKPRESSURE_THRESHOLD` \
  Default value: `10485760`
* `flight.backpressure.timeout` - Maximum amount of time in `milliseconds` to wait for backpressure block to be
  released.
  If block is not released for `timeout` time then data stream will be cancelled.
  Environment variable: `TF_FLIGHT_BACKPRESSURE_TIMEOUT` \
  Default value: `30000`
* `flight.server.readonly` - Is arrow flight server in read-only mode? \
  Environment variable: `TF_FLIGHT_SERVER_READONLY` \
  Default value: `false`
* `flight.sql.ddl.catalogs.support` - Is arrow flight server ddl operations support catalogs? \
  Environment variable: `TF_FLIGHT_SQL_DDL_CATALOGS_SUPPORT` \
  Default value: `false`
* `flight.sql.ddl.schemas.support` - Is arrow flight server ddl operations support schemas? \
  Environment variable: `TF_FLIGHT_SQL_DDL_SCHEMAS_SUPPORT` \
  Default value: `false`
* `flight.sql.ddl.all.tables.selectable` - Is all tables selectable in arrow flight server? \
  Environment variable: `TF_FLIGHT_SQL_DDL_ALL_TABLES_SELECTABLE` \
  Default value: `false`
* `flight.sql.identifier.quote.char` - Arrow flight server identifiers quote char \
  Environment variable: `TF_FLIGHT_SQL_IDENTIFIER_QUOTE_CHAR` \
  Default value: `"`
* `flight.sql.identifier.case.sensitivity` - Arrow flight server identifiers case sensitivity \
  Environment variable: `TF_FLIGHT_SQL_IDENTIFIER_CASE_SENSITIVIY` \
  Default value: `0`
  > Values from 0 to 3 are allowed, where:
  > * 0 - unknown case sensitivity;
  > * 1 - case insensitive;
  > * 2 - upper case sensitivity;
  > * 3 - lower case sensitivity.
* `flight.sql.identifier.quoted.case.sensitivity` - Arrow flight server quoted identifiers case sensitivity \
  Environment variable: `TF_FLIGHT_SQL_IDENTIFIER_QUOTED_CASE_SENSITIVIY` \
  Default value: `0`
  > Values from 0 to 3 are allowed, where:
  > * 0 - unknown case sensitivity;
  > * 1 - case insensitive;
  > * 2 - upper case sensitivity;
  > * 3 - lower case sensitivity.
* `flight.sql.max.table.columns` - Maximum allowed number of columns in arrow flight server tables \
  Environment variable: `TF_FLIGHT_SQL_MAX_TABLE_COLUMNS` \
  Default value: `1024`

### Trino Connection Configuration

* `trino.host` - Trino DB host \
  Environment variable: `TF_TRINO_HOST` \
  Default value: `localhost`
* `trino.port` - Trino DB port \
  Environment variable: `TF_TRINO_PORT` \
  Default value: `443`
* `trino.ssl` - Is connection to Trino should use SSL? \
  Environment variable: `TF_TRINO_SSL` \
  Default value: `false`
* `trino.version` - Trino Server version
  Environment variable: `TF_TRINO_VERSION` \
  Default value: `452`
* `trino.driver.version` - Trino driver version
  Environment variable: `TF_TRINO_DRIVER_VERSION` \
  Default value: `452`
  > This server uses Trino Driver v.452 for connections. No need to override this configuration value.

### SSL Configuration

* `ssl.keystore.path` - Location of keystore to use for connection to LDAP with SSL \
  Environment variable: `TF_SSL_KEYSTORE_PATH` \
  Default value: `$JAVA_HOME/lib/security/cacerts`
* `ssl.keystore.password` - Password to keystore  \
  Environment variable: `TF_SSL_KEYSTORE_PASSWORD` \
  Default value: `changeit`
* `ssl.keystore.type` - Type of keystore \
  Environment variable: `TF_SSL_KEYSTORE_TYPE` \
  Default value: `jks`
* `ssl.truststore.path` - Location of keystore to use for connection to LDAP with SSL \
  Environment variable: `TF_SSL_TRUSTSTORE_PATH` \
  Default value: **defaults to `sss.keystore.path` value**
* `ssl.truststore.password` - Password to truststore  \
  Environment variable: `TF_SSL_TRUSTSTORE_PASSWORD` \
  Default value: **defaults to `sss.keystore.password` value**
* `ssl.truststore.type` - Type of truststore \
  Environment variable: `TF_SSL_TRUSTSTORE_TYPE` \
  Default value: **defaults to `sss.keystore.type` value**

### LDAP Configuration

* `ldap.provider` - LDAP provider URL \
  Environment variable: `TF_LDAP_PROVIDER` \
  Default value: `null` - need to be provided explicitly if LDAP authorization middleware is used.
  > Must have `ldaps` scheme and be accessible on port `636`.
* `ldap.domain` - LDAP domain \
  Environment variable: `TF_LDAP_DOMAIN` \
  Default value: `null` - need to be provided explicitly if LDAP authorization middleware is used.
  > User is prefixed with domain when connection to LDAP is established: `ldap.domain\user`.
* `ldap.referral` - LDAP referral \
  Environment variable: `TF_LDAP_REFERRAL` \
  Default value: `follow`
* `ldap.timeout ` - LDAP connection timeout (millis) \
  Environment variable: `TF_LDAP_TIMEOUT` \
  Default value: `5000`
* `ldap.retries.count` - LDAP connection retries \
  Environment variable: `TF_LDAP_RETRIES_COUNT` \
  Default value: `3`
* `ldap.retries.interval` - LDAP connection retry interval (millis) \
  Environment variable: `TF_LDAP_RETRIES_INTERVAL` \
  Default value: `5000`
* `ldap.search.dn1` - Primary LDAP search DN to look for user attributes \
  Environment variable: `TF_LDAP_SEARCH_DN1` \
  Default value: `null` - need to be provided explicitly if LDAP authorization middleware is used.
* `ldap.search.dn2` - Secondary LDAP search DN to look for user attributes \
  Environment variable: `TF_LDAP_SEARCH_DN2` \
  Default value: `null` - if omitted, then search will be limited to primary DN only.
* `ldap.search.dn3` - Tertiary LDAP search DN to look for user attributes \
  Environment variable: `TF_LDAP_SEARCH_DN3` \
  Default value: `null` - if omitted, then search will be limited to primary and secondary DNs only.
* `ldap.search.filter` - LDAP search filter to fetch results for a particular user \
  Environment variable: `TF_LDAP_SEARCH_FILTER` \
  Default value: `null` - need to be provided explicitly if LDAP authorization middleware is used.
  > Typically, here one need to set some search filter template with `${USER}` value which will be substituted with
  > actual username during authorisation.
* `ldap.search.attribute` - LDAP search attribute containing user role membership \
  Environment variable: `TF_LDAP_SEARCH_ATTRIBUTE` \
  Default value: `null` - need to be provided explicitly if LDAP authorization middleware is used.
* `ldap.auth.role` - LDAP authorisation role (authorise only users who have this role) \
  Environment variable: `TF_LDAP_AUTH_ROLE` \
  Default value: `null` - need to be provided explicitly if LDAP authorization middleware is used.

## Supported Trino Data Types

When querying data from Trino via JDBC driver the Trino types are converted to JDBC types.
Then, JDBC types are mapped to the Apache Arrow types. So, when data is actually consumed by the proxy server
the JDBC record are retrieved in batches and converted to Apache Arrow vector in accordance with defined type mapping.

Table below shows how Trino types are mapped to JDBC and Arrow types and also provides information whether Trino type
is supported by the proxy server.

| Trino Type              | JDBC Type (with code)          | Arrow Type                                                  | Supported?  |
|-------------------------|--------------------------------|-------------------------------------------------------------|-------------|
| BOOLEAN                 | BOOLEAN (16)                   | ArrowType.Bool                                              | YES         |
| TINYINT                 | TINYINT (-6)                   | ArrowType.Int(8, signed)                                    | YES         |
| SMALLINT                | SMALLINT (5)                   | ArrowType.Int(16, signed)                                   | YES         |
| INTEGER                 | INTEGER (4)                    | ArrowType.Int(32, signed)                                   | YES         |
| BIGINT                  | BIGINT (-5)                    | ArrowType.Int(64, signed)                                   | YES         |
| REAL                    | REAL (7)                       | ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)      | YES         |
| DOUBLE                  | DOUBLE (8)                     | ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)      | YES         |
| DECIMAL                 | DECIMAL (3)                    | ArrowType.Decimal(precision, scale)                         | YES         |
| VARCHAR                 | VARCHAR (12)                   | ArrowType.Utf8                                              | YES         |
| CHAR                    | CHAR (1)                       | ArrowType.Utf8                                              | YES         |
| VARBINARY               | VARBINARY (-3)                 | ArrowType.Binary                                            | YES         |
| JSON                    | JAVA_OBJECT (2000)             | ***!!! unmapped !!!***                                      | NO          |
| DATE                    | DATE (91)                      | ArrowType.Date(DateUnit.DAY)                                | YES         |
| TIME                    | TIME (92)                      | ArrowType.Time(TimeUnit.MILLISECOND, 32)                    | YES         |
| TIME(P)                 | TIME (92)                      | ArrowType.Time(TimeUnit.MILLISECOND, 32)                    | YES         |
| TIME WITH TIMEZONE      | TIME_WITH_TIMEZONE (2013)      | ArrowType.Time(TimeUnit.MILLISECOND, 32)                    | LIMITED <1> |
| TIMESTAMP               | TIMESTAMP (93)                 | ArrowType.Timestamp(TimeUnit.MILLISECOND, default_timezone) | YES         |
| TIMESTAMP(P)            | TIMESTAMP (93)                 | ArrowType.Timestamp(TimeUnit.MILLISECOND, default_timezone) | YES         |
| TIMESTAMP WITH TIMEZONE | TIMESTAMP_WITH_TIMEZONE (2014) | ArrowType.Timestamp(TimeUnit.MILLISECOND, default_timezone) | LIMITED <1> |
| INTERVAL YEAR TO MONTH  | JAVA_OBJECT (2000)             | ***!!! unmapped !!!***                                      | NO          |
| INTERVAL DAY TO SECOND  | JAVA_OBJECT (2000)             | ***!!! unmapped !!!***                                      | NO          |
| ARRAY                   | ARRAY (2003)                   | ***!!! unmapped !!!***                                      | NO          |
| MAP                     | JAVA_OBJECT (2000)             | ***!!! unmapped !!!***                                      | NO          |
| ROW                     | JAVA_OBJECT (2000)             | ***!!! unmapped !!!***                                      | NO          |
| IPADDRESS               | JAVA_OBJECT (2000)             | ***!!! unmapped !!!***                                      | NO          |
| UUID                    | JAVA_OBJECT (2000)             | ***!!! unmapped !!!***                                      | NO          |
| HyperLogLog             | JAVA_OBJECT (2000)             | ***!!! unmapped !!!***                                      | NO          |
| P4HyperLogLog           | JAVA_OBJECT (2000)             | ***!!! unmapped !!!***                                      | NO          |
| SetDigest               | JAVA_OBJECT (2000)             | ***!!! unmapped !!!***                                      | NO          |
| QDigest                 | JAVA_OBJECT (2000)             | ***!!! unmapped !!!***                                      | NO          |
| TDigest                 | JAVA_OBJECT (2000)             | ***!!! unmapped !!!***                                      | NO          |

> **<1>** Despite server supports mapping for `TIME_WITH_TIMEZONE` and `TIMESTAMP_WITH_TIMEZONE` the actual
> conversion replaces timezone with server default one. Therefore, this types should be used with caution.

## Custom JMX Metrics

> Some of the metrics below serve results per query size group: queries are assigned to size group 
> depending on the number of rows they returned. Size group is a power of 10. Thus, group #4 represents all
> queries that returned between 10'000 and 100'000 rows.

### Status Count

* `arrow_flight_server_status_TotalCount` - total number of calls to the server.
* `arrow_flight_server_status_Count` - number of calls to the server grouped per call status.

### Allocated Buffer Memory

* `arrow_flight_server_buffer_Allocated` - current memory allocated by Arrow Buffer Allocator.

### Listener Wait Time

There could be situations when data is streamed from Trino to proxy server faster that from proxy server to the client.
In such situations, the data must be buffered at proxy server while waiting to be sent to the client.
This is a risky process as buffer can overflow JVM memory. In order to avoid that, the server implements so called
`backpressure strategy`: when the buffer with results reaches maximum allowed size, the listener is set on hold and
waits until proxy server streams data to the client. Metrics below can be used to monitor time that listener was waiting
thus identifying backpressure issues.

* `arrow_flight_server_listener_TotalWaitTime` - total listener wait time.
* `arrow_flight_server_listener_WaitTime` - total listener wait time grouped per query size group.
* `arrow_flight_server_listener_AvgWaitTime` - average listener wait time per single row grouped per query size group.
* `arrow_flight_server_listener_TotalAvgWaitTime` - overall average listener wait time per single row.

### Query Count

Server counts number of queries that was executed.  Only queries that actually streamed data to the client
are considered. Cancelled and errored queries are omitted.

* `arrow_flight_server_query_TotalCount` - total number of queries executed so far.
* `arrow_flight_server_query_CountBySize` - number of queries grouped per query size group. 
* `arrow_flight_server_query_CountByUser` - number of queries grouped per user.

### Row Count

Server counts number of rows that was streamed to the client. In addition, server measures the time that was taken 
to stream data. **Note** that measurement includes only time taken to stream the data and ignores the time taken to
execute query in Trino, as this information is not related to proxy server performance.

* `arrow_flight_server_row_TotalCount` - total number of rows streamed so far.
* `arrow_flight_server_row_RowsPerUser` - number of rows grouped per user.
* `arrow_flight_server_row_AvgRowsPerQuery` - average number of rows per query.
* `arrow_flight_server_row_StreamingRate` - average streaming rate for last 10 queries (rows/ms).


## Know Issues when using Arrow Flight SQL ODBC Driver from Dremio

Trino community does not have an open-source version of the ODBC driver. Therefore, one of the primary use case 
to run this proxy server is to allow ODBC connectivity to Trino using open-source ODBC drivers,
such as [Arrow Flight SQL ODBC Driver from Dremio](https://docs.dremio.com/current/sonar/client-applications/drivers/arrow-flight-sql-odbc-driver/).

Though, there are some known issues we faced while using this driver for connections to Trino proxy. These are:

* ODBC Driver does not support special characters in user passwords, such as `.,!=:;`.
* ODBC Driver does not forward JDBC errors to the client.
  They stays inside the proxy server and can be found in the server logs.
