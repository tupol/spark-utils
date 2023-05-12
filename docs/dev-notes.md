# Development Notes and Hints

## Testing Individual Modules

```
sbt "project spark-utils-core; clean; test"
sbt "project spark-utils-io; clean; test"
sbt "project spark-utils-io-pureconfig; clean; test"
```

## Test Coverage

The test coverage is monitored by the [`scoverage`](https://github.com/scoverage/sbt-scoverage) plugin.

To generate the report one should run

```sbt coverage test coverageReport coverageAggregate```

After a build the coverage report is generated. By looking at the build logs one can observe the global coverage
results as well as the location of the generated detailed reports.

```
[info] Generating scoverage reports...
[info] Written Cobertura report [.../tupol/spark-utils/target/scala-2.11/coverage-report/cobertura.xml]
[info] Written XML coverage report [.../tupol/spark-utils/target/scala-2.11/scoverage-report/scoverage.xml]
[info] Written HTML coverage report [.../tupol/spark-utils/target/scala-2.11/scoverage-report/index.html]
[info] Statement coverage.: 88.51%
[info] Branch coverage....: 79.17%
[info] Coverage reports completed
[info] All done. Coverage was [88.51%]
```

## Running Unit Tests

The following options will help running the tests:


```
--add-opens=java.base/java.lang=ALL-UNNAMED, 
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED, 
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED, 
--add-opens=java.base/java.io=ALL-UNNAMED, 
--add-opens=java.base/java.net=ALL-UNNAMED, 
--add-opens=java.base/java.nio=ALL-UNNAMED, 
--add-opens=java.base/java.util=ALL-UNNAMED, 
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED, 
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED, 
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED, 
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED, 
--add-opens=java.base/sun.security.action=ALL-UNNAMED, 
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED, 
--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED)
```