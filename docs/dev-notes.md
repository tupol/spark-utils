# Development Notes and Hints


## Test Coverage

The test coverage is monitored by the [`scoverage`](https://github.com/scoverage/sbt-scoverage) plugin.

To generate the report one should run

```sbt coverage test coverageReport coverageAggregate```

After a build the coverage report is generated. By looking at the build logs one can observe the global coverage
results as well as the location of the generated detailed reports.

```
[info] Generating scoverage reports...
[info] Written Cobertura report [/Users/olivertupran/work/lg/am-apps/target/scala-2.11/coverage-report/cobertura.xml]
[info] Written XML coverage report [/Users/olivertupran/work/lg/am-apps/target/scala-2.11/scoverage-report/scoverage.xml]
[info] Written HTML coverage report [/Users/olivertupran/work/lg/am-apps/target/scala-2.11/scoverage-report/index.html]
[info] Statement coverage.: 89.26%
[info] Branch coverage....: 86.96%
[info] Coverage reports completed
[info] Aggregation complete. Coverage was [89.26]
[info] All done. Coverage was [89.26%]
```
