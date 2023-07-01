# Migration Status

The below diagram contains a rough overview of the migration progress based on the gradle modules.

```mermaid
gantt
    title Migration Milestones
    dateFormat  YYYY-MM-DD
    axisFormat %B %Y
    tickInterval 1month
    section clients-kt
        Migrate clients main module: 2023-05-25, 1M
        Migrate clients test module: 1M
    section generator-kt
        Migrate generator module: 2023-07-01, 7d
        Implement generator for Kotlin: 14d
    section core-kt
        Migrate module: 2023-08-01, 7d
    section storage-kt
        Migrate module: 2023-08-07, 7d
    section connect-kt
        Migrate connect main module: 2023-08-01, 14d
        Migrate connect test module: 2023-08-01, 14d
    section server-common-kt
        Migrate module: 2023-08-01, 7d
    section streams-kt
        Migrate classes: 40d
    section log4j-appender
        Migrate module: 2023-07-01, 1d
    section group-coordinator-kt
        Migrate module: 2023-07-01, 1d
    section tools-kt
        Migrate module: 7d
    section shell-kt
        Migrate module: 7d
    section jmh-benchmarks-kt
        Migrate module: 7d
    section metadata-kt
        Migrate module: 14d
    section raft-kt
        Migrate module: 14d
    section trogdor-kt
        Migrate module: 14d
    section examples-kt
        Migrate module: 14d
```

## clients-kt Breakdown

The below Gantt diagram contains the details of the clients-kt migration progress.

```mermaid
gantt
    title clients-kt Breakdown
    dateFormat  YYYY-MM-DD
    axisFormat %W
    tickInterval 2week
    section core migration
        Migrate main module: 2023-05-25, 35d
        Fix migration errors: e1, 2023-07-01, 7d
    section fixing / testing
        Migrate test module: after e1, 30d
        Fix migration bugs: after e1, 30d
        Check nullable fields: f1, after e1, 30d
    section cleanup
        Remove deprecated property methods: after f1, 7d
    section reporting
        Report findings: 7d
        Discuss considerations: 7d
```

The following table contains an overview of the tasks related to the clients-kt migration:

| Task                                                     |     Status      |
|----------------------------------------------------------|:---------------:|
| **Migrate clients java module of version 3.4**           |   *Completed*   |
| Optimize class constructors and property methods         | **In progress** |
| Rename modules to -kt instead of -ktx                    |   *Completed*   |
| Fix migration bugs                                       | **In progress** |
| Migrate generator (Java)                                 | **In progress** |
| Implement generator-kt                                   |        ✖        |
| Migrate tests                                            | **In progress** |
| Report founded bugs                                      |        ✖        |
| Rewrite `KafkaFuture` and migrate to Coroutines          |        ✖        |
| Write migration steps for consumers                      |        ✖        |
| Cleanup documentation                                    |        ✖        |
| Update project to latest kafka version                   |        ✖        |
| Update visibility modifiers                              |        ✖        |
| Consider further official Kotlin libraries               |        ✖        |
| Optimize code                                            |        ✖        |
| Replace `.toSet()` with `.toHashSet()` wherever suitable |        ✖        |
