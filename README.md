# kafka-kt

This is an unofficial and experimental Kotlin migration of the
[Apache kafka](https://github.com/apache/kafka) project.

## Migration

### Migration Status

| Task                                             |      Status      |
|--------------------------------------------------|:----------------:|
| **Migrate source code of version 3.4**           | **In progress️** |
| Optimize class constructors and property methods | **In progress️** |
| Rename modules to -kt instead of -ktx            |        ✖         |
| Fix migration bugs                               |        ✖         |
| Report founded bugs                              |        ✖         |
| Rewrite `KafkaFuture` and migrate to Coroutines  |        ✖         |
| Write migration steps for consumers              |        ✖         |
| Cleanup documentation                            |        ✖         |
| Update project to latest kafka version           |        ✖         |
| Update visibility modifiers                      |        ✖         |
| Consider further official Kotlin libraries       |        ✖         |
| Optimize code                                    |        ✖         |

## Breaking Changes

This project has the goal to allow users to easily integrate Apache kafka into Kotlin without the
burden of expensive code migrations. Due to the differences between the two languages Java and
Kotlin there are still some breaking changes in specific parts of the code that affect the
integrations.

### Constructors / Instantiation of kafka classes

The constructors of multiple classes were merged into fewer constructors in Kotlin (most of the time
to a single constructor) to make use of default values in constructors, reduce code duplication and
unnecessary overloads, and to improve readability.

### Property access

kafka was using class methods to access the properties of a class, introducing unnecessary
parentheses and reducing the readability of code. It would be possible to simplify the calls in
Kotlin if the methods were prefixed with `get`. Since this was not the case, the migration marked
these functions as deprecated and provided properties to improve the readability of code and
reducing unnecessary parentheses when using class properties.

Some methods from Java may already been replaced with Kotlin properties. This is the case for some
boolean values as well as for conflicting fields.

### Enum classes' name property

In Kotlin the enum classes have already a name property, preventing any enum class to provide a
custom name for its enums. Therefore, the existing `name` field in enum classes was renamed to
`altName`.

### Collection Immutability

Due to the immutability of collections in Kotlin, many operations could be simplified. There may be
bugs introduced with the use of immutable collections, but due to the constant use of immutable
collections in the migrations, collections can now be reused without worrying about mutations.
Because of this it is also expected that the overall memory usage and performance is improved.

### Null Safety

The null safety was probably one of the main reasons the entire project was migrated. Thanks to the
null safety in Kotlin nullable fields are marked accordingly, which results to a clearer API
definition, more safety and less null-checks in consumer code.

### Serialization and Nullability

Implementations of `Serializer` and `Deserializer` should, according to the documentation, prefer
`null`s over exceptions when something goes wrong during the serialization and deserialization
process. Due to the null-safety in Kotlin the migration can make extensive use of nullable and
non-nullable serialization and deserialization. For this reason, additional Serdes with null-safety
were introduced.

### `KafkaFuture` and Kotlin Coroutines

During the migration process the `KafkaFuture` class will be replaced to follow a more Kotlin-like
approach. This will remove the current nullability from results on success and errors on failure.
The use of sealed classes will be considered.