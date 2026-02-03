# Hive Development Setup

## Prerequisites
- SDKMAN! installed (for Java version management)
- Maven 3.6+
- Git

## Building Hive

### For Hive 3.x (Java 8)

```bash
# Set Java version
sdk use java 8.0.402-amzn
```

follow - https://6sense.atlassian.net/wiki/spaces/BIG/pages/5225251268/Building+GDP+binaries#Hive

> If face issue due to x86 arch. - https://issues.apache.org/jira/browse/HIVE-21939
```python
- com.google.protobuf:protoc:2.5.0
+ com.google.protobuf:protoc:3.7.1
```

### For Hive 4.x (Java 17)

```bash
# Set Java version
sdk use java 17.0.10-amzn

# Build Hive
mvn clean install -DskipTests
```

## Running Tests

### Run all tests
```bash
mvn clean test
```

### Run specific test class
```bash
mvn test -Dtest=TestClassName -pl ql
```

### Run specific test module
```bash
mvn test -pl ql
mvn test -pl metastore
```

## Tips

- Add `-X` flag for debug output: `mvn clean install -DskipTests -X`
- Use `-o` for offline mode if dependencies are cached: `mvn clean install -DskipTests -o`
- Build faster with parallel builds: `mvn clean install -DskipTests -T 1C` (1 thread per core)