# Hive Metastore Properties Package

## Overview

The `properties` package provides a **type-safe, schema-driven property management system** for Hive metastore objects (clusters, databases, tables). It allows you to declare, validate, and persist custom properties with compile-time type checking and runtime serialization support.

## Core Components

### 1. **PropertySchema** (`PropertySchema.java`)
Defines the blueprint for what properties can be set on a metastore object.

**Key responsibilities:**
- Declares allowed properties and their types (e.g., STRING, INTEGER, BOOLEAN, DATE, etc.)
- Manages default values for properties
- Tracks schema version (incremented when properties are added/removed)
- Generates a digest (UUID) for schema identity/caching
- Supports thread-safe serialization/deserialization

**Main methods:**
- `declareProperty(name, type, defaultValue)` - Register a new property in the schema
- `getPropertyType(name)` - Retrieve the declared type
- `getDefaultValue(name)` - Get the default value for a property
- `removeProperty(name)` - Remove a property (testing/cleanup)
- `getDigest()` - Get a stable UUID representing the schema state

**Example use case:**
```java
PropertySchema tableSchema = new PropertySchema("table-schema", 1, new TreeMap<>());
tableSchema.declareProperty("owner", PropertyType.STRING, "system");
tableSchema.declareProperty("created_date", PropertyType.DATETIME);
tableSchema.declareProperty("tags", PropertyType.JSON);
```

### 2. **PropertyType** (`PropertyType.java`)
An abstract type system for property values with parsing, formatting, and serialization.

**Supported types:**
- `STRING` - Text values
- `BOOLEAN` - true/false values
- `INTEGER` - 32-bit integers
- `LONG` - 64-bit integers
- `DOUBLE` - Floating-point values
- `DATETIME` - ISO8601 dates (UTC timezone)
- `JSON` - Complex nested objects (Gson-backed)

**Key methods:**
- `cast(value)` - Type-coerce a value to this type
- `parse(str)` - Parse from string (deserialization)
- `format(value)` - Convert to string (serialization)
- `read(DataInput)` / `write(DataOutput)` - Binary I/O
- `register(type)` - Register custom property types

**Type safety:**
Each type handles conversion, null-safety, and format consistency. For example, the DATETIME type always uses ISO8601 format with UTC timezone, ensuring cross-system consistency.

### 3. **PropertyMap** (`PropertyMap.java`)
Holds the actual property values for a metastore object, validated against a schema.

**Key features:**
- **Copy-on-write**: Dirty flag prevents unnecessary copies; shared content until modified
- **Thread-safe**: Isolated from concurrent modifications
- **Schema-bound**: All properties must match the associated PropertySchema
- **Serializable**: Can be persisted and restored with full type information

**Purpose:**
While PropertySchema defines *what properties are allowed*, PropertyMap holds the *actual values* for a specific object instance.

### 4. **PropertyManager** (`PropertyManager.java`)
High-level manager orchestrating property operations at the session level.

**Responsibilities:**
- Manages multiple property schemas in a unified namespace
- Handles transactional property reads and writes
- Tracks dirty maps to batch updates and reduce persistence store hits
- Integrates with JEXL for dynamic property expressions
- Coordinates with PropertyStore for persistence

**Key design:**
- One PropertyManager instance per session
- All PropertyMaps managed by the same manager must use one of its known schemas
- Avoids writing the entire map to the store on each property change

### 5. **PropertyStore** (`PropertyStore.java`)
Persistence layer for storing/retrieving properties from the metastore.

**Responsibilities:**
- Load property maps from the database
- Save property maps to the database
- Manage property schema persistence
- Can be wrapped by `CachingPropertyStore` for performance

### 6. Supporting Classes

- **Digester** - Generates stable UUID digests for schema/map identity
- **SerializationProxy** - Custom serialization to handle transient fields safely
- **SoftCache** - Soft reference cache for property schemas (GC-friendly)
- **PropertyException** - Exception type for property-related errors

## Workflow Example

```java
// 1. Define a schema (typically done at startup)
PropertySchema tableSchema = new PropertySchema("table-schema", 1, new TreeMap<>());
tableSchema.declareProperty("owner", PropertyType.STRING, "admin");
tableSchema.declareProperty("is_external", PropertyType.BOOLEAN, false);
tableSchema.declareProperty("created_time", PropertyType.DATETIME);

// 2. Create a property manager
PropertyManager manager = new PropertyManager("my-namespace");

// 3. Load or create a property map for a table
PropertyMap tableProps = manager.newPropertyMap(tableSchema);

// 4. Set values
tableProps.put("owner", "alice");
tableProps.put("is_external", true);

// 5. Persist
manager.persistProperties(tableProps);

// 6. Later, retrieve and use
PropertyMap loaded = manager.loadProperties(objectId);
String owner = (String) loaded.get("owner");
```

## Design Patterns

### Thread Safety
- Schemas use `AtomicInteger` for version counters
- PropertyMaps use copy-on-write with dirty flags
- Digest computation is double-checked locked (lazy initialization)

### Serialization
- Custom `SerializationProxy` handles transient field safety
- Transient fields prevent accidental Java serialization of internal state
- Both XML (DataInput/DataOutput) and Java object serialization supported

### Type Safety
- Every property value is validated against its declared type
- Type mismatches on property declaration throw `IllegalArgumentException`
- Null values are allowed and handled consistently across types

### Schema Versioning
- Version incremented when properties added/removed
- Digest UUID allows detecting schema changes
- Supports schema evolution without breaking existing data

## Key Features

✅ **Type-safe** - Compile-time and runtime type checking  
✅ **Extensible** - Custom PropertyTypes can be registered  
✅ **Persistent** - Full serialization/deserialization support  
✅ **Efficient** - Copy-on-write semantics avoid unnecessary writes  
✅ **Transactional** - Integrated with metastore transactions  
✅ **Versioned** - Schema evolution tracking built-in  

## Use Cases in Hive

- **Table metadata**: Owner, creation time, custom tags
- **Database properties**: Environment (dev/prod), compliance flags
- **Cluster properties**: Configuration overrides, feature flags
- **Custom extensions**: Any application-specific metadata via JSON type

## See Also

- [PropertySchema.java](./standalone-metastore/metastore-common/src/main/java/org/apache/hadoop/hive/metastore/properties/PropertySchema.java)
- [PropertyType.java](./standalone-metastore/metastore-common/src/main/java/org/apache/hadoop/hive/metastore/properties/PropertyType.java)
- [PropertyMap.java](./standalone-metastore/metastore-common/src/main/java/org/apache/hadoop/hive/metastore/properties/PropertyMap.java)
- [PropertyManager.java](./standalone-metastore/metastore-common/src/main/java/org/apache/hadoop/hive/metastore/properties/PropertyManager.java)
