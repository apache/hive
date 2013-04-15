/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace java org.apache.hadoop.hive.serde
namespace php org.apache.hadoop.hive.serde
namespace py org_apache_hadoop_hive_serde
namespace cpp Hive

  // name of serialization scheme.
const string SERIALIZATION_LIB = "serialization.lib"
const string SERIALIZATION_CLASS = "serialization.class"
const string SERIALIZATION_FORMAT = "serialization.format"
const string SERIALIZATION_DDL = "serialization.ddl"
const string SERIALIZATION_NULL_FORMAT = "serialization.null.format"
const string SERIALIZATION_LAST_COLUMN_TAKES_REST = "serialization.last.column.takes.rest"
const string SERIALIZATION_SORT_ORDER = "serialization.sort.order"
const string SERIALIZATION_USE_JSON_OBJECTS = "serialization.use.json.object"

const string FIELD_DELIM = "field.delim"
const string COLLECTION_DELIM = "colelction.delim"
const string LINE_DELIM = "line.delim"
const string MAPKEY_DELIM = "mapkey.delim"
const string QUOTE_CHAR = "quote.delim"
const string ESCAPE_CHAR = "escape.delim"

typedef string PrimitiveType
typedef string CollectionType

const string VOID_TYPE_NAME       = "void";
const string BOOLEAN_TYPE_NAME  = "boolean";
const string TINYINT_TYPE_NAME   = "tinyint";
const string SMALLINT_TYPE_NAME  = "smallint";
const string INT_TYPE_NAME       = "int";
const string BIGINT_TYPE_NAME    = "bigint";
const string FLOAT_TYPE_NAME     = "float";
const string DOUBLE_TYPE_NAME    = "double";
const string STRING_TYPE_NAME    = "string";
const string DATE_TYPE_NAME      = "date";
const string DATETIME_TYPE_NAME  = "datetime";
const string TIMESTAMP_TYPE_NAME = "timestamp";
const string DECIMAL_TYPE_NAME   = "decimal";
const string BINARY_TYPE_NAME    = "binary";

const string LIST_TYPE_NAME = "array";
const string MAP_TYPE_NAME  = "map";
const string STRUCT_TYPE_NAME  = "struct";
const string UNION_TYPE_NAME  = "uniontype";

const string LIST_COLUMNS = "columns";
const string LIST_COLUMN_TYPES = "columns.types";

const set<string> PrimitiveTypes  = [ VOID_TYPE_NAME BOOLEAN_TYPE_NAME TINYINT_TYPE_NAME SMALLINT_TYPE_NAME INT_TYPE_NAME BIGINT_TYPE_NAME FLOAT_TYPE_NAME DOUBLE_TYPE_NAME STRING_TYPE_NAME  DATE_TYPE_NAME DATETIME_TYPE_NAME TIMESTAMP_TYPE_NAME DECIMAL_TYPE_NAME BINARY_TYPE_NAME],
const set<string> CollectionTypes = [ LIST_TYPE_NAME MAP_TYPE_NAME ],


