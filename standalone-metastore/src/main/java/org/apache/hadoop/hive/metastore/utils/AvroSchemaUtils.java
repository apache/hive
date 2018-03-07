/*
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
package org.apache.hadoop.hive.metastore.utils;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.serde2.typeinfo.MetastoreTypeCategory;
import org.apache.hadoop.hive.serde2.typeinfo.MetastoreTypeInfoFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.avro.AvroSerDeConstants;
import org.apache.hadoop.hive.serde2.avro.SchemaResolutionProblem;
import org.apache.hadoop.hive.serde2.avro.TypeInfoToSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;

/*
 * Many of the util methods are copied from AvroSerDeUtils from Hive
 */
public class AvroSchemaUtils {
  private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaUtils.class);
  /**
   * Enum container for all avro table properties.
   * If introducing a new avro-specific table property,
   * add it here. Putting them in an enum rather than separate strings
   * allows them to be programmatically grouped and referenced together.
   */
  public static enum AvroTableProperties {
    SCHEMA_LITERAL("avro.schema.literal"),
    SCHEMA_URL("avro.schema.url"),
    SCHEMA_NAMESPACE("avro.schema.namespace"),
    SCHEMA_NAME("avro.schema.name"),
    SCHEMA_DOC("avro.schema.doc"),
    AVRO_SERDE_SCHEMA("avro.serde.schema"),
    SCHEMA_RETRIEVER("avro.schema.retriever");

    private final String propName;

    AvroTableProperties(String propName) {
      this.propName = propName;
    }

    public String getPropName(){
      return this.propName;
    }
  }

  // Following parameters slated for removal, prefer usage of enum above, that allows programmatic access.
  @Deprecated public static final String SCHEMA_LITERAL = "avro.schema.literal";
  @Deprecated public static final String SCHEMA_URL = "avro.schema.url";
  @Deprecated public static final String SCHEMA_NAMESPACE = "avro.schema.namespace";
  @Deprecated public static final String SCHEMA_NAME = "avro.schema.name";
  @Deprecated public static final String SCHEMA_DOC = "avro.schema.doc";
  @Deprecated public static final String AVRO_SERDE_SCHEMA = AvroTableProperties.AVRO_SERDE_SCHEMA.getPropName();
  @Deprecated public static final String SCHEMA_RETRIEVER = AvroTableProperties.SCHEMA_RETRIEVER.getPropName();

  public static final String SCHEMA_NONE = "none";
  public static final String EXCEPTION_MESSAGE = "Neither "
      + AvroTableProperties.SCHEMA_LITERAL.getPropName() + " nor "
      + AvroTableProperties.SCHEMA_URL.getPropName() + " specified, can't determine table schema";

  public static final String LIST_COLUMN_COMMENTS = "columns.comments";
  public static final char COMMA = ',';

  public static List<FieldSchema> getFieldsFromAvroSchema(Configuration configuration,
      Properties properties) throws Exception {
    // Reset member variables so we don't get in a half-constructed state
    Schema schema = null;
    List<String> columnNames = null;
    List<TypeInfo> columnTypes = null;

    final String columnNameProperty = properties.getProperty(ColumnType.LIST_COLUMNS);
    final String columnTypeProperty = properties.getProperty(ColumnType.LIST_COLUMN_TYPES);
    final String columnCommentProperty = properties.getProperty(LIST_COLUMN_COMMENTS,"");
    final String columnNameDelimiter = properties.containsKey(ColumnType.COLUMN_NAME_DELIMITER) ? properties
        .getProperty(ColumnType.COLUMN_NAME_DELIMITER) : String.valueOf(COMMA);

    if (hasExternalSchema(properties)
        || columnNameProperty == null || columnNameProperty.isEmpty()
        || columnTypeProperty == null || columnTypeProperty.isEmpty()) {
      schema = AvroSchemaUtils.determineSchemaOrThrowException(configuration, properties);
    } else {
      // Get column names and sort order
      columnNames = StringUtils.intern(
          Arrays.asList(columnNameProperty.split(columnNameDelimiter)));
      columnTypes = new TypeInfoParser(columnTypeProperty, MetastoreTypeInfoFactory
          .getInstance()).parseTypeInfos();

      schema = getSchemaFromCols(properties, columnNames, columnTypes, columnCommentProperty);
      properties.setProperty(AvroTableProperties.SCHEMA_LITERAL.getPropName(), schema.toString());
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Avro schema is " + schema);
    }

    if (configuration == null) {
      LOG.debug("Configuration null, not inserting schema");
    } else {
      configuration.set(
          AvroTableProperties.AVRO_SERDE_SCHEMA.getPropName(), schema.toString(false));
    }
    return new AvroFieldSchemaGenerator(schema).getFieldSchemas();
  }


  private static boolean hasExternalSchema(Properties properties) {
    return properties.getProperty(AvroTableProperties.SCHEMA_LITERAL.getPropName()) != null
        || properties.getProperty(AvroTableProperties.SCHEMA_URL.getPropName()) != null;
  }

  public static boolean supportedCategories(TypeInfo ti) {
    final MetastoreTypeCategory c = ti.getCategory();
    return c.equals(MetastoreTypeCategory.PRIMITIVE) ||
        c.equals(MetastoreTypeCategory.MAP)          ||
        c.equals(MetastoreTypeCategory.LIST)         ||
        c.equals(MetastoreTypeCategory.STRUCT)       ||
        c.equals(MetastoreTypeCategory.UNION);
  }

  /**
   * Attempt to determine the schema via the usual means, but do not throw
   * an exception if we fail.  Instead, signal failure via a special
   * schema.
   */
  public static Schema determineSchemaOrReturnErrorSchema(Configuration conf, Properties props) {
    try {
      return AvroSchemaUtils.determineSchemaOrThrowException(conf, props);
    } catch (Exception e) {
      LOG.warn("Encountered exception determining schema. Returning signal " +
          "schema to indicate problem", e);
    }
    return SchemaResolutionProblem.SIGNAL_BAD_SCHEMA;
  }

  /**
   * Determine the schema to that's been provided for Avro serde work.
   * @param properties containing a key pointing to the schema, one way or another
   * @return schema to use while serdeing the avro file
   */
  public static Schema determineSchemaOrThrowException(Configuration conf, Properties properties)
      throws Exception {
    String schemaString = properties.getProperty(AvroTableProperties.SCHEMA_LITERAL.getPropName());
    if(schemaString != null && !schemaString.equals(SCHEMA_NONE))
      return AvroSchemaUtils.getSchemaFor(schemaString);

    // Try pulling directly from URL
    schemaString = properties.getProperty(AvroTableProperties.SCHEMA_URL.getPropName());
    if (schemaString == null) {
      final String columnNameProperty = properties.getProperty(ColumnType.LIST_COLUMNS);
      final String columnTypeProperty = properties.getProperty(ColumnType.LIST_COLUMN_TYPES);
      final String columnCommentProperty = properties.getProperty(LIST_COLUMN_COMMENTS);
      if (columnNameProperty == null || columnNameProperty.isEmpty()
          || columnTypeProperty == null || columnTypeProperty.isEmpty() ) {
        throw new IOException(EXCEPTION_MESSAGE);
      }
      final String columnNameDelimiter = properties.containsKey(ColumnType.COLUMN_NAME_DELIMITER) ? properties
          .getProperty(ColumnType.COLUMN_NAME_DELIMITER) : String.valueOf(COMMA);
      // Get column names and types
      List<String> columnNames = Arrays.asList(columnNameProperty.split(columnNameDelimiter));
      List<TypeInfo> columnTypes =
          new TypeInfoParser(columnTypeProperty,
              MetastoreTypeInfoFactory.getInstance()).parseTypeInfos();
      //TODO Why can't we directly bypass this whole logic and use ColumnTypeInfo to use
      //AvroFieldSchemaGenerator directly?
      Schema schema = getSchemaFromCols(properties, columnNames, columnTypes, columnCommentProperty);
      properties.setProperty(AvroTableProperties.SCHEMA_LITERAL.getPropName(), schema.toString());
      if (conf != null)
        conf.set(AvroTableProperties.AVRO_SERDE_SCHEMA.getPropName(), schema.toString(false));
      return schema;
    } else if(schemaString.equals(SCHEMA_NONE)) {
      throw new Exception(EXCEPTION_MESSAGE);
    }

    try {
      Schema s = getSchemaFromFS(schemaString, conf);
      if (s == null) {
        //in case schema is not a file system
        return AvroSchemaUtils.getSchemaFor(new URL(schemaString));
      }
      return s;
    } catch (IOException ioe) {
      throw new Exception("Unable to read schema from given path: " + schemaString, ioe);
    } catch (URISyntaxException urie) {
      throw new Exception("Unable to read schema from given path: " + schemaString, urie);
    }
  }

  // Protected for testing and so we can pass in a conf for testing.
  protected static Schema getSchemaFromFS(String schemaFSUrl,
      Configuration conf) throws IOException, URISyntaxException {
    FSDataInputStream in = null;
    FileSystem fs = null;
    try {
      fs = FileSystem.get(new URI(schemaFSUrl), conf);
    } catch (IOException ioe) {
      //return null only if the file system in schema is not recognized
      if (LOG.isDebugEnabled()) {
        String msg = "Failed to open file system for uri " + schemaFSUrl + " assuming it is not a FileSystem url";
        LOG.debug(msg, ioe);
      }

      return null;
    }
    try {
      in = fs.open(new Path(schemaFSUrl));
      Schema s = AvroSchemaUtils.getSchemaFor(in);
      return s;
    } finally {
      if(in != null) in.close();
    }
  }

  public static Schema getSchemaFor(File file) {
    Schema.Parser parser = new Schema.Parser();
    Schema schema;
    try {
      schema = parser.parse(file);
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse Avro schema from " + file.getName(), e);
    }
    return schema;
  }

  public static Schema getSchemaFor(InputStream stream) {
    Schema.Parser parser = new Schema.Parser();
    Schema schema;
    try {
      schema = parser.parse(stream);
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse Avro schema", e);
    }
    return schema;
  }

  public static Schema getSchemaFor(String str) {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(str);
    return schema;
  }

  public static Schema getSchemaFor(URL url) {
    InputStream in = null;
    try {
      in = url.openStream();
      return getSchemaFor(in);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse Avro schema", e);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          // Ignore
        }
      }
    }
  }

  public static Schema getSchemaFromCols(Properties properties,
      List<String> columnNames, List<TypeInfo> columnTypes, String columnCommentProperty) {
    List<String> columnComments;
    if (columnCommentProperty == null || columnCommentProperty.isEmpty()) {
      columnComments = new ArrayList<String>();
    } else {
      //Comments are separated by "\0" in columnCommentProperty, see method getSchema
      //in MetaStoreUtils where this string columns.comments is generated
      columnComments = Arrays.asList(columnCommentProperty.split("\0"));

      if (LOG.isDebugEnabled()) {
        LOG.debug("columnComments is " + columnCommentProperty);
      }
    }
    if (columnNames.size() != columnTypes.size()) {
      throw new IllegalArgumentException("getSchemaFromCols initialization failed. Number of column " +
          "name and column type differs. columnNames = " + columnNames + ", columnTypes = " +
          columnTypes);
    }

    final String tableName = properties.getProperty(AvroSerDeConstants.TABLE_NAME);
    final String tableComment = properties.getProperty(AvroSerDeConstants.TABLE_COMMENT);
    TypeInfoToSchema metastoreTypeInfoToSchema = new TypeInfoToSchema();
    return metastoreTypeInfoToSchema.convert(columnNames, columnTypes, columnComments,
        properties.getProperty(AvroTableProperties.SCHEMA_NAMESPACE.getPropName()),
        properties.getProperty(AvroTableProperties.SCHEMA_NAME.getPropName(), tableName),
        properties.getProperty(AvroTableProperties.SCHEMA_DOC.getPropName(), tableComment));

  }

  /**
   * Determine if an Avro schema is of type Union[T, NULL].  Avro supports nullable
   * types via a union of type T and null.  This is a very common use case.
   * As such, we want to silently convert it to just T and allow the value to be null.
   *
   * When a Hive union type is used with AVRO, the schema type becomes
   * Union[NULL, T1, T2, ...]. The NULL in the union should be silently removed
   *
   * @return true if type represents Union[T, Null], false otherwise
   */
  public static boolean isNullableType(Schema schema) {
    if (!schema.getType().equals(Schema.Type.UNION)) {
      return false;
    }

    List<Schema> itemSchemas = schema.getTypes();
    if (itemSchemas.size() < 2) {
      return false;
    }

    for (Schema itemSchema : itemSchemas) {
      if (Schema.Type.NULL.equals(itemSchema.getType())) {
        return true;
      }
    }

    // [null, null] not allowed, so this check is ok.
    return false;
  }

  /**
   * In a nullable type, get the schema for the non-nullable type.  This method
   * does no checking that the provides Schema is nullable.
   */
  public static Schema getOtherTypeFromNullableType(Schema schema) {
    List<Schema> itemSchemas = new ArrayList<>();
    for (Schema itemSchema : schema.getTypes()) {
      if (!Schema.Type.NULL.equals(itemSchema.getType())) {
        itemSchemas.add(itemSchema);
      }
    }

    if (itemSchemas.size() > 1) {
      return Schema.createUnion(itemSchemas);
    } else {
      return itemSchemas.get(0);
    }
  }
}
