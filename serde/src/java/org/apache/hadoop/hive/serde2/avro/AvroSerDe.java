/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.avro;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;

/**
 * Read or write Avro data from Hive.
 */
@SerDeSpec(schemaProps = {
    serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES,
    AvroSerDe.LIST_COLUMN_COMMENTS, AvroSerDe.TABLE_NAME, AvroSerDe.TABLE_COMMENT,
    AvroSerdeUtils.SCHEMA_LITERAL, AvroSerdeUtils.SCHEMA_URL,
    AvroSerdeUtils.SCHEMA_NAMESPACE, AvroSerdeUtils.SCHEMA_NAME, AvroSerdeUtils.SCHEMA_DOC})
public class AvroSerDe extends AbstractSerDe {
  private static final Log LOG = LogFactory.getLog(AvroSerDe.class);

  public static final String TABLE_NAME = "name";
  public static final String TABLE_COMMENT = "comment";
  public static final String LIST_COLUMN_COMMENTS = "columns.comments";

  public static final String DECIMAL_TYPE_NAME = "decimal";
  public static final String CHAR_TYPE_NAME = "char";
  public static final String VARCHAR_TYPE_NAME = "varchar";
  public static final String DATE_TYPE_NAME = "date";
  public static final String TIMESTAMP_TYPE_NAME = "timestamp-millis";
  public static final String AVRO_PROP_LOGICAL_TYPE = "logicalType";
  public static final String AVRO_PROP_PRECISION = "precision";
  public static final String AVRO_PROP_SCALE = "scale";
  public static final String AVRO_PROP_MAX_LENGTH = "maxLength";
  public static final String AVRO_STRING_TYPE_NAME = "string";
  public static final String AVRO_INT_TYPE_NAME = "int";
  public static final String AVRO_LONG_TYPE_NAME = "long";

  private ObjectInspector oi;
  private List<String> columnNames;
  private List<TypeInfo> columnTypes;
  private Schema schema;
  private AvroDeserializer avroDeserializer = null;
  private AvroSerializer avroSerializer = null;

  private boolean badSchema = false;

  @Override
  public void initialize(Configuration configuration, Properties tableProperties,
                         Properties partitionProperties) throws SerDeException {
    // Avro should always use the table properties for initialization (see HIVE-6835).
    initialize(configuration, tableProperties);
  }

  @Override
  public void initialize(Configuration configuration, Properties properties) throws SerDeException {
    // Reset member variables so we don't get in a half-constructed state
    if (schema != null) {
      LOG.info("Resetting already initialized AvroSerDe");
    }

    schema = null;
    oi = null;
    columnNames = null;
    columnTypes = null;

    final String columnNameProperty = properties.getProperty(serdeConstants.LIST_COLUMNS);
    final String columnTypeProperty = properties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    final String columnCommentProperty = properties.getProperty(LIST_COLUMN_COMMENTS,"");

    if (hasExternalSchema(properties)
        || columnNameProperty == null || columnNameProperty.isEmpty()
        || columnTypeProperty == null || columnTypeProperty.isEmpty()) {
      schema = determineSchemaOrReturnErrorSchema(configuration, properties);
    } else {
      // Get column names and sort order
      columnNames = Arrays.asList(columnNameProperty.split(","));
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);

      schema = getSchemaFromCols(properties, columnNames, columnTypes, columnCommentProperty);
      properties.setProperty(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(), schema.toString());
    }

    LOG.info("Avro schema is " + schema);

    if (configuration == null) {
      LOG.info("Configuration null, not inserting schema");
    } else {
      configuration.set(
          AvroSerdeUtils.AvroTableProperties.AVRO_SERDE_SCHEMA.getPropName(), schema.toString(false));
    }

    badSchema = schema.equals(SchemaResolutionProblem.SIGNAL_BAD_SCHEMA);

    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(schema);
    this.columnNames = aoig.getColumnNames();
    this.columnTypes = aoig.getColumnTypes();
    this.oi = aoig.getObjectInspector();
  }

  private boolean hasExternalSchema(Properties properties) {
    return properties.getProperty(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName()) != null
        || properties.getProperty(AvroSerdeUtils.AvroTableProperties.SCHEMA_URL.getPropName()) != null;
  }

  private boolean hasExternalSchema(Map<String, String> tableParams) {
    return tableParams.containsKey(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName())
        || tableParams.containsKey(AvroSerdeUtils.AvroTableProperties.SCHEMA_URL.getPropName());
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
      LOG.info("columnComments is " + columnCommentProperty);
    }
    if (columnNames.size() != columnTypes.size()) {
      throw new IllegalArgumentException("AvroSerde initialization failed. Number of column " +
          "name and column type differs. columnNames = " + columnNames + ", columnTypes = " +
          columnTypes);
    }

    final String tableName = properties.getProperty(TABLE_NAME);
    final String tableComment = properties.getProperty(TABLE_COMMENT);
    TypeInfoToSchema typeInfoToSchema = new TypeInfoToSchema();
    return typeInfoToSchema.convert(columnNames, columnTypes, columnComments,
        properties.getProperty(AvroSerdeUtils.AvroTableProperties.SCHEMA_NAMESPACE.getPropName()),
        properties.getProperty(AvroSerdeUtils.AvroTableProperties.SCHEMA_NAME.getPropName(), tableName),
        properties.getProperty(AvroSerdeUtils.AvroTableProperties.SCHEMA_DOC.getPropName(), tableComment));

  }

  /**
   * Attempt to determine the schema via the usual means, but do not throw
   * an exception if we fail.  Instead, signal failure via a special
   * schema.  This is used because Hive calls init on the serde during
   * any call, including calls to update the serde properties, meaning
   * if the serde is in a bad state, there is no way to update that state.
   */
  public Schema determineSchemaOrReturnErrorSchema(Configuration conf, Properties props) {
    try {
      configErrors = "";
      return AvroSerdeUtils.determineSchemaOrThrowException(conf, props);
    } catch(AvroSerdeException he) {
      LOG.warn("Encountered AvroSerdeException determining schema. Returning " +
              "signal schema to indicate problem", he);
      configErrors = new String("Encountered AvroSerdeException determining schema. Returning " +
              "signal schema to indicate problem: " + he.getMessage());
      return schema = SchemaResolutionProblem.SIGNAL_BAD_SCHEMA;
    } catch (Exception e) {
      LOG.warn("Encountered exception determining schema. Returning signal " +
              "schema to indicate problem", e);
      configErrors = new String("Encountered exception determining schema. Returning signal " +
              "schema to indicate problem: " + e.getMessage());
      return SchemaResolutionProblem.SIGNAL_BAD_SCHEMA;
    }
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return AvroGenericRecordWritable.class;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
    if(badSchema) {
      throw new BadSchemaException();
    }
    return getSerializer().serialize(o, objectInspector, columnNames, columnTypes, schema);
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    if(badSchema) {
      throw new BadSchemaException();
    }
    return getDeserializer().deserialize(columnNames, columnTypes, writable, schema);
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return oi;
  }

  @Override
  public SerDeStats getSerDeStats() {
    // No support for statistics. That seems to be a popular answer.
    return null;
  }

  private AvroDeserializer getDeserializer() {
    if(avroDeserializer == null) {
      avroDeserializer = new AvroDeserializer();
    }

    return avroDeserializer;
  }

  private AvroSerializer getSerializer() {
    if(avroSerializer == null) {
      avroSerializer = new AvroSerializer();
    }

    return avroSerializer;
  }

  @Override
  public boolean shouldStoreFieldsInMetastore(Map<String, String> tableParams) {
    return !hasExternalSchema(tableParams);
  }
}
