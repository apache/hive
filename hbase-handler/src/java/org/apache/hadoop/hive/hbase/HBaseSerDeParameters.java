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

package org.apache.hadoop.hive.hbase;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.hbase.ColumnMappings.ColumnMapping;
import org.apache.hadoop.hive.hbase.struct.AvroHBaseValueFactory;
import org.apache.hadoop.hive.hbase.struct.DefaultHBaseValueFactory;
import org.apache.hadoop.hive.hbase.struct.HBaseValueFactory;
import org.apache.hadoop.hive.hbase.struct.StructHBaseValueFactory;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.AvroTableProperties;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * HBaseSerDeParameters encapsulates SerDeParameters and additional configurations that are specific for
 * HBaseSerDe.
 *
 */
public class HBaseSerDeParameters {

  public static final String AVRO_SERIALIZATION_TYPE = "avro";
  public static final String STRUCT_SERIALIZATION_TYPE = "struct";

  private final LazySerDeParameters serdeParams;

  private final Configuration job;

  private final String columnMappingString;
  private final ColumnMappings columnMappings;
  private final boolean doColumnRegexMatching;
  private final boolean doColumnPrefixCut;

  private final long putTimestamp;
  private final HBaseKeyFactory keyFactory;
  private final List<HBaseValueFactory> valueFactories;

  HBaseSerDeParameters(Configuration job, Properties tbl, String serdeName) throws SerDeException {
    this.job = job;

    // Read configuration parameters
    columnMappingString = tbl.getProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING);
    doColumnRegexMatching =
        Boolean.parseBoolean(tbl.getProperty(HBaseSerDe.HBASE_COLUMNS_REGEX_MATCHING, "true"));
    doColumnPrefixCut = Boolean.parseBoolean(tbl.getProperty(HBaseSerDe.HBASE_COLUMNS_PREFIX_HIDE, "false"));
    // Parse and initialize the HBase columns mapping
    columnMappings = HBaseSerDe.parseColumnsMapping(columnMappingString, doColumnRegexMatching, doColumnPrefixCut);

    // Build the type property string if not supplied
    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    String autogenerate = tbl.getProperty(HBaseSerDe.HBASE_AUTOGENERATE_STRUCT);

    if (columnTypeProperty == null || columnTypeProperty.isEmpty()) {
      String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
      if (columnNameProperty == null || columnNameProperty.isEmpty()) {
        if (autogenerate == null || autogenerate.isEmpty()) {
          throw new IllegalArgumentException("Either the columns must be specified or the "
              + HBaseSerDe.HBASE_AUTOGENERATE_STRUCT + " property must be set to true.");
        }

        tbl.setProperty(serdeConstants.LIST_COLUMNS,
            columnMappings.toNamesString(tbl, autogenerate));
      }

      tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES,
          columnMappings.toTypesString(tbl, job, autogenerate));
    }

    this.serdeParams = new LazySerDeParameters(job, tbl, serdeName);
    this.putTimestamp = Long.parseLong(tbl.getProperty(HBaseSerDe.HBASE_PUT_TIMESTAMP, "-1"));

    columnMappings.setHiveColumnDescription(serdeName, serdeParams.getColumnNames(),
        serdeParams.getColumnTypes());

    // Precondition: make sure this is done after the rest of the SerDe initialization is done.
    String hbaseTableStorageType = tbl.getProperty(HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE);
    columnMappings.parseColumnStorageTypes(hbaseTableStorageType);

    this.keyFactory = initKeyFactory(job, tbl);
    this.valueFactories = initValueFactories(job, tbl);
  }

  public List<String> getColumnNames() {
    return serdeParams.getColumnNames();
  }

  public List<TypeInfo> getColumnTypes() {
    return serdeParams.getColumnTypes();
  }

  public LazySerDeParameters getSerdeParams() {
    return serdeParams;
  }

  public long getPutTimestamp() {
    return putTimestamp;
  }

  public int getKeyIndex() {
    return columnMappings.getKeyIndex();
  }

  public ColumnMapping getKeyColumnMapping() {
    return columnMappings.getKeyMapping();
  }

  public int getTimestampIndex() {
    return columnMappings.getTimestampIndex();
  }

  public ColumnMapping getTimestampColumnMapping() {
    return columnMappings.getTimestampMapping();
  }

  public ColumnMappings getColumnMappings() {
    return columnMappings;
  }

  public HBaseKeyFactory getKeyFactory() {
    return keyFactory;
  }

  public List<HBaseValueFactory> getValueFactories() {
    return valueFactories;
  }

  public Configuration getBaseConfiguration() {
    return job;
  }

  public TypeInfo getTypeForName(String columnName) {
    List<String> columnNames = serdeParams.getColumnNames();
    List<TypeInfo> columnTypes = serdeParams.getColumnTypes();
    for (int i = 0; i < columnNames.size(); i++) {
      if (columnName.equals(columnNames.get(i))) {
        return columnTypes.get(i);
      }
    }
    throw new IllegalArgumentException("Invalid column name " + columnName);
  }

  public String toString() {
    return "[" + columnMappingString + ":" + getColumnNames() + ":" + getColumnTypes() + "]";
  }

  private HBaseKeyFactory initKeyFactory(Configuration conf, Properties tbl) throws SerDeException {
    try {
      HBaseKeyFactory keyFactory = createKeyFactory(conf, tbl);
      if (keyFactory != null) {
        keyFactory.init(this, tbl);
      }
      return keyFactory;
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  private static HBaseKeyFactory createKeyFactory(Configuration job, Properties tbl)
      throws Exception {
    String factoryClassName = tbl.getProperty(HBaseSerDe.HBASE_COMPOSITE_KEY_FACTORY);
    if (factoryClassName != null) {
      Class<?> factoryClazz = loadClass(factoryClassName, job);
      return (HBaseKeyFactory) ReflectionUtils.newInstance(factoryClazz, job);
    }
    String keyClassName = tbl.getProperty(HBaseSerDe.HBASE_COMPOSITE_KEY_CLASS);
    if (keyClassName != null) {
      Class<?> keyClass = loadClass(keyClassName, job);
      return new CompositeHBaseKeyFactory(keyClass);
    }
    return new DefaultHBaseKeyFactory();
  }

  private static Class<?> loadClass(String className, @Nullable Configuration configuration)
      throws Exception {
    if (configuration != null) {
      return configuration.getClassByName(className);
    }
    return JavaUtils.loadClass(className);
  }

  private List<HBaseValueFactory> initValueFactories(Configuration conf, Properties tbl)
      throws SerDeException {
    List<HBaseValueFactory> valueFactories = createValueFactories(conf, tbl);

    for (HBaseValueFactory valueFactory : valueFactories) {
      valueFactory.init(this, conf, tbl);
    }

    return valueFactories;
  }

  private List<HBaseValueFactory> createValueFactories(Configuration conf, Properties tbl)
      throws SerDeException {
    List<HBaseValueFactory> valueFactories = new ArrayList<HBaseValueFactory>();

    try {
      for (int i = 0; i < columnMappings.size(); i++) {
        String serType = getSerializationType(conf, tbl, columnMappings.getColumnsMapping()[i]);

        if (AVRO_SERIALIZATION_TYPE.equals(serType)) {
          Schema schema = getSchema(conf, tbl, columnMappings.getColumnsMapping()[i]);
          valueFactories.add(new AvroHBaseValueFactory(i, schema));
        } else if (STRUCT_SERIALIZATION_TYPE.equals(serType)) {
          String structValueClassName = tbl.getProperty(HBaseSerDe.HBASE_STRUCT_SERIALIZER_CLASS);

          if (structValueClassName == null) {
            throw new IllegalArgumentException(HBaseSerDe.HBASE_STRUCT_SERIALIZER_CLASS
                + " must be set for hbase columns of type [" + STRUCT_SERIALIZATION_TYPE + "]");
          }

          Class<?> structValueClass = loadClass(structValueClassName, job);
          valueFactories.add(new StructHBaseValueFactory(i, structValueClass));
        } else {
          valueFactories.add(new DefaultHBaseValueFactory(i));
        }
      }
    } catch (Exception e) {
      throw new SerDeException(e);
    }

    return valueFactories;
  }

  /**
   * Get the type for the given {@link ColumnMapping colMap}
   * */
  private String getSerializationType(Configuration conf, Properties tbl,
      ColumnMapping colMap) throws Exception {
    String serType = null;

    if (colMap.qualifierName == null) {
      // only a column family

      if (colMap.qualifierPrefix != null) {
        serType = tbl.getProperty(colMap.familyName + "." + colMap.qualifierPrefix + "."
            + HBaseSerDe.SERIALIZATION_TYPE);
      } else {
        serType = tbl.getProperty(colMap.familyName + "." + HBaseSerDe.SERIALIZATION_TYPE);
      }
    } else if (!colMap.hbaseRowKey) {
      // not an hbase row key. This should either be a prefix or an individual qualifier
      String qualifierName = colMap.qualifierName;

      if (colMap.qualifierName.endsWith("*")) {
        qualifierName = colMap.qualifierName.substring(0, colMap.qualifierName.length() - 1);
      }

      serType =
          tbl.getProperty(colMap.familyName + "." + qualifierName + "."
              + HBaseSerDe.SERIALIZATION_TYPE);
    }

    return serType;
  }

  private Schema getSchema(Configuration conf, Properties tbl, ColumnMapping colMap)
      throws Exception {
    String serType = null;
    String serClassName = null;
    String schemaLiteral = null;
    String schemaUrl = null;

    if (colMap.qualifierName == null) {
      // only a column family

      if (colMap.qualifierPrefix != null) {
        serType =
            tbl.getProperty(colMap.familyName + "." + colMap.qualifierPrefix + "."
                + HBaseSerDe.SERIALIZATION_TYPE);

        serClassName =
            tbl.getProperty(colMap.familyName + "." + colMap.qualifierPrefix + "."
                + serdeConstants.SERIALIZATION_CLASS);

        schemaLiteral =
            tbl.getProperty(colMap.familyName + "." + colMap.qualifierPrefix + "."
                + AvroTableProperties.SCHEMA_LITERAL.getPropName());

        schemaUrl =
            tbl.getProperty(colMap.familyName + "." + colMap.qualifierPrefix + "."
                + AvroTableProperties.SCHEMA_URL.getPropName());
      } else {
        serType = tbl.getProperty(colMap.familyName + "." + HBaseSerDe.SERIALIZATION_TYPE);

        serClassName =
            tbl.getProperty(colMap.familyName + "." + serdeConstants.SERIALIZATION_CLASS);

        schemaLiteral = tbl.getProperty(colMap.familyName + "." + AvroTableProperties.SCHEMA_LITERAL.getPropName());

        schemaUrl = tbl.getProperty(colMap.familyName + "." + AvroTableProperties.SCHEMA_URL.getPropName());
      }
    } else if (!colMap.hbaseRowKey) {
      // not an hbase row key. This should either be a prefix or an individual qualifier
      String qualifierName = colMap.qualifierName;

      if (colMap.qualifierName.endsWith("*")) {
        qualifierName = colMap.qualifierName.substring(0, colMap.qualifierName.length() - 1);
      }

      serType =
          tbl.getProperty(colMap.familyName + "." + qualifierName + "."
              + HBaseSerDe.SERIALIZATION_TYPE);

      serClassName =
          tbl.getProperty(colMap.familyName + "." + qualifierName + "."
              + serdeConstants.SERIALIZATION_CLASS);

      schemaLiteral =
          tbl.getProperty(colMap.familyName + "." + qualifierName + "."
              + AvroTableProperties.SCHEMA_LITERAL.getPropName());

      schemaUrl =
          tbl.getProperty(colMap.familyName + "." + qualifierName + "." + AvroTableProperties.SCHEMA_URL.getPropName());
    }

    if (serType == null) {
      throw new IllegalArgumentException("serialization.type property is missing");
    }

    String avroSchemaRetClass = tbl.getProperty(AvroTableProperties.SCHEMA_RETRIEVER.getPropName());

    if (schemaLiteral == null && serClassName == null && schemaUrl == null
        && avroSchemaRetClass == null) {
      throw new IllegalArgumentException("serialization.type was set to [" + serType
          + "] but neither " + AvroTableProperties.SCHEMA_LITERAL.getPropName() + ", " + AvroTableProperties.SCHEMA_URL.getPropName()
          + ", serialization.class or " + AvroTableProperties.SCHEMA_RETRIEVER.getPropName() + " property was set");
    }

    Class<?> deserializerClass = null;

    if (serClassName != null) {
      deserializerClass = loadClass(serClassName, conf);
    }

    Schema schema = null;

    // only worry about getting schema if we are dealing with Avro
    if (serType.equalsIgnoreCase(AVRO_SERIALIZATION_TYPE)) {
      if (avroSchemaRetClass == null) {
        // bother about generating a schema only if a schema retriever class wasn't provided
        if (schemaLiteral != null) {
          schema = Schema.parse(schemaLiteral);
        } else if (schemaUrl != null) {
          schema = HBaseSerDeHelper.getSchemaFromFS(schemaUrl, conf);
        } else if (deserializerClass != null) {
          schema = ReflectData.get().getSchema(deserializerClass);
        }
      }
    }

    return schema;
  }
}
