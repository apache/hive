/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.type.MetastoreTypeInfo;
import org.apache.hadoop.hive.metastore.utils.AvroSchemaUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.StorageSchemaUtils;

import org.apache.hadoop.hive.serde2.avro.AvroSerdeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.hadoop.hive.metastore.ColumnType.LIST_COLUMN_COMMENTS;

/**
 * Default StorageSchemaReader.  This just throws as the metastore currently doesn't know how to
 * read schemas from storage.
 */
public class DefaultStorageSchemaReader implements StorageSchemaReader {
  private final static Logger LOG = LoggerFactory.getLogger(DefaultStorageSchemaReader.class);

  private static final String AVRO_SERIALIZATION_LIB =
      "org.apache.hadoop.hive.serde2.avro.AvroSerDe";

  @Override
  public List<FieldSchema> readSchema(Table tbl, EnvironmentContext envContext,
      Configuration conf) throws MetaException {
    String serializationLib = tbl.getSd().getSerdeInfo().getSerializationLib();
    if (null == serializationLib || MetastoreConf
        .getStringCollection(conf, MetastoreConf.ConfVars.SERDES_USING_METASTORE_FOR_SCHEMA)
        .contains(serializationLib)) {
      //safety check to make sure we should be using storage schema reader for this table
      throw new MetaException(
          "Invalid usage of default storage schema reader for table " + tbl.getTableName()
              + " with storage descriptor " + tbl.getSd().getSerdeInfo().getSerializationLib());
    }
    Properties tblMetadataProperties = MetaStoreUtils.getTableMetadata(tbl);
    if(AVRO_SERIALIZATION_LIB.equals(serializationLib)) {
      //in case of avro table use AvroStorageSchemaReader utils
      try {
        return AvroSchemaUtils.getFieldsFromAvroSchema(conf, tblMetadataProperties);
      } catch (AvroSerdeException e) {
        LOG.warn("Exception received while reading avro schema for table " + tbl.getTableName(), e);
        throw new MetaException(e.getMessage());
      } catch (IOException e) {
        LOG.warn("Exception received while reading avro schema for table " + tbl.getTableName(), e);
        throw new MetaException(e.getMessage());
      }
    } else {
      return getFieldSchemasFromTableMetadata(tblMetadataProperties);
    }
  }

  /**
   * This method implements a generic way to get the FieldSchemas from the table metadata
   * properties like column names and column types. Most of the serdes have the same implemention
   * in their initialize method
   * //TODO refactor the common code from the serdes and move it to serde-api so that there is no
   * //duplicate code
   *
   * @return list of FieldSchema objects
   */
  public static List<FieldSchema> getFieldSchemasFromTableMetadata(
      Properties tblMetadataProperties) {
    List<String> columnNames = null;
    List< MetastoreTypeInfo> columnTypes = null;
    // Get column names and types
    String columnNameProperty = tblMetadataProperties.getProperty( ColumnType.LIST_COLUMNS);
    String columnTypeProperty = tblMetadataProperties.getProperty( ColumnType.LIST_COLUMN_TYPES);
    final String columnNameDelimiter = tblMetadataProperties
        .containsKey( ColumnType.COLUMN_NAME_DELIMITER) ? tblMetadataProperties
        .getProperty( ColumnType.COLUMN_NAME_DELIMITER) : String
        .valueOf(StorageSchemaUtils.COMMA);
    // all table column names
    if (columnNameProperty.isEmpty()) {
      columnNames = Collections.emptyList();
    } else {
      columnNames = Arrays.asList(columnNameProperty.split(columnNameDelimiter));
    }

    // all column types
    if (columnTypeProperty.isEmpty()) {
      columnTypes = Collections.emptyList();
    } else {
      columnTypes = StorageSchemaUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    final String columnCommentProperty =
        tblMetadataProperties.getProperty(LIST_COLUMN_COMMENTS, "");
    List<String> columnComments = null;
    if (columnCommentProperty == null || columnCommentProperty.isEmpty()) {
      columnComments = new ArrayList<>(0);
    } else {
      columnComments = Arrays.asList(
          columnCommentProperty.split(String.valueOf(ColumnType.COLUMN_COMMENTS_DELIMITER)));
    }
    LOG.debug("columns: {}, {}", columnNameProperty, columnNames);
    LOG.debug("types: {}, {} ", columnTypeProperty, columnTypes);
    LOG.debug("comments: {} ", columnCommentProperty);
    return getFieldSchemaFromColumnInfo(columnNames, columnTypes, columnComments);
  }

  private static List<FieldSchema> getFieldSchemaFromColumnInfo(List<String> columnNames,
      List<MetastoreTypeInfo> columnTypes, List<String> columnComments) {
    int len = columnNames.size();
    List<FieldSchema> fieldSchemas = new ArrayList<>(len);
    for (int i = 0; i < len; i++) {
      FieldSchema fieldSchema = new FieldSchema();
      fieldSchema.setName(columnNames.get(i));
      //In case of complex types getTypeName() will recusively go into typeName
      //of individual fields when the ColumnType was constructed
      //in SchemaToTypeInfo.generateColumnTypes in the constructor
      fieldSchema.setType(columnTypes.get(i).getTypeName());
      fieldSchema.setComment(StorageSchemaUtils.determineFieldComment(columnComments.get(i)));
    }
    return fieldSchemas;
  }
}
