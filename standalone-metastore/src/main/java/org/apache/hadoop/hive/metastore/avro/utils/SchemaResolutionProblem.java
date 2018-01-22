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
package org.apache.hadoop.hive.metastore.avro.utils;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.StorageSchemaReader;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * This class is copied from SchemaResolutionProblem from the Hive source code as part of metastore
 * separation
 */
class SchemaResolutionProblem {
  static final String sentinelString = "{\n" +
        "    \"namespace\": \"org.apache.hadoop.hive\",\n" +
        "    \"name\": \"CannotDetermineSchemaSentinel\",\n" +
        "    \"type\": \"record\",\n" +
        "    \"fields\": [\n" +
        "        {\n" +
        "            \"name\":\"ERROR_ERROR_ERROR_ERROR_ERROR_ERROR_ERROR\",\n" +
        "            \"type\":\"string\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"name\":\"Cannot_determine_schema\",\n" +
        "            \"type\":\"string\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"name\":\"check\",\n" +
        "            \"type\":\"string\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"name\":\"schema\",\n" +
        "            \"type\":\"string\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"name\":\"url\",\n" +
        "            \"type\":\"string\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"name\":\"and\",\n" +
        "            \"type\":\"string\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"name\":\"literal\",\n" +
        "            \"type\":\"string\"\n" +
        "        }\n" +
        "    ]\n" +
        "}";
  public final static Schema SIGNAL_BAD_SCHEMA = AvroSchemaUtils.getSchemaFor(sentinelString);

  public static class AvroStorageSchemaReader implements StorageSchemaReader {
    private static final Logger LOG = LoggerFactory.getLogger(AvroStorageSchemaReader.class);

    @Override
    public List<FieldSchema> readSchema(Table tbl, EnvironmentContext envContext,
        Configuration conf) throws MetaException {
      Properties tblMetadataProperties = MetaStoreUtils.getTableMetadata(tbl);
      try {
        return AvroSchemaUtils.getFieldsFromAvroSchema(conf, tblMetadataProperties);
      } catch (AvroSerdeException e) {
        LOG.warn("Exception received while reading avro schema for table " + tbl.getTableName(), e);
        throw new MetaException(e.getMessage());
      } catch (IOException e) {
        LOG.warn("Received IOException while reading avro schema for table " + tbl.getTableName(), e);
        throw new MetaException(e.getMessage());
      }
    }
  }
}
