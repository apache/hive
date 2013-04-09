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

import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;

/**
 * Read or write Avro data from Hive.
 */
public class AvroSerDe extends AbstractSerDe {
  private static final Log LOG = LogFactory.getLog(AvroSerDe.class);
  private ObjectInspector oi;
  private List<String> columnNames;
  private List<TypeInfo> columnTypes;
  private Schema schema;
  private AvroDeserializer avroDeserializer = null;
  private AvroSerializer avroSerializer = null;

  private boolean badSchema = false;

  @Override
  public void initialize(Configuration configuration, Properties properties) throws SerDeException {
    // Reset member variables so we don't get in a half-constructed state
    if(schema != null) {
      LOG.info("Resetting already initialized AvroSerDe");
    }

    schema = null;
    oi = null;
    columnNames  = null;
    columnTypes = null;

    schema =  AvroSerdeUtils.determineSchemaOrReturnErrorSchema(properties);
    if(configuration == null) {
      LOG.info("Configuration null, not inserting schema");
    } else {
      configuration.set(AvroSerdeUtils.AVRO_SERDE_SCHEMA, schema.toString(false));
    }

    badSchema = schema.equals(SchemaResolutionProblem.SIGNAL_BAD_SCHEMA);

    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(schema);
    this.columnNames = aoig.getColumnNames();
    this.columnTypes = aoig.getColumnTypes();
    this.oi = aoig.getObjectInspector();
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
}
