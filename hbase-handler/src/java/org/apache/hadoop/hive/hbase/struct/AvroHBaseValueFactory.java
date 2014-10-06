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
package org.apache.hadoop.hive.hbase.struct;

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.hbase.HBaseSerDeParameters;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.avro.AvroLazyObjectInspector;
import org.apache.hadoop.hive.serde2.avro.AvroSchemaRetriever;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Avro specific implementation of the {@link HBaseValueFactory}
 * */
public class AvroHBaseValueFactory extends DefaultHBaseValueFactory {

  private AvroSchemaRetriever avroSchemaRetriever;
  private Schema schema;

  /**
   * Constructor
   * 
   * @param schema the associated {@link Schema schema}
   * */
  public AvroHBaseValueFactory(int fieldID, Schema schema) {
    super(fieldID);
    this.schema = schema;
  }

  @Override
  public void init(HBaseSerDeParameters hbaseParams, Configuration conf, Properties properties)
      throws SerDeException {
    super.init(hbaseParams, conf, properties);
    String avroSchemaRetClass = properties.getProperty(AvroSerdeUtils.SCHEMA_RETRIEVER);

    if (avroSchemaRetClass != null) {
      Class<?> avroSchemaRetrieverClass = null;
      try {
        avroSchemaRetrieverClass = conf.getClassByName(avroSchemaRetClass);
      } catch (ClassNotFoundException e) {
        throw new SerDeException(e);
      }

      initAvroSchemaRetriever(avroSchemaRetrieverClass, conf, properties);
    }
  }

  @Override
  public ObjectInspector createValueObjectInspector(TypeInfo type) throws SerDeException {
    ObjectInspector oi =
        LazyFactory.createLazyObjectInspector(type, serdeParams.getSeparators(), 1,
        serdeParams.getNullSequence(), serdeParams.isEscaped(), serdeParams.getEscapeChar(),
        ObjectInspectorOptions.AVRO);

    // initialize the object inspectors
    initInternalObjectInspectors(oi);

    return oi;
  }

  @Override
  public byte[] serializeValue(Object object, StructField field) throws IOException {
    // Explicit avro serialization not supported yet. Revert to default
    return super.serializeValue(object, field);
  }
  
  /**
   * Initialize the instance for {@link AvroSchemaRetriever}
   *
   * @throws SerDeException
   * */
  private void initAvroSchemaRetriever(Class<?> avroSchemaRetrieverClass, Configuration conf,
      Properties tbl) throws SerDeException {

    try {
      avroSchemaRetriever = (AvroSchemaRetriever) avroSchemaRetrieverClass.getDeclaredConstructor(
          Configuration.class, Properties.class).newInstance(
          conf, tbl);
    } catch (NoSuchMethodException e) {
      // the constructor wasn't defined in the implementation class. Flag error
      throw new SerDeException("Constructor not defined in schema retriever class [" + avroSchemaRetrieverClass.getName() + "]", e);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  /**
   * Initialize the internal object inspectors
   * */
  private void initInternalObjectInspectors(ObjectInspector oi) {
    if (oi instanceof AvroLazyObjectInspector) {
      initAvroObjectInspector(oi);
    } else if (oi instanceof MapObjectInspector) {
      // we found a map objectinspector. Grab the objectinspector for the value and initialize it
      // aptly
      ObjectInspector valueOI = ((MapObjectInspector) oi).getMapValueObjectInspector();

      if (valueOI instanceof AvroLazyObjectInspector) {
        initAvroObjectInspector(valueOI);
      }
    }
  }

  /**
   * Recursively initialize the {@link AvroLazyObjectInspector} and all its nested ois
   *
   * @param oi ObjectInspector to be recursively initialized
   * @param schema {@link Schema} to be initialized with
   * @param schemaRetriever class to be used to retrieve schema
   * */
  private void initAvroObjectInspector(ObjectInspector oi) {
    // Check for a list. If found, recursively init its members
    if (oi instanceof ListObjectInspector) {
      ListObjectInspector loi = (ListObjectInspector) oi;

      initAvroObjectInspector(loi.getListElementObjectInspector());
      return;
    }

    // Check for a nested message. If found, set the schema, else return.
    if (!(oi instanceof AvroLazyObjectInspector)) {
      return;
    }

    AvroLazyObjectInspector aoi = (AvroLazyObjectInspector) oi;

    aoi.setSchemaRetriever(avroSchemaRetriever);
    aoi.setReaderSchema(schema);

    // call the method recursively over all the internal fields of the given avro
    // objectinspector
    for (StructField field : aoi.getAllStructFieldRefs()) {
      initAvroObjectInspector(field.getFieldObjectInspector());
    }
  }
}