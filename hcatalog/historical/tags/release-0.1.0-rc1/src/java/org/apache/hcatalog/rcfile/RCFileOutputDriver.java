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
package org.apache.hcatalog.rcfile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatOutputStorageDriver;

/**
 * The storage driver for writing RCFile data through HCatOutputFormat.
 */
 public class RCFileOutputDriver extends HCatOutputStorageDriver {

   /** The serde for serializing the HCatRecord to bytes writable */
   private SerDe serde;

   /** The object inspector for the given schema */
   private StructObjectInspector objectInspector;

   /** The schema for the output data */
   private HCatSchema outputSchema;

   /** The cached RCFile output format instance */
   private OutputFormat outputFormat = null;

  /* (non-Javadoc)
   * @see org.apache.hcatalog.mapreduce.HCatOutputStorageDriver#convertValue(org.apache.hcatalog.data.HCatRecord)
   */
  @Override
  public Writable convertValue(HCatRecord value) throws IOException {
    try {

      return serde.serialize(value.getAll(), objectInspector);
    } catch(SerDeException e) {
      throw new IOException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hcatalog.mapreduce.HCatOutputStorageDriver#generateKey(org.apache.hcatalog.data.HCatRecord)
   */
  @Override
  public WritableComparable<?> generateKey(HCatRecord value) throws IOException {
    //key is not used for RCFile output
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.hcatalog.mapreduce.HCatOutputStorageDriver#getOutputFormat(java.util.Properties)
   */
  @SuppressWarnings("unchecked")
  @Override
  public OutputFormat<? super WritableComparable<?>, ? super Writable> getOutputFormat() throws IOException {
    if( outputFormat == null ) {
      outputFormat = new RCFileMapReduceOutputFormat();
    }

    return outputFormat;
  }

  /* (non-Javadoc)
   * @see org.apache.hcatalog.mapreduce.HCatOutputStorageDriver#setOutputPath(org.apache.hadoop.mapreduce.JobContext, java.lang.String)
   */
  @Override
  public void setOutputPath(JobContext jobContext, String location) throws IOException {
    //Not calling FileOutputFormat.setOutputPath since that requires a Job instead of JobContext
    jobContext.getConfiguration().set("mapred.output.dir", location);
  }

  /* (non-Javadoc)
   * @see org.apache.hcatalog.mapreduce.HCatOutputStorageDriver#setPartitionValues(org.apache.hadoop.mapreduce.JobContext, java.util.Map)
   */
  @Override
  public void setPartitionValues(JobContext jobContext, Map<String, String> partitionValues)
      throws IOException {
    //default implementation of HCatOutputStorageDriver.getPartitionLocation will use the partition
    //values to generate the data location, so partition values not used here
  }

  /* (non-Javadoc)
   * @see org.apache.hcatalog.mapreduce.HCatOutputStorageDriver#setSchema(org.apache.hadoop.mapreduce.JobContext, org.apache.hadoop.hive.metastore.api.Schema)
   */
  @Override
  public void setSchema(JobContext jobContext, HCatSchema schema) throws IOException {
    outputSchema = schema;
    RCFileMapReduceOutputFormat.setColumnNumber(
        jobContext.getConfiguration(), schema.getFields().size());
  }

  @Override
  public void initialize(JobContext context,Properties hcatProperties) throws IOException {

    super.initialize(context, hcatProperties);

    List<FieldSchema> fields = HCatUtil.getFieldSchemaList(outputSchema.getFields());
    hcatProperties.setProperty(Constants.LIST_COLUMNS,
          MetaStoreUtils.getColumnNamesFromFieldSchema(fields));
    hcatProperties.setProperty(Constants.LIST_COLUMN_TYPES,
          MetaStoreUtils.getColumnTypesFromFieldSchema(fields));

    // setting these props to match LazySimpleSerde
    hcatProperties.setProperty(Constants.SERIALIZATION_NULL_FORMAT, "\\N");
    hcatProperties.setProperty(Constants.SERIALIZATION_FORMAT, "1");

    try {
      serde = new ColumnarSerDe();
      serde.initialize(context.getConfiguration(), hcatProperties);
      objectInspector = createStructObjectInspector();

    } catch (SerDeException e) {
      throw new IOException(e);
    }
  }

  public StructObjectInspector createStructObjectInspector() throws IOException {

    if( outputSchema == null ) {
      throw new IOException("Invalid output schema specified");
    }

    List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
    List<String> fieldNames = new ArrayList<String>();

    for(HCatFieldSchema hcatFieldSchema : outputSchema.getFields()) {
      TypeInfo type = TypeInfoUtils.getTypeInfoFromTypeString(hcatFieldSchema.getTypeString());

      fieldNames.add(hcatFieldSchema.getName());
      fieldInspectors.add(getObjectInspector(type));
    }

    StructObjectInspector structInspector = ObjectInspectorFactory.
        getStandardStructObjectInspector(fieldNames, fieldInspectors);
    return structInspector;
  }

  public ObjectInspector getObjectInspector(TypeInfo type) throws IOException {

    switch(type.getCategory()) {

    case PRIMITIVE :
      PrimitiveTypeInfo primitiveType = (PrimitiveTypeInfo) type;
      return PrimitiveObjectInspectorFactory.
        getPrimitiveJavaObjectInspector(primitiveType.getPrimitiveCategory());

    case MAP :
      MapTypeInfo mapType = (MapTypeInfo) type;
      MapObjectInspector mapInspector = ObjectInspectorFactory.getStandardMapObjectInspector(
          getObjectInspector(mapType.getMapKeyTypeInfo()), getObjectInspector(mapType.getMapValueTypeInfo()));
      return mapInspector;

    case LIST :
      ListTypeInfo listType = (ListTypeInfo) type;
      ListObjectInspector listInspector = ObjectInspectorFactory.getStandardListObjectInspector(
          getObjectInspector(listType.getListElementTypeInfo()));
      return listInspector;

    case STRUCT :
      StructTypeInfo structType = (StructTypeInfo) type;
      List<TypeInfo> fieldTypes = structType.getAllStructFieldTypeInfos();

      List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
      for(TypeInfo fieldType : fieldTypes) {
        fieldInspectors.add(getObjectInspector(fieldType));
      }

      StructObjectInspector structInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
          structType.getAllStructFieldNames(), fieldInspectors);
      return structInspector;

    default :
      throw new IOException("Unknown field schema type");
    }
  }

}
