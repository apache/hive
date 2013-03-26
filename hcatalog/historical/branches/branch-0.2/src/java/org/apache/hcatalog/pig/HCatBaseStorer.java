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

package org.apache.hcatalog.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatFieldSchema.Type;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreMetadata;
import org.apache.pig.backend.BackendException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;

/**
 * Base class for HCatStorer and HCatEximStorer
 *
 */

public abstract class HCatBaseStorer extends StoreFunc implements StoreMetadata {

  /**
   *
   */
  protected static final String COMPUTED_OUTPUT_SCHEMA = "hcat.output.schema";
  protected final List<String> partitionKeys;
  protected final Map<String,String> partitions;
  protected Schema pigSchema;
  private RecordWriter<WritableComparable<?>, HCatRecord> writer;
  protected HCatSchema computedSchema;
  protected static final String PIG_SCHEMA = "hcat.pig.store.schema";
  protected String sign;

  public HCatBaseStorer(String partSpecs, String schema) throws Exception {

    partitionKeys = new ArrayList<String>();
    partitions = new HashMap<String, String>();
    if(partSpecs != null && !partSpecs.trim().isEmpty()){
      String[] partKVPs = partSpecs.split(",");
      for(String partKVP : partKVPs){
        String[] partKV = partKVP.split("=");
        if(partKV.length == 2) {
          String partKey = partKV[0].trim(); 
          partitionKeys.add(partKey);
          partitions.put(partKey, partKV[1].trim());
        } else {
          throw new FrontendException("Invalid partition column specification. "+partSpecs, PigHCatUtil.PIG_EXCEPTION_CODE);
        }
      }
    }

    if(schema != null) {
      pigSchema = Utils.getSchemaFromString(schema);
    }

  }

  @Override
  public void checkSchema(ResourceSchema resourceSchema) throws IOException {

    /*  Schema provided by user and the schema computed by Pig
     * at the time of calling store must match.
     */
    Schema runtimeSchema = Schema.getPigSchema(resourceSchema);
    if(pigSchema != null){
      if(! Schema.equals(runtimeSchema, pigSchema, false, true) ){
        throw new FrontendException("Schema provided in store statement doesn't match with the Schema" +
            "returned by Pig run-time. Schema provided in HCatStorer: "+pigSchema.toString()+ " Schema received from Pig runtime: "+runtimeSchema.toString(), PigHCatUtil.PIG_EXCEPTION_CODE);
      }
    } else {
      pigSchema = runtimeSchema;
    }
    UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[]{sign}).setProperty(PIG_SCHEMA,ObjectSerializer.serialize(pigSchema));
  }

  /** Constructs HCatSchema from pigSchema. Passed tableSchema is the existing
   * schema of the table in metastore.
   */
  protected HCatSchema convertPigSchemaToHCatSchema(Schema pigSchema, HCatSchema tableSchema) throws FrontendException{

    List<HCatFieldSchema> fieldSchemas = new ArrayList<HCatFieldSchema>(pigSchema.size());
    for(FieldSchema fSchema : pigSchema.getFields()){
      byte type = fSchema.type;
      HCatFieldSchema hcatFSchema;

      try {

        // Find out if we need to throw away the tuple or not.
        if(type == DataType.BAG && removeTupleFromBag(tableSchema, fSchema)){
          List<HCatFieldSchema> arrFields = new ArrayList<HCatFieldSchema>(1);
          arrFields.add(getHCatFSFromPigFS(fSchema.schema.getField(0).schema.getField(0), tableSchema));
          hcatFSchema = new HCatFieldSchema(fSchema.alias, Type.ARRAY, new HCatSchema(arrFields), null);
      }
      else{
          hcatFSchema = getHCatFSFromPigFS(fSchema, tableSchema);
      }
      fieldSchemas.add(hcatFSchema);
      } catch (HCatException he){
          throw new FrontendException(he.getMessage(),PigHCatUtil.PIG_EXCEPTION_CODE,he);
      }
    }

    return new HCatSchema(fieldSchemas);
  }

  private void validateUnNested(Schema innerSchema) throws FrontendException{

    for(FieldSchema innerField : innerSchema.getFields()){
      validateAlias(innerField.alias);
      if(DataType.isComplex(innerField.type)) {
        throw new FrontendException("Complex types cannot be nested. "+innerField, PigHCatUtil.PIG_EXCEPTION_CODE);
      }
    }
  }

  private boolean removeTupleFromBag(HCatSchema tableSchema, FieldSchema bagFieldSchema) throws HCatException{

    String colName = bagFieldSchema.alias;
    for(HCatFieldSchema field : tableSchema.getFields()){
      if(colName.equalsIgnoreCase(field.getName())){
        return (field.getArrayElementSchema().get(0).getType() == Type.STRUCT) ? false : true;
      }
    }
    // Column was not found in table schema. Its a new column
    List<FieldSchema> tupSchema = bagFieldSchema.schema.getFields();
    return (tupSchema.size() == 1 && tupSchema.get(0).schema == null) ? true : false;
  }


  private HCatFieldSchema getHCatFSFromPigFS(FieldSchema fSchema, HCatSchema hcatTblSchema) throws FrontendException, HCatException{

    byte type = fSchema.type;
    switch(type){

    case DataType.CHARARRAY:
    case DataType.BIGCHARARRAY:
      return new HCatFieldSchema(fSchema.alias, Type.STRING, null);

    case DataType.INTEGER:
      return new HCatFieldSchema(fSchema.alias, Type.INT, null);

    case DataType.LONG:
      return new HCatFieldSchema(fSchema.alias, Type.BIGINT, null);

    case DataType.FLOAT:
      return new HCatFieldSchema(fSchema.alias, Type.FLOAT, null);

    case DataType.DOUBLE:
      return new HCatFieldSchema(fSchema.alias, Type.DOUBLE, null);

    case DataType.BAG:
      Schema bagSchema = fSchema.schema;
      List<HCatFieldSchema> arrFields = new ArrayList<HCatFieldSchema>(1);
      arrFields.add(getHCatFSFromPigFS(bagSchema.getField(0), hcatTblSchema));
      return new HCatFieldSchema(fSchema.alias, Type.ARRAY, new HCatSchema(arrFields), "");

    case DataType.TUPLE:
      List<String> fieldNames = new ArrayList<String>();
      List<HCatFieldSchema> hcatFSs = new ArrayList<HCatFieldSchema>();
      for( FieldSchema fieldSchema : fSchema.schema.getFields()){
        fieldNames.add( fieldSchema.alias);
        hcatFSs.add(getHCatFSFromPigFS(fieldSchema, hcatTblSchema));
      }
      return new HCatFieldSchema(fSchema.alias, Type.STRUCT, new HCatSchema(hcatFSs), "");

    case DataType.MAP:{
      // Pig's schema contain no type information about map's keys and
      // values. So, if its a new column assume <string,string> if its existing
      // return whatever is contained in the existing column.
      HCatFieldSchema mapField = getTableCol(fSchema.alias, hcatTblSchema);
      HCatFieldSchema valFS;
      List<HCatFieldSchema> valFSList = new ArrayList<HCatFieldSchema>(1);

      if(mapField != null){
        Type mapValType = mapField.getMapValueSchema().get(0).getType();

        switch(mapValType){
        case STRING:
        case BIGINT:
        case INT:
        case FLOAT:
        case DOUBLE:
          valFS = new HCatFieldSchema(fSchema.alias, mapValType, null);
          break;
        default:
          throw new FrontendException("Only pig primitive types are supported as map value types.", PigHCatUtil.PIG_EXCEPTION_CODE);
        }
        valFSList.add(valFS);
        return new HCatFieldSchema(fSchema.alias,Type.MAP,Type.STRING, new HCatSchema(valFSList),"");
      }

      // Column not found in target table. Its a new column. Its schema is map<string,string>
      valFS = new HCatFieldSchema(fSchema.alias, Type.STRING, "");
      valFSList.add(valFS);
      return new HCatFieldSchema(fSchema.alias,Type.MAP,Type.STRING, new HCatSchema(valFSList),"");
     }

    default:
      throw new FrontendException("Unsupported type: "+type+"  in Pig's schema", PigHCatUtil.PIG_EXCEPTION_CODE);
    }
  }

  @Override
  public void prepareToWrite(RecordWriter writer) throws IOException {
    this.writer = writer;
    computedSchema = (HCatSchema)ObjectSerializer.deserialize(UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[]{sign}).getProperty(COMPUTED_OUTPUT_SCHEMA));
  }

  @Override
  public void putNext(Tuple tuple) throws IOException {

    List<Object> outgoing = new ArrayList<Object>(tuple.size());

    int i = 0;
    for(HCatFieldSchema fSchema : computedSchema.getFields()){
      outgoing.add(getJavaObj(tuple.get(i++), fSchema));
    }
    try {
      writer.write(null, new DefaultHCatRecord(outgoing));
    } catch (InterruptedException e) {
      throw new BackendException("Error while writing tuple: "+tuple, PigHCatUtil.PIG_EXCEPTION_CODE, e);
    }
  }

  private Object getJavaObj(Object pigObj, HCatFieldSchema hcatFS) throws ExecException, HCatException{

    // The real work-horse. Spend time and energy in this method if there is
    // need to keep HCatStorer lean and go fast.
    Type type = hcatFS.getType();

    switch(type){

    case STRUCT:
      // Unwrap the tuple.
      return ((Tuple)pigObj).getAll();
      //        Tuple innerTup = (Tuple)pigObj;
      //
      //      List<Object> innerList = new ArrayList<Object>(innerTup.size());
      //      int i = 0;
      //      for(HCatTypeInfo structFieldTypeInfo : typeInfo.getAllStructFieldTypeInfos()){
      //        innerList.add(getJavaObj(innerTup.get(i++), structFieldTypeInfo));
      //      }
      //      return innerList;
    case ARRAY:
      // Unwrap the bag.
      DataBag pigBag = (DataBag)pigObj;
      HCatFieldSchema tupFS = hcatFS.getArrayElementSchema().get(0);
      boolean needTuple = tupFS.getType() == Type.STRUCT;
      List<Object> bagContents = new ArrayList<Object>((int)pigBag.size());
      Iterator<Tuple> bagItr = pigBag.iterator();

      while(bagItr.hasNext()){
        // If there is only one element in tuple contained in bag, we throw away the tuple.
        bagContents.add(needTuple ? getJavaObj(bagItr.next(), tupFS) : bagItr.next().get(0));

      }
      return bagContents;

      //    case MAP:
      //     Map<String,DataByteArray> pigMap = (Map<String,DataByteArray>)pigObj;
      //     Map<String,Long> typeMap = new HashMap<String, Long>();
      //     for(Entry<String, DataByteArray> entry: pigMap.entrySet()){
      //       typeMap.put(entry.getKey(), new Long(entry.getValue().toString()));
      //     }
      //     return typeMap;
    default:
      return pigObj;
    }
  }

  @Override
  public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException {

    // Need to necessarily override this method since default impl assumes HDFS
    // based location string.
    return location;
  }

  @Override
  public void setStoreFuncUDFContextSignature(String signature) {
    sign = signature;
  }


  protected void doSchemaValidations(Schema pigSchema, HCatSchema tblSchema) throws FrontendException, HCatException{

    // Iterate through all the elements in Pig Schema and do validations as
    // dictated by semantics, consult HCatSchema of table when need be.

    for(FieldSchema pigField : pigSchema.getFields()){
      byte type = pigField.type;
      String alias = pigField.alias;
      validateAlias(alias);
      HCatFieldSchema hcatField = getTableCol(alias, tblSchema);

      if(DataType.isComplex(type)){
        switch(type){

        case DataType.MAP:
          if(hcatField != null){
            if(hcatField.getMapKeyType() != Type.STRING){
              throw new FrontendException("Key Type of map must be String "+hcatField,  PigHCatUtil.PIG_EXCEPTION_CODE);
            }
            if(hcatField.getMapValueSchema().get(0).isComplex()){
              throw new FrontendException("Value type of map cannot be complex" + hcatField, PigHCatUtil.PIG_EXCEPTION_CODE);
            }
          }
          break;

        case DataType.BAG:
          // Only map is allowed as complex type in tuples inside bag.
          for(FieldSchema innerField : pigField.schema.getField(0).schema.getFields()){
            if(innerField.type == DataType.BAG || innerField.type == DataType.TUPLE) {
              throw new FrontendException("Complex types cannot be nested. "+innerField, PigHCatUtil.PIG_EXCEPTION_CODE);
            }
            validateAlias(innerField.alias);
          }
          if(hcatField != null){
            // Do the same validation for HCatSchema.
            HCatFieldSchema arrayFieldScehma = hcatField.getArrayElementSchema().get(0);
            Type hType = arrayFieldScehma.getType();
            if(hType == Type.STRUCT){
              for(HCatFieldSchema structFieldInBag : arrayFieldScehma.getStructSubSchema().getFields()){
                if(structFieldInBag.getType() == Type.STRUCT || structFieldInBag.getType() == Type.ARRAY){
                  throw new FrontendException("Nested Complex types not allowed "+ hcatField, PigHCatUtil.PIG_EXCEPTION_CODE);
                }
              }
            }
            if(hType == Type.MAP){
              if(arrayFieldScehma.getMapKeyType() != Type.STRING){
                throw new FrontendException("Key Type of map must be String "+hcatField, PigHCatUtil.PIG_EXCEPTION_CODE);
              }
              if(arrayFieldScehma.getMapValueSchema().get(0).isComplex()){
                throw new FrontendException("Value type of map cannot be complex "+hcatField, PigHCatUtil.PIG_EXCEPTION_CODE);
              }
            }
            if(hType == Type.ARRAY) {
              throw new FrontendException("Arrays cannot contain array within it. "+hcatField, PigHCatUtil.PIG_EXCEPTION_CODE);
            }
          }
          break;

        case DataType.TUPLE:
          validateUnNested(pigField.schema);
          if(hcatField != null){
            for(HCatFieldSchema structFieldSchema : hcatField.getStructSubSchema().getFields()){
              if(structFieldSchema.isComplex()){
                throw new FrontendException("Nested Complex types are not allowed."+hcatField, PigHCatUtil.PIG_EXCEPTION_CODE);
              }
            }
          }
          break;

        default:
          throw new FrontendException("Internal Error.", PigHCatUtil.PIG_EXCEPTION_CODE);
        }
      }
    }

    for(HCatFieldSchema hcatField : tblSchema.getFields()){

      // We dont do type promotion/demotion.
      Type hType = hcatField.getType();
      switch(hType){
      case SMALLINT:
      case TINYINT:
      case BOOLEAN:
        throw new FrontendException("Incompatible type found in hcat table schema: "+hcatField, PigHCatUtil.PIG_EXCEPTION_CODE);
      }
    }
  }

  private void validateAlias(String alias) throws FrontendException{
    if(alias == null) {
      throw new FrontendException("Column name for a field is not specified. Please provide the full schema as an argument to HCatStorer.", PigHCatUtil.PIG_EXCEPTION_CODE);
    }
    if(alias.matches(".*[A-Z]+.*")) {
      throw new FrontendException("Column names should all be in lowercase. Invalid name found: "+alias, PigHCatUtil.PIG_EXCEPTION_CODE);
    }
  }

  // Finds column by name in HCatSchema, if not found returns null.
  private HCatFieldSchema getTableCol(String alias, HCatSchema tblSchema){

    for(HCatFieldSchema hcatField : tblSchema.getFields()){
      if(hcatField.getName().equalsIgnoreCase(alias)){
        return hcatField;
      }
    }
    // Its a new column
    return null;
  }

  @Override
  public void cleanupOnFailure(String location, Job job) throws IOException {
    // No-op.
  }

  @Override
  public void storeStatistics(ResourceStatistics stats, String arg1, Job job) throws IOException {
  }
}
