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
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.data.schema.HCatFieldSchema.Type;
import org.apache.hcatalog.mapreduce.HCatOutputCommitter;
import org.apache.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hcatalog.mapreduce.HCatTableInfo;
import org.apache.pig.PigException;
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
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;

/**
 * HowlStorer.
 *
 */

public class HCatStorer extends StoreFunc implements StoreMetadata {

  /**
   *
   */
  private static final String COMPUTED_OUTPUT_SCHEMA = "howl.output.schema";
  private final Map<String,String> partitions;
  private Schema pigSchema;
  private RecordWriter<WritableComparable<?>, HCatRecord> writer;
  private HCatSchema computedSchema;
  private static final String PIG_SCHEMA = "howl.pig.store.schema";
  private String sign;

  public HCatStorer(String partSpecs, String schema) throws ParseException, FrontendException {

    partitions = new HashMap<String, String>();
    if(partSpecs != null && !partSpecs.trim().isEmpty()){
      String[] partKVPs = partSpecs.split(",");
      for(String partKVP : partKVPs){
        String[] partKV = partKVP.split("=");
        if(partKV.length == 2) {
          partitions.put(partKV[0].trim(), partKV[1].trim());
        } else {
          throw new FrontendException("Invalid partition column specification. "+partSpecs, PigHCatUtil.PIG_EXCEPTION_CODE);
        }
      }
    }

    if(schema != null) {
      pigSchema = Utils.getSchemaFromString(schema);
    }

  }

  public HCatStorer(String partSpecs) throws ParseException, FrontendException {
    this(partSpecs, null);
  }

  public HCatStorer() throws FrontendException, ParseException{
    this(null,null);
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
            "returned by Pig run-time. Schema provided in HowlStorer: "+pigSchema.toString()+ " Schema received from Pig runtime: "+runtimeSchema.toString(), PigHCatUtil.PIG_EXCEPTION_CODE);
      }
    } else {
      pigSchema = runtimeSchema;
    }
    UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[]{sign}).setProperty(PIG_SCHEMA,ObjectSerializer.serialize(pigSchema));
  }

  /** Constructs HCatSchema from pigSchema. Passed tableSchema is the existing
   * schema of the table in metastore.
   */
  private HCatSchema convertPigSchemaToHCatSchema(Schema pigSchema, HCatSchema tableSchema) throws FrontendException{

    List<HCatFieldSchema> fieldSchemas = new ArrayList<HCatFieldSchema>(pigSchema.size());
    for(FieldSchema fSchema : pigSchema.getFields()){
      byte type = fSchema.type;
      HCatFieldSchema howlFSchema;

      try {

        // Find out if we need to throw away the tuple or not.
        if(type == DataType.BAG && removeTupleFromBag(tableSchema, fSchema)){
          List<HCatFieldSchema> arrFields = new ArrayList<HCatFieldSchema>(1);
          arrFields.add(getHowlFSFromPigFS(fSchema.schema.getField(0).schema.getField(0)));
          howlFSchema = new HCatFieldSchema(fSchema.alias, Type.ARRAY, new HCatSchema(arrFields), null);
      }
      else{
          howlFSchema = getHowlFSFromPigFS(fSchema);
      }
      fieldSchemas.add(howlFSchema);
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


  private HCatFieldSchema getHowlFSFromPigFS(FieldSchema fSchema) throws FrontendException, HCatException{

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
      arrFields.add(getHowlFSFromPigFS(bagSchema.getField(0)));
      return new HCatFieldSchema(fSchema.alias, Type.ARRAY, new HCatSchema(arrFields), "");

    case DataType.TUPLE:
      List<String> fieldNames = new ArrayList<String>();
      List<HCatFieldSchema> howlFSs = new ArrayList<HCatFieldSchema>();
      for( FieldSchema fieldSchema : fSchema.schema.getFields()){
        fieldNames.add( fieldSchema.alias);
        howlFSs.add(getHowlFSFromPigFS(fieldSchema));
      }
      return new HCatFieldSchema(fSchema.alias, Type.STRUCT, new HCatSchema(howlFSs), "");

    case DataType.MAP:{
      // Pig's schema contain no type information about map's keys and
      // values. So, if its a new column assume <string,string> if its existing
      // return whatever is contained in the existing column.
      HCatFieldSchema mapField = getTableCol(fSchema.alias, howlTblSchema);
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
  public OutputFormat getOutputFormat() throws IOException {
    return new HCatOutputFormat();
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

  private Object getJavaObj(Object pigObj, HCatFieldSchema howlFS) throws ExecException, HCatException{

    // The real work-horse. Spend time and energy in this method if there is
    // need to keep HowlStorer lean and go fast.
    Type type = howlFS.getType();

    switch(type){

    case STRUCT:
      // Unwrap the tuple.
      return ((Tuple)pigObj).getAll();
      //        Tuple innerTup = (Tuple)pigObj;
      //
      //      List<Object> innerList = new ArrayList<Object>(innerTup.size());
      //      int i = 0;
      //      for(HowlTypeInfo structFieldTypeInfo : typeInfo.getAllStructFieldTypeInfos()){
      //        innerList.add(getJavaObj(innerTup.get(i++), structFieldTypeInfo));
      //      }
      //      return innerList;
    case ARRAY:
      // Unwrap the bag.
      DataBag pigBag = (DataBag)pigObj;
      HCatFieldSchema tupFS = howlFS.getArrayElementSchema().get(0);
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


  private void doSchemaValidations(Schema pigSchema, HCatSchema tblSchema) throws FrontendException, HCatException{

    // Iterate through all the elements in Pig Schema and do validations as
    // dictated by semantics, consult HCatSchema of table when need be.

    for(FieldSchema pigField : pigSchema.getFields()){
      byte type = pigField.type;
      String alias = pigField.alias;
      validateAlias(alias);
      HCatFieldSchema howlField = getTableCol(alias, tblSchema);

      if(DataType.isComplex(type)){
        switch(type){

        case DataType.MAP:
          if(howlField != null){
            if(howlField.getMapKeyType() != Type.STRING){
              throw new FrontendException("Key Type of map must be String "+howlField,  PigHCatUtil.PIG_EXCEPTION_CODE);
            }
            if(howlField.getMapValueSchema().get(0).isComplex()){
              throw new FrontendException("Value type of map cannot be complex" + howlField, PigHCatUtil.PIG_EXCEPTION_CODE);
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
          if(howlField != null){
            // Do the same validation for HCatSchema.
            HCatFieldSchema arrayFieldScehma = howlField.getArrayElementSchema().get(0);
            Type hType = arrayFieldScehma.getType();
            if(hType == Type.STRUCT){
              for(HCatFieldSchema structFieldInBag : arrayFieldScehma.getStructSubSchema().getFields()){
                if(structFieldInBag.getType() == Type.STRUCT || structFieldInBag.getType() == Type.ARRAY){
                  throw new FrontendException("Nested Complex types not allowed "+ howlField, PigHCatUtil.PIG_EXCEPTION_CODE);
                }
              }
            }
            if(hType == Type.MAP){
              if(arrayFieldScehma.getMapKeyType() != Type.STRING){
                throw new FrontendException("Key Type of map must be String "+howlField, PigHCatUtil.PIG_EXCEPTION_CODE);
              }
              if(arrayFieldScehma.getMapValueSchema().get(0).isComplex()){
                throw new FrontendException("Value type of map cannot be complex "+howlField, PigHCatUtil.PIG_EXCEPTION_CODE);
              }
            }
            if(hType == Type.ARRAY) {
              throw new FrontendException("Arrays cannot contain array within it. "+howlField, PigHCatUtil.PIG_EXCEPTION_CODE);
            }
          }
          break;

        case DataType.TUPLE:
          validateUnNested(pigField.schema);
          if(howlField != null){
            for(HCatFieldSchema structFieldSchema : howlField.getStructSubSchema().getFields()){
              if(structFieldSchema.isComplex()){
                throw new FrontendException("Nested Complex types are not allowed."+howlField, PigHCatUtil.PIG_EXCEPTION_CODE);
              }
            }
          }
          break;

        default:
          throw new FrontendException("Internal Error.", PigHCatUtil.PIG_EXCEPTION_CODE);
        }
      }
    }

    for(HCatFieldSchema howlField : tblSchema.getFields()){

      // We dont do type promotion/demotion.
      Type hType = howlField.getType();
      switch(hType){
      case SMALLINT:
      case TINYINT:
      case BOOLEAN:
        throw new FrontendException("Incompatible type found in howl table schema: "+howlField, PigHCatUtil.PIG_EXCEPTION_CODE);
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

    for(HCatFieldSchema howlField : tblSchema.getFields()){
      if(howlField.getName().equalsIgnoreCase(alias)){
        return howlField;
      }
    }
    // Its a new column
    return null;
  }
  HCatSchema howlTblSchema;

  @Override
  public void cleanupOnFailure(String location, Job job) throws IOException {
    // No-op.
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {

    Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[]{sign});

    String[] userStr = location.split("\\.");
    HCatTableInfo tblInfo;
    if(userStr.length == 2) {
      tblInfo = HCatTableInfo.getOutputTableInfo(PigHCatUtil.getHowlServerUri(job),
          PigHCatUtil.getHowlServerPrincipal(job), userStr[0],userStr[1],partitions);
    } else {
      tblInfo = HCatTableInfo.getOutputTableInfo(PigHCatUtil.getHowlServerUri(job),
          PigHCatUtil.getHowlServerPrincipal(job), null,userStr[0],partitions);
    }



    Configuration config = job.getConfiguration();
    if(!HCatUtil.checkJobContextIfRunningFromBackend(job)){

      Schema schema = (Schema)ObjectSerializer.deserialize(p.getProperty(PIG_SCHEMA));
      if(schema != null){
        pigSchema = schema;
      }
      if(pigSchema == null){
        throw new FrontendException("Schema for data cannot be determined.", PigHCatUtil.PIG_EXCEPTION_CODE);
      }
      try{
        HCatOutputFormat.setOutput(job, tblInfo);
      } catch(HCatException he) {
          // pass the message to the user - essentially something about the table
          // information passed to HCatOutputFormat was not right
          throw new PigException(he.getMessage(), PigHCatUtil.PIG_EXCEPTION_CODE, he);
      }
      howlTblSchema = HCatOutputFormat.getTableSchema(job);
      try{
        doSchemaValidations(pigSchema, howlTblSchema);
      } catch(HCatException he){
        throw new FrontendException(he.getMessage(), PigHCatUtil.PIG_EXCEPTION_CODE, he);
      }
      computedSchema = convertPigSchemaToHCatSchema(pigSchema,howlTblSchema);
      HCatOutputFormat.setSchema(job, computedSchema);
      p.setProperty(HCatConstants.HCAT_KEY_OUTPUT_INFO, config.get(HCatConstants.HCAT_KEY_OUTPUT_INFO));
      if(config.get(HCatConstants.HCAT_KEY_HIVE_CONF) != null){
        p.setProperty(HCatConstants.HCAT_KEY_HIVE_CONF, config.get(HCatConstants.HCAT_KEY_HIVE_CONF));
      }
      if(config.get(HCatConstants.HCAT_KEY_TOKEN_SIGNATURE) != null){
        p.setProperty(HCatConstants.HCAT_KEY_TOKEN_SIGNATURE,
            config.get(HCatConstants.HCAT_KEY_TOKEN_SIGNATURE));
      }
      p.setProperty(COMPUTED_OUTPUT_SCHEMA,ObjectSerializer.serialize(computedSchema));

    }else{
      config.set(HCatConstants.HCAT_KEY_OUTPUT_INFO, p.getProperty(HCatConstants.HCAT_KEY_OUTPUT_INFO));
      if(p.getProperty(HCatConstants.HCAT_KEY_HIVE_CONF) != null){
        config.set(HCatConstants.HCAT_KEY_HIVE_CONF, p.getProperty(HCatConstants.HCAT_KEY_HIVE_CONF));
      }
      if(p.getProperty(HCatConstants.HCAT_KEY_TOKEN_SIGNATURE) != null){
        config.set(HCatConstants.HCAT_KEY_TOKEN_SIGNATURE,
            p.getProperty(HCatConstants.HCAT_KEY_TOKEN_SIGNATURE));
      }
    }
  }

  @Override
  public void storeSchema(ResourceSchema schema, String arg1, Job job) throws IOException {
    if( job.getConfiguration().get("mapred.job.tracker", "").equalsIgnoreCase("local") ) {
      //In local mode, mapreduce will not call HowlOutputCommitter.cleanupJob.
      //Calling it from here so that the partition publish happens.
      //This call needs to be removed after MAPREDUCE-1447 is fixed.
      new HCatOutputCommitter(null).cleanupJob(job);
    }
  }

  @Override
  public void storeStatistics(ResourceStatistics stats, String arg1, Job job) throws IOException {
  }
}
