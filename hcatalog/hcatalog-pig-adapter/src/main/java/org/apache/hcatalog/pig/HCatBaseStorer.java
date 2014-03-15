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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hcatalog.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
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
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
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
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.pig.HCatBaseStorer} instead
 */

abstract class HCatBaseStorer extends StoreFunc implements StoreMetadata {

  private static final List<Type> SUPPORTED_INTEGER_CONVERSIONS =
    Lists.newArrayList(Type.TINYINT, Type.SMALLINT, Type.INT);
  protected static final String COMPUTED_OUTPUT_SCHEMA = "hcat.output.schema";
  protected final List<String> partitionKeys;
  protected final Map<String, String> partitions;
  protected Schema pigSchema;
  private RecordWriter<WritableComparable<?>, HCatRecord> writer;
  protected HCatSchema computedSchema;
  protected static final String PIG_SCHEMA = "hcat.pig.store.schema";
  protected String sign;

  public HCatBaseStorer(String partSpecs, String schema) throws Exception {

    partitionKeys = new ArrayList<String>();
    partitions = new HashMap<String, String>();
    if (partSpecs != null && !partSpecs.trim().isEmpty()) {
      String[] partKVPs = partSpecs.split(",");
      for (String partKVP : partKVPs) {
        String[] partKV = partKVP.split("=");
        if (partKV.length == 2) {
          String partKey = partKV[0].trim();
          partitionKeys.add(partKey);
          partitions.put(partKey, partKV[1].trim());
        } else {
          throw new FrontendException("Invalid partition column specification. " + partSpecs, PigHCatUtil.PIG_EXCEPTION_CODE);
        }
      }
    }

    if (schema != null) {
      pigSchema = Utils.getSchemaFromString(schema);
    }

  }

  @Override
  public void checkSchema(ResourceSchema resourceSchema) throws IOException {

    /*  Schema provided by user and the schema computed by Pig
    * at the time of calling store must match.
    */
    Schema runtimeSchema = Schema.getPigSchema(resourceSchema);
    if (pigSchema != null) {
      if (!Schema.equals(runtimeSchema, pigSchema, false, true)) {
        throw new FrontendException("Schema provided in store statement doesn't match with the Schema" +
          "returned by Pig run-time. Schema provided in HCatStorer: " + pigSchema.toString() + " Schema received from Pig runtime: " + runtimeSchema.toString(), PigHCatUtil.PIG_EXCEPTION_CODE);
      }
    } else {
      pigSchema = runtimeSchema;
    }
    UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[]{sign}).setProperty(PIG_SCHEMA, ObjectSerializer.serialize(pigSchema));
  }

  /** Constructs HCatSchema from pigSchema. Passed tableSchema is the existing
   * schema of the table in metastore.
   */
  protected HCatSchema convertPigSchemaToHCatSchema(Schema pigSchema, HCatSchema tableSchema) throws FrontendException {
    List<HCatFieldSchema> fieldSchemas = new ArrayList<HCatFieldSchema>(pigSchema.size());
    for (FieldSchema fSchema : pigSchema.getFields()) {
      try {
        HCatFieldSchema hcatFieldSchema = getColFromSchema(fSchema.alias, tableSchema);

        fieldSchemas.add(getHCatFSFromPigFS(fSchema, hcatFieldSchema));
      } catch (HCatException he) {
        throw new FrontendException(he.getMessage(), PigHCatUtil.PIG_EXCEPTION_CODE, he);
      }
    }
    return new HCatSchema(fieldSchemas);
  }

  public static boolean removeTupleFromBag(HCatFieldSchema hcatFieldSchema, FieldSchema bagFieldSchema) throws HCatException {
    if (hcatFieldSchema != null && hcatFieldSchema.getArrayElementSchema().get(0).getType() != Type.STRUCT) {
      return true;
    }
    // Column was not found in table schema. Its a new column
    List<FieldSchema> tupSchema = bagFieldSchema.schema.getFields();
    if (hcatFieldSchema == null && tupSchema.size() == 1 && (tupSchema.get(0).schema == null || (tupSchema.get(0).type == DataType.TUPLE && tupSchema.get(0).schema.size() == 1))) {
      return true;
    }
    return false;
  }


  private HCatFieldSchema getHCatFSFromPigFS(FieldSchema fSchema, HCatFieldSchema hcatFieldSchema) throws FrontendException, HCatException {
    byte type = fSchema.type;
    switch (type) {

    case DataType.CHARARRAY:
    case DataType.BIGCHARARRAY:
      return new HCatFieldSchema(fSchema.alias, Type.STRING, null);

    case DataType.INTEGER:
      if (hcatFieldSchema != null) {
        if (!SUPPORTED_INTEGER_CONVERSIONS.contains(hcatFieldSchema.getType())) {
          throw new FrontendException("Unsupported type: " + type + "  in Pig's schema",
            PigHCatUtil.PIG_EXCEPTION_CODE);
        }
        return new HCatFieldSchema(fSchema.alias, hcatFieldSchema.getType(), null);
      } else {
        return new HCatFieldSchema(fSchema.alias, Type.INT, null);
      }

    case DataType.LONG:
      return new HCatFieldSchema(fSchema.alias, Type.BIGINT, null);

    case DataType.FLOAT:
      return new HCatFieldSchema(fSchema.alias, Type.FLOAT, null);

    case DataType.DOUBLE:
      return new HCatFieldSchema(fSchema.alias, Type.DOUBLE, null);

    case DataType.BYTEARRAY:
      return new HCatFieldSchema(fSchema.alias, Type.BINARY, null);

    case DataType.BAG:
      Schema bagSchema = fSchema.schema;
      List<HCatFieldSchema> arrFields = new ArrayList<HCatFieldSchema>(1);
      FieldSchema field;
      // Find out if we need to throw away the tuple or not.
      if (removeTupleFromBag(hcatFieldSchema, fSchema)) {
        field = bagSchema.getField(0).schema.getField(0);
      } else {
        field = bagSchema.getField(0);
      }
      arrFields.add(getHCatFSFromPigFS(field, hcatFieldSchema == null ? null : hcatFieldSchema.getArrayElementSchema().get(0)));
      return new HCatFieldSchema(fSchema.alias, Type.ARRAY, new HCatSchema(arrFields), "");

    case DataType.TUPLE:
      List<String> fieldNames = new ArrayList<String>();
      List<HCatFieldSchema> hcatFSs = new ArrayList<HCatFieldSchema>();
      HCatSchema structSubSchema = hcatFieldSchema == null ? null : hcatFieldSchema.getStructSubSchema();
      List<FieldSchema> fields = fSchema.schema.getFields();
      for (int i = 0; i < fields.size(); i++) {
        FieldSchema fieldSchema = fields.get(i);
        fieldNames.add(fieldSchema.alias);
        hcatFSs.add(getHCatFSFromPigFS(fieldSchema, structSubSchema == null ? null : structSubSchema.get(i)));
      }
      return new HCatFieldSchema(fSchema.alias, Type.STRUCT, new HCatSchema(hcatFSs), "");

    case DataType.MAP: {
      // Pig's schema contain no type information about map's keys and
      // values. So, if its a new column assume <string,string> if its existing
      // return whatever is contained in the existing column.

      HCatFieldSchema valFS;
      List<HCatFieldSchema> valFSList = new ArrayList<HCatFieldSchema>(1);

      if (hcatFieldSchema != null) {
        return new HCatFieldSchema(fSchema.alias, Type.MAP, Type.STRING, hcatFieldSchema.getMapValueSchema(), "");
      }

      // Column not found in target table. Its a new column. Its schema is map<string,string>
      valFS = new HCatFieldSchema(fSchema.alias, Type.STRING, "");
      valFSList.add(valFS);
      return new HCatFieldSchema(fSchema.alias, Type.MAP, Type.STRING, new HCatSchema(valFSList), "");
    }

    default:
      throw new FrontendException("Unsupported type: " + type + "  in Pig's schema", PigHCatUtil.PIG_EXCEPTION_CODE);
    }
  }

  @Override
  public void prepareToWrite(RecordWriter writer) throws IOException {
    this.writer = writer;
    computedSchema = (HCatSchema) ObjectSerializer.deserialize(UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[]{sign}).getProperty(COMPUTED_OUTPUT_SCHEMA));
  }

  @Override
  public void putNext(Tuple tuple) throws IOException {

    List<Object> outgoing = new ArrayList<Object>(tuple.size());

    int i = 0;
    for (HCatFieldSchema fSchema : computedSchema.getFields()) {
      outgoing.add(getJavaObj(tuple.get(i++), fSchema));
    }
    try {
      writer.write(null, new DefaultHCatRecord(outgoing));
    } catch (InterruptedException e) {
      throw new BackendException("Error while writing tuple: " + tuple, PigHCatUtil.PIG_EXCEPTION_CODE, e);
    }
  }

  private Object getJavaObj(Object pigObj, HCatFieldSchema hcatFS) throws HCatException, BackendException {
    try {

      // The real work-horse. Spend time and energy in this method if there is
      // need to keep HCatStorer lean and go fast.
      Type type = hcatFS.getType();
      switch (type) {

      case BINARY:
        if (pigObj == null) {
          return null;
        }
        return ((DataByteArray) pigObj).get();

      case STRUCT:
        if (pigObj == null) {
          return null;
        }
        HCatSchema structSubSchema = hcatFS.getStructSubSchema();
        // Unwrap the tuple.
        List<Object> all = ((Tuple) pigObj).getAll();
        ArrayList<Object> converted = new ArrayList<Object>(all.size());
        for (int i = 0; i < all.size(); i++) {
          converted.add(getJavaObj(all.get(i), structSubSchema.get(i)));
        }
        return converted;

      case ARRAY:
        if (pigObj == null) {
          return null;
        }
        // Unwrap the bag.
        DataBag pigBag = (DataBag) pigObj;
        HCatFieldSchema tupFS = hcatFS.getArrayElementSchema().get(0);
        boolean needTuple = tupFS.getType() == Type.STRUCT;
        List<Object> bagContents = new ArrayList<Object>((int) pigBag.size());
        Iterator<Tuple> bagItr = pigBag.iterator();

        while (bagItr.hasNext()) {
          // If there is only one element in tuple contained in bag, we throw away the tuple.
          bagContents.add(getJavaObj(needTuple ? bagItr.next() : bagItr.next().get(0), tupFS));

        }
        return bagContents;
      case MAP:
        if (pigObj == null) {
          return null;
        }
        Map<?, ?> pigMap = (Map<?, ?>) pigObj;
        Map<Object, Object> typeMap = new HashMap<Object, Object>();
        for (Entry<?, ?> entry : pigMap.entrySet()) {
          // the value has a schema and not a FieldSchema
          typeMap.put(
            // Schema validation enforces that the Key is a String
            (String) entry.getKey(),
            getJavaObj(entry.getValue(), hcatFS.getMapValueSchema().get(0)));
        }
        return typeMap;
      case STRING:
      case INT:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
        return pigObj;
      case SMALLINT:
        if (pigObj == null) {
          return null;
        }
        if ((Integer) pigObj < Short.MIN_VALUE || (Integer) pigObj > Short.MAX_VALUE) {
          throw new BackendException("Value " + pigObj + " is outside the bounds of column " +
            hcatFS.getName() + " with type " + hcatFS.getType(), PigHCatUtil.PIG_EXCEPTION_CODE);
        }
        return ((Integer) pigObj).shortValue();
      case TINYINT:
        if (pigObj == null) {
          return null;
        }
        if ((Integer) pigObj < Byte.MIN_VALUE || (Integer) pigObj > Byte.MAX_VALUE) {
          throw new BackendException("Value " + pigObj + " is outside the bounds of column " +
            hcatFS.getName() + " with type " + hcatFS.getType(), PigHCatUtil.PIG_EXCEPTION_CODE);
        }
        return ((Integer) pigObj).byteValue();
      case BOOLEAN:
        // would not pass schema validation anyway
        throw new BackendException("Incompatible type " + type + " found in hcat table schema: " + hcatFS, PigHCatUtil.PIG_EXCEPTION_CODE);
      default:
        throw new BackendException("Unexpected type " + type + " for value " + pigObj + (pigObj == null ? "" : " of class " + pigObj.getClass().getName()), PigHCatUtil.PIG_EXCEPTION_CODE);
      }
    } catch (BackendException e) {
      // provide the path to the field in the error message
      throw new BackendException(
        (hcatFS.getName() == null ? " " : hcatFS.getName() + ".") + e.getMessage(),
        e.getCause() == null ? e : e.getCause());
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


  protected void doSchemaValidations(Schema pigSchema, HCatSchema tblSchema) throws FrontendException, HCatException {

    // Iterate through all the elements in Pig Schema and do validations as
    // dictated by semantics, consult HCatSchema of table when need be.

    for (FieldSchema pigField : pigSchema.getFields()) {
      HCatFieldSchema hcatField = getColFromSchema(pigField.alias, tblSchema);
      validateSchema(pigField, hcatField);
    }

    try {
      PigHCatUtil.validateHCatTableSchemaFollowsPigRules(tblSchema);
    } catch (IOException e) {
      throw new FrontendException("HCatalog schema is not compatible with Pig: " + e.getMessage(), PigHCatUtil.PIG_EXCEPTION_CODE, e);
    }
  }


  private void validateSchema(FieldSchema pigField, HCatFieldSchema hcatField)
    throws HCatException, FrontendException {
    validateAlias(pigField.alias);
    byte type = pigField.type;
    if (DataType.isComplex(type)) {
      switch (type) {

      case DataType.MAP:
        if (hcatField != null) {
          if (hcatField.getMapKeyType() != Type.STRING) {
            throw new FrontendException("Key Type of map must be String " + hcatField, PigHCatUtil.PIG_EXCEPTION_CODE);
          }
          // Map values can be primitive or complex
        }
        break;

      case DataType.BAG:
        HCatSchema arrayElementSchema = hcatField == null ? null : hcatField.getArrayElementSchema();
        for (FieldSchema innerField : pigField.schema.getField(0).schema.getFields()) {
          validateSchema(innerField, getColFromSchema(pigField.alias, arrayElementSchema));
        }
        break;

      case DataType.TUPLE:
        HCatSchema structSubSchema = hcatField == null ? null : hcatField.getStructSubSchema();
        for (FieldSchema innerField : pigField.schema.getFields()) {
          validateSchema(innerField, getColFromSchema(pigField.alias, structSubSchema));
        }
        break;

      default:
        throw new FrontendException("Internal Error.", PigHCatUtil.PIG_EXCEPTION_CODE);
      }
    }
  }

  private void validateAlias(String alias) throws FrontendException {
    if (alias == null) {
      throw new FrontendException("Column name for a field is not specified. Please provide the full schema as an argument to HCatStorer.", PigHCatUtil.PIG_EXCEPTION_CODE);
    }
    if (alias.matches(".*[A-Z]+.*")) {
      throw new FrontendException("Column names should all be in lowercase. Invalid name found: " + alias, PigHCatUtil.PIG_EXCEPTION_CODE);
    }
  }

  // Finds column by name in HCatSchema, if not found returns null.
  private HCatFieldSchema getColFromSchema(String alias, HCatSchema tblSchema) {
    if (tblSchema != null) {
      for (HCatFieldSchema hcatField : tblSchema.getFields()) {
        if (hcatField != null && hcatField.getName() != null && hcatField.getName().equalsIgnoreCase(alias)) {
          return hcatField;
        }
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
