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

package org.apache.hive.hcatalog.pig;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Type;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
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
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Base class for HCatStorer and HCatEximStorer
 *
 */

abstract class HCatBaseStorer extends StoreFunc implements StoreMetadata {

  private static final Logger LOG = LoggerFactory.getLogger( HCatBaseStorer.class );

  private static final List<Type> SUPPORTED_INTEGER_CONVERSIONS =
    Lists.newArrayList(Type.TINYINT, Type.SMALLINT, Type.INT);
  protected static final String COMPUTED_OUTPUT_SCHEMA = "hcat.output.schema";
  protected final List<String> partitionKeys;
  protected final Map<String, String> partitions;
  protected Schema pigSchema;
  private RecordWriter<WritableComparable<?>, HCatRecord> writer;
  protected HCatSchema computedSchema;
  protected static final String PIG_SCHEMA = "hcat.pig.store.schema";
  /**
   * Controls what happens when incoming Pig value is out-of-range for target Hive column
   */
  static final String ON_OOR_VALUE_OPT = "onOutOfRangeValue";
  /**
   * prop name in Configuration/context
   */
  static final String ON_OORA_VALUE_PROP = "hcat.pig.store.onoutofrangevalue";
  /**
   * valid values for ON_OOR_VALUE_OPT
   */
  public static enum  OOR_VALUE_OPT_VALUES {Null, Throw}
  protected String sign;
  //it's key that this is a per HCatStorer instance object
  private final DataLossLogger dataLossLogger = new DataLossLogger();
  private final OOR_VALUE_OPT_VALUES onOutOfRange;

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

    if (schema != null && !schema.trim().isEmpty()) {
      pigSchema = Utils.getSchemaFromString(schema);
    }
    Properties udfProps = UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[]{sign});
    onOutOfRange = OOR_VALUE_OPT_VALUES.valueOf(udfProps.getProperty(ON_OORA_VALUE_PROP, getDefaultValue().name()));
  }
  static OOR_VALUE_OPT_VALUES getDefaultValue() {
    return OOR_VALUE_OPT_VALUES.Null;
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
    if(LOG.isDebugEnabled()) {
      LOG.debug("convertPigSchemaToHCatSchema(pigSchema,tblSchema)=(" + pigSchema + "," + tableSchema + ")");
    }
    List<HCatFieldSchema> fieldSchemas = new ArrayList<HCatFieldSchema>(pigSchema.size());
    for (FieldSchema fSchema : pigSchema.getFields()) {
      try {
        HCatFieldSchema hcatFieldSchema = getColFromSchema(fSchema.alias, tableSchema);
        //if writing to a partitioned table, then pigSchema will have more columns than tableSchema
        //partition columns are not part of tableSchema... e.g. TestHCatStorer#testPartColsInData()
//        HCatUtil.assertNotNull(hcatFieldSchema, "Nothing matching '" + fSchema.alias + "' found " +
//                "in target table schema", LOG);
        fieldSchemas.add(getHCatFSFromPigFS(fSchema, hcatFieldSchema, pigSchema, tableSchema));
      } catch (HCatException he) {
        throw new FrontendException(he.getMessage(), PigHCatUtil.PIG_EXCEPTION_CODE, he);
      }
    }
    
    HCatSchema s = new HCatSchema(fieldSchemas);
    LOG.debug("convertPigSchemaToHCatSchema(computed)=(" + s + ")");
    return s;
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
  /**
   * Here we are processing HCat table schema as derived from metastore, 
   * thus it should have information about all fields/sub-fields, but not for partition columns
   */
  private HCatFieldSchema getHCatFSFromPigFS(FieldSchema fSchema, HCatFieldSchema hcatFieldSchema,
                                             Schema pigSchema, HCatSchema tableSchema)
          throws FrontendException, HCatException {
    if(hcatFieldSchema == null) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("hcatFieldSchema is null for fSchema '" + fSchema.alias + "'");
        //throw new IllegalArgumentException("hcatFiledSchema is null; fSchema=" + fSchema + " " +
        //      "(pigSchema, tableSchema)=(" + pigSchema + "," + tableSchema + ")");
      }
    }
    byte type = fSchema.type;
    switch (type) {

    case DataType.CHARARRAY:
    case DataType.BIGCHARARRAY:
      if(hcatFieldSchema != null && hcatFieldSchema.getTypeInfo() != null) {
        return new HCatFieldSchema(fSchema.alias, hcatFieldSchema.getTypeInfo(), null);
      }
      return new HCatFieldSchema(fSchema.alias, TypeInfoFactory.stringTypeInfo, null);
    case DataType.INTEGER:
      if (hcatFieldSchema != null) {
        if (!SUPPORTED_INTEGER_CONVERSIONS.contains(hcatFieldSchema.getType())) {
          throw new FrontendException("Unsupported type: " + type + "  in Pig's schema",
            PigHCatUtil.PIG_EXCEPTION_CODE);
        }
        return new HCatFieldSchema(fSchema.alias, hcatFieldSchema.getTypeInfo(), null);
      }
      return new HCatFieldSchema(fSchema.alias, TypeInfoFactory.intTypeInfo, null);
    case DataType.LONG:
      return new HCatFieldSchema(fSchema.alias, TypeInfoFactory.longTypeInfo, null);
    case DataType.FLOAT:
      return new HCatFieldSchema(fSchema.alias, TypeInfoFactory.floatTypeInfo, null);
    case DataType.DOUBLE:
      return new HCatFieldSchema(fSchema.alias, TypeInfoFactory.doubleTypeInfo, null);
    case DataType.BYTEARRAY:
      return new HCatFieldSchema(fSchema.alias, TypeInfoFactory.binaryTypeInfo, null);
    case DataType.BOOLEAN:
      return new HCatFieldSchema(fSchema.alias, TypeInfoFactory.booleanTypeInfo, null);
    case DataType.DATETIME:
      //Pig DATETIME can map to DATE or TIMESTAMP (see HCatBaseStorer#validateSchema()) which
      //is controlled by Hive target table information
      if(hcatFieldSchema != null && hcatFieldSchema.getTypeInfo() != null) {
        return new HCatFieldSchema(fSchema.alias, hcatFieldSchema.getTypeInfo(), null);
      }
      return new HCatFieldSchema(fSchema.alias, TypeInfoFactory.timestampTypeInfo, null);
    case DataType.BIGDECIMAL:
      if(hcatFieldSchema != null && hcatFieldSchema.getTypeInfo() != null) {
        return new HCatFieldSchema(fSchema.alias, hcatFieldSchema.getTypeInfo(), null);
      }
      return new HCatFieldSchema(fSchema.alias, TypeInfoFactory.decimalTypeInfo, null);
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
      arrFields.add(getHCatFSFromPigFS(field, hcatFieldSchema == null ? null : hcatFieldSchema
              .getArrayElementSchema().get(0), pigSchema, tableSchema));
      return new HCatFieldSchema(fSchema.alias, Type.ARRAY, new HCatSchema(arrFields), "");
    case DataType.TUPLE:
      List<HCatFieldSchema> hcatFSs = new ArrayList<HCatFieldSchema>();
      HCatSchema structSubSchema = hcatFieldSchema == null ? null : hcatFieldSchema.getStructSubSchema();
      List<FieldSchema> fields = fSchema.schema.getFields();
      for (int i = 0; i < fields.size(); i++) {
        FieldSchema fieldSchema = fields.get(i);
        hcatFSs.add(getHCatFSFromPigFS(fieldSchema, structSubSchema == null ? null : structSubSchema.get(i), pigSchema, tableSchema));
      }
      return new HCatFieldSchema(fSchema.alias, Type.STRUCT, new HCatSchema(hcatFSs), "");
    case DataType.MAP: {
      // Pig's schema contain no type information about map's keys and
      // values. So, if its a new column assume <string,string> if its existing
      // return whatever is contained in the existing column.

      HCatFieldSchema valFS;
      List<HCatFieldSchema> valFSList = new ArrayList<HCatFieldSchema>(1);

      if (hcatFieldSchema != null) {
        return HCatFieldSchema.createMapTypeFieldSchema(fSchema.alias, hcatFieldSchema.getMapKeyTypeInfo(), 
          hcatFieldSchema.getMapValueSchema(), "");
      }

      // Column not found in target table. Its a new column. Its schema is map<string,string>
      valFS = new HCatFieldSchema(fSchema.alias, TypeInfoFactory.stringTypeInfo, "");
      valFSList.add(valFS);
      return HCatFieldSchema.createMapTypeFieldSchema(fSchema.alias,
        TypeInfoFactory.stringTypeInfo, new HCatSchema(valFSList), "");
    }
    case DataType.BIGINTEGER:
      //fall through; doesn't map to Hive/Hcat type; here for completeness
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

  /**
   * Convert from Pig value object to Hive value object
   * This method assumes that {@link #validateSchema(org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema, org.apache.hive.hcatalog.data.schema.HCatFieldSchema, org.apache.pig.impl.logicalLayer.schema.Schema, org.apache.hive.hcatalog.data.schema.HCatSchema, int)}
   * which checks the types in Pig schema are compatible with target Hive table, has been called.
   */
  private Object getJavaObj(Object pigObj, HCatFieldSchema hcatFS) throws HCatException, BackendException {
    try {
      if(pigObj == null) return null;
      // The real work-horse. Spend time and energy in this method if there is
      // need to keep HCatStorer lean and go fast.
      Type type = hcatFS.getType();
      switch (type) {
      case BINARY:
        return ((DataByteArray) pigObj).get();

      case STRUCT:
        HCatSchema structSubSchema = hcatFS.getStructSubSchema();
        // Unwrap the tuple.
        List<Object> all = ((Tuple) pigObj).getAll();
        ArrayList<Object> converted = new ArrayList<Object>(all.size());
        for (int i = 0; i < all.size(); i++) {
          converted.add(getJavaObj(all.get(i), structSubSchema.get(i)));
        }
        return converted;

      case ARRAY:
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
        if ((Integer) pigObj < Short.MIN_VALUE || (Integer) pigObj > Short.MAX_VALUE) {
          handleOutOfRangeValue(pigObj, hcatFS);
          return null;
        }
        return ((Integer) pigObj).shortValue();
      case TINYINT:
        if ((Integer) pigObj < Byte.MIN_VALUE || (Integer) pigObj > Byte.MAX_VALUE) {
          handleOutOfRangeValue(pigObj, hcatFS);
          return null;
        }
        return ((Integer) pigObj).byteValue();
      case BOOLEAN:
        if( pigObj instanceof String ) {
          if( ((String)pigObj).trim().compareTo("0") == 0 ) {
            return Boolean.FALSE;
          }
          if( ((String)pigObj).trim().compareTo("1") == 0 ) {
            return Boolean.TRUE;
          }
          throw new BackendException("Unexpected type " + type + " for value " + pigObj
            + " of class " + pigObj.getClass().getName(), PigHCatUtil.PIG_EXCEPTION_CODE);
        }
        return Boolean.parseBoolean( pigObj.toString() );
      case DECIMAL:
        BigDecimal bd = (BigDecimal)pigObj;
        DecimalTypeInfo dti = (DecimalTypeInfo)hcatFS.getTypeInfo();
        if(bd.precision() > dti.precision() || bd.scale() > dti.scale()) {
          handleOutOfRangeValue(pigObj, hcatFS);
          return null;
        }
        return HiveDecimal.create(bd);
      case CHAR:
        String charVal = (String)pigObj;
        CharTypeInfo cti = (CharTypeInfo)hcatFS.getTypeInfo(); 
        if(charVal.length() > cti.getLength()) {
          handleOutOfRangeValue(pigObj, hcatFS);
          return null;
        }
        return new HiveChar(charVal, cti.getLength());
      case VARCHAR:
        String varcharVal = (String)pigObj;
        VarcharTypeInfo vti = (VarcharTypeInfo)hcatFS.getTypeInfo();
        if(varcharVal.length() > vti.getLength()) {
          handleOutOfRangeValue(pigObj, hcatFS);
          return null;
        }
        return new HiveVarchar(varcharVal, vti.getLength());
      case TIMESTAMP:
        DateTime dt = (DateTime)pigObj;
        return new Timestamp(dt.getMillis());//getMillis() returns UTC time regardless of TZ
      case DATE:
        /**
         * We ignore any TZ setting on Pig value since java.sql.Date doesn't have it (in any
         * meaningful way).  So the assumption is that if Pig value has 0 time component (midnight)
         * we assume it reasonably 'fits' into a Hive DATE.  If time part is not 0, it's considered
         * out of range for target type.
         */
        DateTime dateTime = ((DateTime)pigObj);
        if(dateTime.getMillisOfDay() != 0) {
          handleOutOfRangeValue(pigObj, hcatFS, "Time component must be 0 (midnight) in local timezone; Local TZ val='" + pigObj + "'");
          return null;
        }
        /*java.sql.Date is a poorly defined API.  Some (all?) SerDes call toString() on it
        [e.g. LazySimpleSerDe, uses LazyUtils.writePrimitiveUTF8()],  which automatically adjusts
          for local timezone.  Date.valueOf() also uses local timezone (as does Date(int,int,int).
          Also see PigHCatUtil#extractPigObject() for corresponding read op.  This way a DATETIME from Pig,
          when stored into Hive and read back comes back with the same value.*/
        return new Date(dateTime.getYear() - 1900, dateTime.getMonthOfYear() - 1, dateTime.getDayOfMonth());
      default:
        throw new BackendException("Unexpected HCat type " + type + " for value " + pigObj
          + " of class " + pigObj.getClass().getName(), PigHCatUtil.PIG_EXCEPTION_CODE);
      }
    } catch (BackendException e) {
      // provide the path to the field in the error message
      throw new BackendException(
        (hcatFS.getName() == null ? " " : hcatFS.getName() + ".") + e.getMessage(), e);
    }
  }

  private void handleOutOfRangeValue(Object pigObj, HCatFieldSchema hcatFS) throws BackendException {
    handleOutOfRangeValue(pigObj, hcatFS, null);
  }
  /**
   * depending on user config, throws an exception or logs a msg if the incoming Pig value is
   * out-of-range for target type.
   * @param additionalMsg may be {@code null} 
   */
  private void handleOutOfRangeValue(Object pigObj, HCatFieldSchema hcatFS, String additionalMsg) throws BackendException {
    String msg = "Pig value '" + pigObj + "' is outside the bounds of column " + hcatFS.getName() +
      " with type " + (hcatFS.getTypeInfo() == null ? hcatFS.getType() : hcatFS.getTypeInfo().getTypeName()) +
      (additionalMsg == null ? "" : "[" + additionalMsg + "]");
    switch (onOutOfRange) {
      case Throw:
        throw new BackendException(msg, PigHCatUtil.PIG_EXCEPTION_CODE);
      case Null:
        dataLossLogger.logDataLossMsg(hcatFS, pigObj, msg);
        break;
      default:
        throw new BackendException("Unexpected " + ON_OOR_VALUE_OPT + " value: '" + onOutOfRange + "'");
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
    int columnPos = 0;//helps with debug messages
    for (FieldSchema pigField : pigSchema.getFields()) {
      HCatFieldSchema hcatField = getColFromSchema(pigField.alias, tblSchema);
      validateSchema(pigField, hcatField, pigSchema, tblSchema, columnPos++);
    }

    try {
      PigHCatUtil.validateHCatTableSchemaFollowsPigRules(tblSchema);
    } catch (IOException e) {
      throw new FrontendException("HCatalog schema is not compatible with Pig: " + e.getMessage(), PigHCatUtil.PIG_EXCEPTION_CODE, e);
    }
  }

  /**
   * This method encodes which Pig type can map (be stored in) to which HCat type.
   * @throws HCatException
   * @throws FrontendException
   */
  private void validateSchema(FieldSchema pigField, HCatFieldSchema hcatField, 
                              Schema topLevelPigSchema, HCatSchema topLevelHCatSchema, 
                              int columnPos)
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
          validateSchema(innerField, getColFromSchema(pigField.alias, arrayElementSchema), 
                  topLevelPigSchema, topLevelHCatSchema, columnPos);
        }
        break;

      case DataType.TUPLE:
        HCatSchema structSubSchema = hcatField == null ? null : hcatField.getStructSubSchema();
        for (FieldSchema innerField : pigField.schema.getFields()) {
          validateSchema(innerField, getColFromSchema(pigField.alias, structSubSchema),
                  topLevelPigSchema, topLevelHCatSchema, columnPos);
        }
        break;

      default:
        throw new FrontendException("Internal Error.", PigHCatUtil.PIG_EXCEPTION_CODE);
      }
    }
    else if(hcatField != null) {
      //there is no point trying to validate further if we have no type info about target field
      switch (type) {
        case DataType.BIGDECIMAL:
          throwTypeMismatchException(type, Lists.newArrayList(Type.DECIMAL), hcatField, columnPos);
          break;
        case DataType.DATETIME:
          throwTypeMismatchException(type, Lists.newArrayList(Type.TIMESTAMP, Type.DATE), hcatField, columnPos);
          break;
        case DataType.BYTEARRAY:
          throwTypeMismatchException(type, Lists.newArrayList(Type.BINARY), hcatField, columnPos);
          break;
        case DataType.BIGINTEGER:
          throwTypeMismatchException(type, Collections.<Type>emptyList(), hcatField, columnPos);
          break;
        case DataType.BOOLEAN:
          throwTypeMismatchException(type, Lists.newArrayList(Type.BOOLEAN), hcatField, columnPos);
          break;
        case DataType.CHARARRAY:
          throwTypeMismatchException(type, Lists.newArrayList(Type.STRING, Type.CHAR, Type.VARCHAR), 
                  hcatField, columnPos);
          break;
        case DataType.DOUBLE:
          throwTypeMismatchException(type, Lists.newArrayList(Type.DOUBLE), hcatField, columnPos);
          break;
        case DataType.FLOAT:
          throwTypeMismatchException(type, Lists.newArrayList(Type.FLOAT), hcatField, columnPos);
          break;
        case DataType.INTEGER:
          throwTypeMismatchException(type, Lists.newArrayList(Type.INT, Type.BIGINT, 
                  Type.TINYINT, Type.SMALLINT), hcatField, columnPos);
          break;
        case DataType.LONG:
          throwTypeMismatchException(type, Lists.newArrayList(Type.BIGINT), hcatField, columnPos);
          break;
        default:
          throw new FrontendException("'" + type + 
                  "' Pig datatype in column " + columnPos + "(0-based) is not supported by HCat", 
                  PigHCatUtil.PIG_EXCEPTION_CODE);
      }
    }
    else {
      if(false) {
        //see HIVE-6194
      throw new FrontendException("(pigSch,hcatSchema)=(" + pigField + "," +
              "" + hcatField + ") (topPig, topHcat)=(" + topLevelPigSchema + "," +
              "" + topLevelHCatSchema + ")");
      }
    }
  }
  private static void throwTypeMismatchException(byte pigDataType,
      List<Type> hcatRequiredType, HCatFieldSchema hcatActualField, 
      int columnPos) throws FrontendException {
    if(!hcatRequiredType.contains(hcatActualField.getType())) {
      throw new FrontendException( 
              "Pig '" + DataType.findTypeName(pigDataType) + "' type in column " + 
              columnPos + "(0-based) cannot map to HCat '" + 
              hcatActualField.getType() + "'type.  Target filed must be of HCat type {" +
              StringUtils.join(hcatRequiredType, " or ") + "}");
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

  /**
   * todo: when job is complete, should print the msgCount table to log 
   */
  private static final class DataLossLogger {
    private static final Map<String, Integer> msgCount = new HashMap<String, Integer>();
    private static String getColumnTypeKey(HCatFieldSchema fieldSchema) {
      return fieldSchema.getName() + "_" + (fieldSchema.getTypeInfo() == null ?
        fieldSchema.getType() : fieldSchema.getTypeInfo());
    }
    private void logDataLossMsg(HCatFieldSchema fieldSchema, Object pigOjb, String msg) {
      String key = getColumnTypeKey(fieldSchema);
      if(!msgCount.containsKey(key)) {
        msgCount.put(key, 0);
        LOG.warn(msg + " " + "Will write NULL instead.  Only 1 such message per type/column is emitted.");
      }
      msgCount.put(key, msgCount.get(key) + 1);
    }
  }
}
