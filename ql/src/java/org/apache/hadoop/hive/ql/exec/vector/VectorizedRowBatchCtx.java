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
package org.apache.hadoop.hive.ql.exec.vector;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.IOPrepareCache;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;

/**
 * Context for Vectorized row batch. this calss does eager deserialization of row data using serde
 * in the RecordReader layer.
 * It has supports partitions in this layer so that the vectorized batch is populated correctly
 * with the partition column.
 */
public class VectorizedRowBatchCtx {

  private static final Log LOG = LogFactory.getLog(VectorizedRowBatchCtx.class.getName());

  // OI for raw row data (EG without partition cols)
  private StructObjectInspector rawRowOI;

  // OI for the row (Raw row OI + partition OI)
  private StructObjectInspector rowOI;

  // Deserializer for the row data
  private Deserializer deserializer;

  // Hash map of partition values. Key=TblColName value=PartitionValue
  private Map<String, Object> partitionValues;
  
  //partition types
  private Map<String, PrimitiveCategory> partitionTypes;

  // partition column positions, for use by classes that need to know whether a given column is a
  // partition column
  private Set<Integer> partitionCols;
  
  // Column projection list - List of column indexes to include. This
  // list does not contain partition columns
  private List<Integer> colsToInclude;

  private Map<Integer, String> scratchColumnTypeMap = null;

  /**
   * Constructor for VectorizedRowBatchCtx
   *
   * @param rawRowOI
   *          OI for raw row data (EG without partition cols)
   * @param rowOI
   *          OI for the row (Raw row OI + partition OI)
   * @param deserializer
   *          Deserializer for the row data
   * @param partitionValues
   *          Hash map of partition values. Key=TblColName value=PartitionValue
   */
  public VectorizedRowBatchCtx(StructObjectInspector rawRowOI, StructObjectInspector rowOI,
      Deserializer deserializer, Map<String, Object> partitionValues, 
      Map<String, PrimitiveCategory> partitionTypes) {
    this.rowOI = rowOI;
    this.rawRowOI = rawRowOI;
    this.deserializer = deserializer;
    this.partitionValues = partitionValues;
    this.partitionTypes = partitionTypes;
  }

  /**
   * Constructor for VectorizedRowBatchCtx
   */
  public VectorizedRowBatchCtx() {

  }

  /**
   * Initializes the VectorizedRowBatch context based on an scratch column type map and
   * object inspector.
   * @param scratchColumnTypeMap
   * @param rowOI
   *          Object inspector that shapes the column types
   */
  public void init(Map<Integer, String> scratchColumnTypeMap,
      StructObjectInspector rowOI) {
    this.scratchColumnTypeMap = scratchColumnTypeMap;
    this.rowOI= rowOI;
    this.rawRowOI = rowOI;
  }

  /**
   * Initializes VectorizedRowBatch context based on the
   * split and Hive configuration (Job conf with hive Plan).
   *
   * @param hiveConf
   *          Hive configuration using Hive plan is extracted
   * @param split
   *          File split of the file being read
   * @throws ClassNotFoundException
   * @throws IOException
   * @throws SerDeException
   * @throws InstantiationException
   * @throws IllegalAccessException
   * @throws HiveException
   */
  public void init(Configuration hiveConf, FileSplit split) throws ClassNotFoundException,
      IOException,
      SerDeException,
      InstantiationException,
      IllegalAccessException,
      HiveException {

    Map<String, PartitionDesc> pathToPartitionInfo = Utilities
        .getMapRedWork(hiveConf).getMapWork().getPathToPartitionInfo();

    PartitionDesc part = HiveFileFormatUtils
        .getPartitionDescFromPathRecursively(pathToPartitionInfo,
            split.getPath(), IOPrepareCache.get().getPartitionDescMap());

    String partitionPath = split.getPath().getParent().toString();
    scratchColumnTypeMap = Utilities
        .getMapWorkAllScratchColumnVectorTypeMaps(hiveConf)
        .get(partitionPath);

    Properties partProps =
        (part.getPartSpec() == null || part.getPartSpec().isEmpty()) ?
            part.getTableDesc().getProperties() : part.getProperties();

    Class serdeclass = hiveConf.getClassByName(part.getSerdeClassName());
    Deserializer partDeserializer = (Deserializer) serdeclass.newInstance(); 
    SerDeUtils.initializeSerDe(partDeserializer, hiveConf, part.getTableDesc().getProperties(),
                               partProps);
    StructObjectInspector partRawRowObjectInspector = (StructObjectInspector) partDeserializer
        .getObjectInspector();

    deserializer = partDeserializer;

    // Check to see if this split is part of a partition of a table
    String pcols = partProps.getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS);

    String[] partKeys = null;
    if (pcols != null && pcols.length() > 0) {

      // Partitions exist for this table. Get the partition object inspector and
      // raw row object inspector (row with out partition col)
      LinkedHashMap<String, String> partSpec = part.getPartSpec();
      partKeys = pcols.trim().split("/");
      String pcolTypes = partProps.getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES);      
      String[] partKeyTypes = pcolTypes.trim().split(":");      
      
      if (partKeys.length  > partKeyTypes.length) {
        throw new HiveException("Internal error : partKeys length, " +partKeys.length +
                " greater than partKeyTypes length, " + partKeyTypes.length);
      }
      
      List<String> partNames = new ArrayList<String>(partKeys.length);
      List<ObjectInspector> partObjectInspectors = new ArrayList<ObjectInspector>(partKeys.length);
      partitionValues = new LinkedHashMap<String, Object>();
      partitionTypes = new LinkedHashMap<String, PrimitiveCategory>();
      for (int i = 0; i < partKeys.length; i++) {
        String key = partKeys[i];
        partNames.add(key);
        ObjectInspector objectInspector = null;
        Object objectVal; 
        if (partSpec == null) {
          // for partitionless table, initialize partValue to empty string.
          // We can have partitionless table even if we have partition keys
          // when there is only only partition selected and the partition key is not
          // part of the projection/include list.
          objectVal = null;
          objectInspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
          partitionTypes.put(key, PrimitiveCategory.STRING);       
        } else {
          // Create a Standard java object Inspector
          objectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
              TypeInfoFactory.getPrimitiveTypeInfo(partKeyTypes[i]));
          objectVal = 
              ObjectInspectorConverters.
              getConverter(PrimitiveObjectInspectorFactory.
                  javaStringObjectInspector, objectInspector).
                  convert(partSpec.get(key));              
          partitionTypes.put(key, TypeInfoFactory.getPrimitiveTypeInfo(partKeyTypes[i]).getPrimitiveCategory());
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Partition column: name: " + key + ", value: " + objectVal + ", type: " + partitionTypes.get(key));
        }
        partitionValues.put(key, objectVal);
        partObjectInspectors.add(objectInspector);
      }

      // Create partition OI
      StructObjectInspector partObjectInspector = ObjectInspectorFactory
          .getStandardStructObjectInspector(partNames, partObjectInspectors);

      // Get row OI from partition OI and raw row OI
      StructObjectInspector rowObjectInspector = ObjectInspectorFactory
          .getUnionStructObjectInspector(Arrays
              .asList(new StructObjectInspector[] {partRawRowObjectInspector, partObjectInspector}));
      rowOI = rowObjectInspector;
      rawRowOI = partRawRowObjectInspector;

      // We have to do this after we've set rowOI, as getColIndexBasedOnColName uses it
      partitionCols = new HashSet<Integer>();
      if (pcols != null && pcols.length() > 0) {
        for (int i = 0; i < partKeys.length; i++) {
          partitionCols.add(getColIndexBasedOnColName(partKeys[i]));
        }
      }

    } else {

      // No partitions for this table, hence row OI equals raw row OI
      rowOI = partRawRowObjectInspector;
      rawRowOI = partRawRowObjectInspector;
    }

    colsToInclude = ColumnProjectionUtils.getReadColumnIDs(hiveConf);
  }
  
  /**
   * Creates a Vectorized row batch and the column vectors.
   *
   * @return VectorizedRowBatch
   * @throws HiveException
   */
  public VectorizedRowBatch createVectorizedRowBatch() throws HiveException
  {
    List<? extends StructField> fieldRefs = rowOI.getAllStructFieldRefs();
    VectorizedRowBatch result = new VectorizedRowBatch(fieldRefs.size());
    for (int j = 0; j < fieldRefs.size(); j++) {
      // If the column is included in the include list or if the column is a
      // partition column then create the column vector. Also note that partition columns are not
      // in the included list.
      if ((colsToInclude == null) || colsToInclude.contains(j)
          || ((partitionValues != null) &&
              partitionValues.containsKey(fieldRefs.get(j).getFieldName()))) {
        ObjectInspector foi = fieldRefs.get(j).getFieldObjectInspector();
        switch (foi.getCategory()) {
        case PRIMITIVE: {
          PrimitiveObjectInspector poi = (PrimitiveObjectInspector) foi;
          // Vectorization currently only supports the following data types:
          // BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, BINARY, STRING, CHAR, VARCHAR, TIMESTAMP,
          // DATE and DECIMAL
          switch (poi.getPrimitiveCategory()) {
          case BOOLEAN:
          case BYTE:
          case SHORT:
          case INT:
          case LONG:
          case TIMESTAMP:
          case DATE:
            result.cols[j] = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
            break;
          case FLOAT:
          case DOUBLE:
            result.cols[j] = new DoubleColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
            break;
          case BINARY:
          case STRING:
          case CHAR:
          case VARCHAR:
            result.cols[j] = new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
            break;
          case DECIMAL:
            DecimalTypeInfo tInfo = (DecimalTypeInfo) poi.getTypeInfo();
            result.cols[j] = new DecimalColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
                tInfo.precision(), tInfo.scale());
            break;
          default:
            throw new RuntimeException("Vectorizaton is not supported for datatype:"
                + poi.getPrimitiveCategory());
          }
          break;
        }
        case LIST:
        case MAP:
        case STRUCT:
        case UNION:
          throw new HiveException("Vectorizaton is not supported for datatype:"
              + foi.getCategory());
        default:
          throw new HiveException("Unknown ObjectInspector category!");
        }    
      }
    }
    result.numCols = fieldRefs.size();
    this.addScratchColumnsToBatch(result);
    result.reset();
    return result;
  }

  /**
   * Adds the row to the batch after deserializing the row
   *
   * @param rowIndex
   *          Row index in the batch to which the row is added
   * @param rowBlob
   *          Row blob (serialized version of row)
   * @param batch
   *          Vectorized batch to which the row is added
   * @param buffer a buffer to copy strings into
   * @throws HiveException
   * @throws SerDeException
   */
  public void addRowToBatch(int rowIndex, Writable rowBlob,
                            VectorizedRowBatch batch,
                            DataOutputBuffer buffer
                            ) throws HiveException, SerDeException
  {
    Object row = this.deserializer.deserialize(rowBlob);
    VectorizedBatchUtil.addRowToBatch(row, this.rawRowOI, rowIndex, batch, buffer);
  }

  /**
   * Deserialized set of rows and populates the batch
   *
   * @param rowBlob
   *          to deserialize
   * @param batch
   *          Vectorized row batch which contains deserialized data
   * @throws SerDeException
   */
  public void convertRowBatchBlobToVectorizedBatch(Object rowBlob, int rowsInBlob,
      VectorizedRowBatch batch)
      throws SerDeException {

    if (deserializer instanceof VectorizedSerde) {
      ((VectorizedSerde) deserializer).deserializeVector(rowBlob, rowsInBlob, batch);
    } else {
      throw new SerDeException(
          "Not able to deserialize row batch. Serde does not implement VectorizedSerde");
    }
  }

  private int getColIndexBasedOnColName(String colName) throws HiveException
  {
    List<? extends StructField> fieldRefs = rowOI.getAllStructFieldRefs();
    for (int i = 0; i < fieldRefs.size(); i++) {
      if (fieldRefs.get(i).getFieldName().equals(colName)) {
        return i;
      }
    }
    throw new HiveException("Not able to find column name in row object inspector");
  }
  
  /**
   * Add the partition values to the batch
   *
   * @param batch
   * @throws HiveException
   */
  public void addPartitionColsToBatch(VectorizedRowBatch batch) throws HiveException
  {
    int colIndex;
    Object value;
    PrimitiveCategory pCategory;
    if (partitionValues != null) {
      for (String key : partitionValues.keySet()) {
        colIndex = getColIndexBasedOnColName(key);
        value = partitionValues.get(key);
        pCategory = partitionTypes.get(key);
        
        switch (pCategory) {
        case BOOLEAN: {
          LongColumnVector lcv = (LongColumnVector) batch.cols[colIndex];
          if (value == null) {
            lcv.noNulls = false;
            lcv.isNull[0] = true;
            lcv.isRepeating = true;
          } else { 
            lcv.fill((Boolean) value == true ? 1 : 0);
            lcv.isNull[0] = false;
          }
        }
        break;          
        
        case BYTE: {
          LongColumnVector lcv = (LongColumnVector) batch.cols[colIndex];
          if (value == null) {
            lcv.noNulls = false;
            lcv.isNull[0] = true;
            lcv.isRepeating = true;
          } else { 
            lcv.fill((Byte) value);
            lcv.isNull[0] = false;
          }
        }
        break;             
        
        case SHORT: {
          LongColumnVector lcv = (LongColumnVector) batch.cols[colIndex];
          if (value == null) {
            lcv.noNulls = false;
            lcv.isNull[0] = true;
            lcv.isRepeating = true;
          } else { 
            lcv.fill((Short) value);
            lcv.isNull[0] = false;
          }
        }
        break;
        
        case INT: {
          LongColumnVector lcv = (LongColumnVector) batch.cols[colIndex];
          if (value == null) {
            lcv.noNulls = false;
            lcv.isNull[0] = true;
            lcv.isRepeating = true;
          } else { 
            lcv.fill((Integer) value);
            lcv.isNull[0] = false;
          }          
        }
        break;
        
        case LONG: {
          LongColumnVector lcv = (LongColumnVector) batch.cols[colIndex];
          if (value == null) {
            lcv.noNulls = false;
            lcv.isNull[0] = true;
            lcv.isRepeating = true;
          } else { 
            lcv.fill((Long) value);
            lcv.isNull[0] = false;
          }          
        }
        break;
        
        case DATE: {
          LongColumnVector lcv = (LongColumnVector) batch.cols[colIndex];
          if (value == null) {
            lcv.noNulls = false;
            lcv.isNull[0] = true;
            lcv.isRepeating = true;
          } else { 
            lcv.fill(DateWritable.dateToDays((Date) value));
            lcv.isNull[0] = false;
          }          
        }
        break;
        
        case TIMESTAMP: {
          LongColumnVector lcv = (LongColumnVector) batch.cols[colIndex];
          if (value == null) {
            lcv.noNulls = false;
            lcv.isNull[0] = true;
            lcv.isRepeating = true;
          } else { 
            lcv.fill(TimestampUtils.getTimeNanoSec((Timestamp) value));
            lcv.isNull[0] = false;
          }
        }
        break;
        
        case FLOAT: {
          DoubleColumnVector dcv = (DoubleColumnVector) batch.cols[colIndex];
          if (value == null) {
            dcv.noNulls = false;
            dcv.isNull[0] = true;
            dcv.isRepeating = true;
          } else {
            dcv.fill((Float) value);
            dcv.isNull[0] = false;
          }          
        }
        break;
        
        case DOUBLE: {
          DoubleColumnVector dcv = (DoubleColumnVector) batch.cols[colIndex];
          if (value == null) {
            dcv.noNulls = false;
            dcv.isNull[0] = true;
            dcv.isRepeating = true;
          } else {
            dcv.fill((Double) value);
            dcv.isNull[0] = false;
          }
        }
        break;
        
        case DECIMAL: {
          DecimalColumnVector dv = (DecimalColumnVector) batch.cols[colIndex];
          if (value == null) {
            dv.noNulls = false;
            dv.isNull[0] = true;
            dv.isRepeating = true;
          } else {
            HiveDecimal hd = (HiveDecimal) value;
            dv.set(0, hd);
            dv.isRepeating = true;
            dv.isNull[0] = false;      
          }
        }
        break;

        case BINARY: {
            BytesColumnVector bcv = (BytesColumnVector) batch.cols[colIndex];
            byte[] bytes = (byte[]) value;
            if (bytes == null) {
              bcv.noNulls = false;
              bcv.isNull[0] = true;
              bcv.isRepeating = true;
            } else {
              bcv.fill(bytes);
              bcv.isNull[0] = false;
            }
          }
          break;

        case STRING:
        case CHAR:
        case VARCHAR: {
          BytesColumnVector bcv = (BytesColumnVector) batch.cols[colIndex];
          String sVal = (String) value;
          if (sVal == null) {
            bcv.noNulls = false;
            bcv.isNull[0] = true;
            bcv.isRepeating = true;
          } else {
            bcv.fill(sVal.getBytes()); 
            bcv.isNull[0] = false;
          }
        }
        break;
        
        default:
          throw new HiveException("Unable to recognize the partition type " + pCategory + 
              " for column " + key);
        }
      }
    }
  }

  /**
   * Determine whether a given column is a partition column
   * @param colnum column number in
   * {@link org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch}s created by this context.
   * @return true if it is a partition column, false otherwise
   */
  public final boolean isPartitionCol(int colnum) {
    return (partitionCols == null) ? false : partitionCols.contains(colnum);
  }

  private void addScratchColumnsToBatch(VectorizedRowBatch vrb) throws HiveException {
    if (scratchColumnTypeMap != null && !scratchColumnTypeMap.isEmpty()) {
      int origNumCols = vrb.numCols;
      int newNumCols = vrb.cols.length+scratchColumnTypeMap.keySet().size();
      vrb.cols = Arrays.copyOf(vrb.cols, newNumCols);
      for (int i = origNumCols; i < newNumCols; i++) {
       String typeName = scratchColumnTypeMap.get(i);
       if (typeName == null) {
         throw new HiveException("No type found for column type entry " + i);
       }
        vrb.cols[i] = allocateColumnVector(typeName,
            VectorizedRowBatch.DEFAULT_SIZE);
      }
      vrb.numCols = vrb.cols.length;
    }
  }

  /**
   * Get the scale and precision for the given decimal type string. The decimal type is assumed to be
   * of the format decimal(precision,scale) e.g. decimal(20,10).
   * @param decimalType The given decimal type string.
   * @return An integer array of size 2 with first element set to precision and second set to scale.
   */
  private int[] getScalePrecisionFromDecimalType(String decimalType) {
    Pattern p = Pattern.compile("\\d+");
    Matcher m = p.matcher(decimalType);
    m.find();
    int precision = Integer.parseInt(m.group());
    m.find();
    int scale = Integer.parseInt(m.group());
    int [] precScale = { precision, scale };
    return precScale;
  }

  private ColumnVector allocateColumnVector(String type, int defaultSize) {
    if (type.equalsIgnoreCase("double")) {
      return new DoubleColumnVector(defaultSize);
    } else if (VectorizationContext.isStringFamily(type)) {
      return new BytesColumnVector(defaultSize);
    } else if (VectorizationContext.decimalTypePattern.matcher(type).matches()){
      int [] precisionScale = getScalePrecisionFromDecimalType(type);
      return new DecimalColumnVector(defaultSize, precisionScale[0], precisionScale[1]);
    } else if (type.equalsIgnoreCase("long") ||
               type.equalsIgnoreCase("date") ||
               type.equalsIgnoreCase("timestamp")) {
      return new LongColumnVector(defaultSize);
    } else {
      throw new Error("Cannot allocate vector column for " + type);
    }
  }

  public VectorColumnAssign[] buildObjectAssigners(VectorizedRowBatch outputBatch)
        throws HiveException {
    List<? extends StructField> fieldRefs = rowOI.getAllStructFieldRefs();
    assert outputBatch.numCols == fieldRefs.size();
    VectorColumnAssign[] assigners = new VectorColumnAssign[fieldRefs.size()];
    for(int i = 0; i < assigners.length; ++i) {
        StructField fieldRef = fieldRefs.get(i);
        ObjectInspector fieldOI = fieldRef.getFieldObjectInspector();
        assigners[i] = VectorColumnAssignFactory.buildObjectAssign(
                outputBatch, i, fieldOI);
    }
    return assigners;
  }
}
