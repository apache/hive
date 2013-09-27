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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.IOPrepareCache;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;

/**
 * Context for Vectorized row batch. this calss does eager deserialization of row data using serde
 * in the RecordReader layer.
 * It has supports partitions in this layer so that the vectorized batch is populated correctly
 * with the partition column.
 */
public class VectorizedRowBatchCtx {

  // OI for raw row data (EG without partition cols)
  private StructObjectInspector rawRowOI;

  // OI for the row (Raw row OI + partition OI)
  private StructObjectInspector rowOI;

  // Deserializer for the row data
  private Deserializer deserializer;

  // Hash map of partition values. Key=TblColName value=PartitionValue
  private Map<String, String> partitionValues;

  // Column projection list - List of column indexes to include. This
  // list does not contain partition columns
  private List<Integer> colsToInclude;

  private Map<Integer, String> columnTypeMap = null;

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
      Deserializer deserializer, Map<String, String> partitionValues) {
    this.rowOI = rowOI;
    this.rawRowOI = rawRowOI;
    this.deserializer = deserializer;
    this.partitionValues = partitionValues;
  }

  /**
   * Constructor for VectorizedRowBatchCtx
   */
  public VectorizedRowBatchCtx() {

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
      IllegalAccessException, HiveException {

    Map<String, PartitionDesc> pathToPartitionInfo = Utilities
        .getMapRedWork(hiveConf).getMapWork().getPathToPartitionInfo();

    PartitionDesc part = HiveFileFormatUtils
        .getPartitionDescFromPathRecursively(pathToPartitionInfo,
            split.getPath(), IOPrepareCache.get().getPartitionDescMap());

    String partitionPath = split.getPath().getParent().toString();
    columnTypeMap = Utilities
        .getMapRedWork(hiveConf).getMapWork().getScratchColumnVectorTypes()
        .get(partitionPath);

    Properties partProps =
        (part.getPartSpec() == null || part.getPartSpec().isEmpty()) ?
            part.getTableDesc().getProperties() : part.getProperties();

    Class serdeclass = hiveConf.getClassByName(part.getSerdeClassName());
    Deserializer partDeserializer = (Deserializer) serdeclass.newInstance(); 
    partDeserializer.initialize(hiveConf, partProps);
    StructObjectInspector partRawRowObjectInspector = (StructObjectInspector) partDeserializer
        .getObjectInspector();

    deserializer = partDeserializer;

    // Check to see if this split is part of a partition of a table
    String pcols = partProps.getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS);

    if (pcols != null && pcols.length() > 0) {

      // Partitions exist for this table. Get the partition object inspector and
      // raw row object inspector (row with out partition col)
      LinkedHashMap<String, String> partSpec = part.getPartSpec();
      String[] partKeys = pcols.trim().split("/");
      List<String> partNames = new ArrayList<String>(partKeys.length);
      partitionValues = new LinkedHashMap<String, String>();
      List<ObjectInspector> partObjectInspectors = new ArrayList<ObjectInspector>(
          partKeys.length);
      for (int i = 0; i < partKeys.length; i++) {
        String key = partKeys[i];
        partNames.add(key);
        if (partSpec == null) {
          // for partitionless table, initialize partValue to empty string.
          // We can have partitionless table even if we have partition keys
          // when there is only only partition selected and the partition key is not
          // part of the projection/include list.
          partitionValues.put(key, "");
        } else {
          partitionValues.put(key, partSpec.get(key));
        }

        partObjectInspectors
            .add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
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
              (partitionValues.get(fieldRefs.get(j).getFieldName()) != null))) {
        ObjectInspector foi = fieldRefs.get(j).getFieldObjectInspector();
        switch (foi.getCategory()) {
        case PRIMITIVE: {
          PrimitiveObjectInspector poi = (PrimitiveObjectInspector) foi;
          // Vectorization currently only supports the following data types:
          // BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, STRING and TIMESTAMP
          switch (poi.getPrimitiveCategory()) {
          case BOOLEAN:
          case BYTE:
          case SHORT:
          case INT:
          case LONG:
          case TIMESTAMP:
            result.cols[j] = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
            break;
          case FLOAT:
          case DOUBLE:
            result.cols[j] = new DoubleColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
            break;
          case STRING:
            result.cols[j] = new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
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
   * @throws HiveException
   * @throws SerDeException
   */
  public void addRowToBatch(int rowIndex, Writable rowBlob, VectorizedRowBatch batch)
      throws HiveException, SerDeException
  {
    Object row = this.deserializer.deserialize(rowBlob);
    VectorizedBatchUtil.AddRowToBatch(row, this.rawRowOI, rowIndex, batch);
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
    String value;
    BytesColumnVector bcv;
    if (partitionValues != null) {
      for (String key : partitionValues.keySet()) {
        colIndex = getColIndexBasedOnColName(key);
        value = partitionValues.get(key);
        bcv = (BytesColumnVector) batch.cols[colIndex];
        bcv.setRef(0, value.getBytes(), 0, value.length());
        bcv.isRepeating = true;
        bcv.isNull[0] = false;
        bcv.noNulls = true;
      }
    }
  }

  private void addScratchColumnsToBatch(VectorizedRowBatch vrb) {
    if (columnTypeMap != null && !columnTypeMap.isEmpty()) {
      int origNumCols = vrb.numCols;
      int newNumCols = vrb.cols.length+columnTypeMap.keySet().size();
      vrb.cols = Arrays.copyOf(vrb.cols, newNumCols);
      for (int i = origNumCols; i < newNumCols; i++) {
        vrb.cols[i] = allocateColumnVector(columnTypeMap.get(i),
            VectorizedRowBatch.DEFAULT_SIZE);
      }
      vrb.numCols = vrb.cols.length;
    }
  }

  private ColumnVector allocateColumnVector(String type, int defaultSize) {
    if (type.equalsIgnoreCase("double")) {
      return new DoubleColumnVector(defaultSize);
    } else if (type.equalsIgnoreCase("string")) {
      return new BytesColumnVector(defaultSize);
    } else {
      return new LongColumnVector(defaultSize);
    }
  }
}
