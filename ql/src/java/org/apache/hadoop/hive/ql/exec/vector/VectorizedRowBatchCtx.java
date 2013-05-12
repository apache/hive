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
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.IOPrepareCache;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
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
 * It has supports partitions in this layer so that the vectorized batch is populated correctly with
 * the partition column.
 * VectorizedRowBatchCtx.
 *
 */
public class VectorizedRowBatchCtx {

  // OI for raw row data (EG without partition cols)
  private StructObjectInspector rawRowOI;

  // OI for the row (Raw row OI + partition OI)
  private StructObjectInspector rowOI;

  // Deserializer for the row data
  private Deserializer deserializer;

  // Hash map of partition values. Key=TblColName value=PartitionValue
  private LinkedHashMap<String, String> partitionValues;

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
      Deserializer deserializer, LinkedHashMap<String, String> partitionValues) {
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
  public void Init(Configuration hiveConf, FileSplit split) throws ClassNotFoundException,
      IOException,
      SerDeException,
      InstantiationException,
      IllegalAccessException, HiveException {

    Map<String, PartitionDesc> pathToPartitionInfo = Utilities
        .getMapRedWork(hiveConf).getPathToPartitionInfo();

    PartitionDesc part = HiveFileFormatUtils
        .getPartitionDescFromPathRecursively(pathToPartitionInfo,
            split.getPath(), IOPrepareCache.get().getPartitionDescMap());
    Class serdeclass = part.getDeserializerClass();

    if (serdeclass == null) {
      String className = part.getSerdeClassName();
      if ((className == null) || (className.isEmpty())) {
        throw new HiveException(
            "SerDe class or the SerDe class name is not set for table: "
                + part.getProperties().getProperty("name"));
      }
      serdeclass = hiveConf.getClassByName(className);
    }

    Properties partProps =
        (part.getPartSpec() == null || part.getPartSpec().isEmpty()) ?
            part.getTableDesc().getProperties() : part.getProperties();

    Deserializer partDeserializer = (Deserializer) serdeclass.newInstance();
    partDeserializer.initialize(hiveConf, partProps);
    StructObjectInspector partRawRowObjectInspector = (StructObjectInspector) partDeserializer
        .getObjectInspector();

    deserializer = partDeserializer;

    // Check to see if this split is part of a partition of a table
    String pcols = partProps
        .getProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS);

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
        partitionValues.put(key, partSpec.get(key));
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
  }

  /**
   * Creates a Vectorized row batch and the column vectors.
   *
   * @return VectorizedRowBatch
   * @throws HiveException
   */
  public VectorizedRowBatch CreateVectorizedRowBatch() throws HiveException
  {
    List<? extends StructField> fieldRefs = rowOI.getAllStructFieldRefs();
    VectorizedRowBatch result = new VectorizedRowBatch(fieldRefs.size());
    for (int j = 0; j < fieldRefs.size(); j++) {
      ObjectInspector foi = fieldRefs.get(j).getFieldObjectInspector();
      switch (foi.getCategory()) {
      case PRIMITIVE: {
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector) foi;
        // Vectorization currently only supports the following data types:
        // SHORT, INT, LONG, FLOAT, DOUBLE, STRING
        switch (poi.getPrimitiveCategory()) {
        case SHORT:
        case INT:
        case LONG:
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
    result.numCols = fieldRefs.size();
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
  public void AddRowToBatch(int rowIndex, Writable rowBlob, VectorizedRowBatch batch)
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
  public void ConvertRowBatchBlobToVectorizedBatch(Object rowBlob, int rowsInBlob,
      VectorizedRowBatch batch)
      throws SerDeException {

    if (deserializer instanceof VectorizedSerde) {
      ((VectorizedSerde) deserializer).deserializeVector(rowBlob, rowsInBlob, batch);
    } else {
      throw new SerDeException(
          "Not able to deserialize row batch. Serde does not implement VectorizedSerde");
    }
  }

  private int GetColIndexBasedOnColName(String colName) throws HiveException
  {
    List<? extends StructField> fieldRefs = rowOI.getAllStructFieldRefs();
    for (int i = 0; i < fieldRefs.size(); i++) {
      if (fieldRefs.get(i).getFieldName() == colName) {
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
  public void AddPartitionColsToBatch(VectorizedRowBatch batch) throws HiveException
  {
    int colIndex;
    String value;
    BytesColumnVector bcv;
    if (partitionValues != null) {
      for (String key : partitionValues.keySet()) {
        colIndex = GetColIndexBasedOnColName(key);
        value = partitionValues.get(key);
        bcv = (BytesColumnVector) batch.cols[colIndex];
        bcv.setRef(0, value.getBytes(), 0, value.length());
        bcv.isRepeating = true;
        bcv.isNull[0] = false;
        bcv.noNulls = true;
      }
    }
  }
}
