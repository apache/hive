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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.IOPrepareCache;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.Explain.Vectorization;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hive.common.util.DateUtils;

import com.google.common.base.Preconditions;

/**
 * Context for Vectorized row batch. this class does eager deserialization of row data using serde
 * in the RecordReader layer.
 * It has supports partitions in this layer so that the vectorized batch is populated correctly
 * with the partition column.
 */
public class VectorizedRowBatchCtx {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(VectorizedRowBatchCtx.class.getName());

  // The following information is for creating VectorizedRowBatch and for helping with
  // knowing how the table is partitioned.
  //
  // It will be stored in MapWork and ReduceWork.
  private String[] rowColumnNames;
  private TypeInfo[] rowColumnTypeInfos;
  private int[] dataColumnNums;
  private int dataColumnCount;
  private int partitionColumnCount;

  private String[] scratchColumnTypeNames;

  /**
   * Constructor for VectorizedRowBatchCtx
   */
  public VectorizedRowBatchCtx() {
  }

  public VectorizedRowBatchCtx(String[] rowColumnNames, TypeInfo[] rowColumnTypeInfos,
      int[] dataColumnNums, int partitionColumnCount, String[] scratchColumnTypeNames) {
    this.rowColumnNames = rowColumnNames;
    this.rowColumnTypeInfos = rowColumnTypeInfos;
    this.dataColumnNums = dataColumnNums;
    this.partitionColumnCount = partitionColumnCount;
    this.scratchColumnTypeNames = scratchColumnTypeNames;

    dataColumnCount = rowColumnTypeInfos.length - partitionColumnCount;
  }

  public String[] getRowColumnNames() {
    return rowColumnNames;
  }

  public TypeInfo[] getRowColumnTypeInfos() {
    return rowColumnTypeInfos;
  }

  public int[] getDataColumnNums() {
    return dataColumnNums;
  }

  public int getDataColumnCount() {
    return dataColumnCount;
  }

  public int getPartitionColumnCount() {
    return partitionColumnCount;
  }

  public String[] getScratchColumnTypeNames() {
    return scratchColumnTypeNames;
  }

  /**
   * Initializes the VectorizedRowBatch context based on an scratch column type names and
   * object inspector.
   * @param structObjectInspector
   * @param scratchColumnTypeNames
   *          Object inspector that shapes the column types
   * @throws HiveException
   */
  public void init(StructObjectInspector structObjectInspector, String[] scratchColumnTypeNames)
          throws HiveException {

    // Row column information.
    rowColumnNames = VectorizedBatchUtil.columnNamesFromStructObjectInspector(structObjectInspector);
    rowColumnTypeInfos = VectorizedBatchUtil.typeInfosFromStructObjectInspector(structObjectInspector);
    dataColumnNums = null;
    partitionColumnCount = 0;
    dataColumnCount = rowColumnTypeInfos.length;

    // Scratch column information.
    this.scratchColumnTypeNames = scratchColumnTypeNames;
  }

  public static void getPartitionValues(VectorizedRowBatchCtx vrbCtx, Configuration hiveConf,
      FileSplit split, Object[] partitionValues) throws IOException {

    Map<Path, PartitionDesc> pathToPartitionInfo = Utilities
        .getMapWork(hiveConf).getPathToPartitionInfo();

    PartitionDesc partDesc = HiveFileFormatUtils
        .getPartitionDescFromPathRecursively(pathToPartitionInfo,
            split.getPath(), IOPrepareCache.get().getPartitionDescMap());

    getPartitionValues(vrbCtx, partDesc, partitionValues);

  }

  public static void getPartitionValues(VectorizedRowBatchCtx vrbCtx, PartitionDesc partDesc,
      Object[] partitionValues) {

    LinkedHashMap<String, String> partSpec = partDesc.getPartSpec();

    for (int i = 0; i < vrbCtx.partitionColumnCount; i++) {
      Object objectValue;
      if (partSpec == null) {
        // For partition-less table, initialize partValue to empty string.
        // We can have partition-less table even if we have partition keys
        // when there is only only partition selected and the partition key is not
        // part of the projection/include list.
        objectValue = null;
      } else {
        String key = vrbCtx.rowColumnNames[vrbCtx.dataColumnCount + i];

        // Create a Standard java object Inspector
        TypeInfo partColTypeInfo = vrbCtx.rowColumnTypeInfos[vrbCtx.dataColumnCount + i];
        ObjectInspector objectInspector =
            TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(partColTypeInfo);
        objectValue =
            ObjectInspectorConverters.
                getConverter(PrimitiveObjectInspectorFactory.
                    javaStringObjectInspector, objectInspector).
                        convert(partSpec.get(key));
        if (partColTypeInfo instanceof CharTypeInfo) {
          objectValue = ((HiveChar) objectValue).getStrippedValue();
        }
      }
      partitionValues[i] = objectValue;
    }
  }

  /**
   * Creates a Vectorized row batch and the column vectors.
   *
   * @return VectorizedRowBatch
   * @throws HiveException
   */
  public VectorizedRowBatch createVectorizedRowBatch()
  {
    final int dataAndPartColumnCount = rowColumnTypeInfos.length;
    final int totalColumnCount = dataAndPartColumnCount + scratchColumnTypeNames.length;
    VectorizedRowBatch result = new VectorizedRowBatch(totalColumnCount);

    if (dataColumnNums == null) {
        // All data and partition columns.
      for (int i = 0; i < dataAndPartColumnCount; i++) {
        TypeInfo typeInfo = rowColumnTypeInfos[i];
        result.cols[i] = VectorizedBatchUtil.createColumnVector(typeInfo);
      }
    } else {
      // Create only needed/included columns data columns.
      for (int i = 0; i < dataColumnNums.length; i++) {
        int columnNum = dataColumnNums[i];
        Preconditions.checkState(columnNum < dataAndPartColumnCount);
        TypeInfo typeInfo = rowColumnTypeInfos[columnNum];
        result.cols[columnNum] = VectorizedBatchUtil.createColumnVector(typeInfo);
      }
      // Always create partition columns.
      final int endColumnNum = dataColumnCount + partitionColumnCount;
      for (int partitionColumnNum = dataColumnCount; partitionColumnNum < endColumnNum; partitionColumnNum++) {
        TypeInfo typeInfo = rowColumnTypeInfos[partitionColumnNum];
        result.cols[partitionColumnNum] = VectorizedBatchUtil.createColumnVector(typeInfo);
      }
    }

    for (int i = 0; i < scratchColumnTypeNames.length; i++) {
      String typeName = scratchColumnTypeNames[i];
      result.cols[rowColumnTypeInfos.length + i] =
          VectorizedBatchUtil.createColumnVector(typeName);
    }

    result.setPartitionInfo(dataColumnCount, partitionColumnCount);

    result.reset();
    return result;
  }

  /**
   * Add the partition values to the batch
   *
   * @param batch
   * @param partitionValues
   * @throws HiveException
   */
  public void addPartitionColsToBatch(VectorizedRowBatch batch, Object[] partitionValues)
  {
    if (partitionValues != null) {
      for (int i = 0; i < partitionColumnCount; i++) {
        Object value = partitionValues[i];

        int colIndex = dataColumnCount + i;
        String partitionColumnName = rowColumnNames[colIndex];
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) rowColumnTypeInfos[colIndex];
        switch (primitiveTypeInfo.getPrimitiveCategory()) {
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
          TimestampColumnVector lcv = (TimestampColumnVector) batch.cols[colIndex];
          if (value == null) {
            lcv.noNulls = false;
            lcv.isNull[0] = true;
            lcv.isRepeating = true;
          } else {
            lcv.fill((Timestamp) value);
            lcv.isNull[0] = false;
          }
        }
        break;

        case INTERVAL_YEAR_MONTH: {
          LongColumnVector lcv = (LongColumnVector) batch.cols[colIndex];
          if (value == null) {
            lcv.noNulls = false;
            lcv.isNull[0] = true;
            lcv.isRepeating = true;
          } else {
            lcv.fill(((HiveIntervalYearMonth) value).getTotalMonths());
            lcv.isNull[0] = false;
          }
        }

        case INTERVAL_DAY_TIME: {
          IntervalDayTimeColumnVector icv = (IntervalDayTimeColumnVector) batch.cols[colIndex];
          if (value == null) {
            icv.noNulls = false;
            icv.isNull[0] = true;
            icv.isRepeating = true;
          } else {
            icv.fill(((HiveIntervalDayTime) value));
            icv.isNull[0] = false;
          }
        }

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
          String sVal = value.toString();
          if (sVal == null) {
            bcv.noNulls = false;
            bcv.isNull[0] = true;
            bcv.isRepeating = true;
          } else {
            bcv.setVal(0, sVal.getBytes());
            bcv.isRepeating = true;
          }
        }
        break;

        default:
          throw new RuntimeException("Unable to recognize the partition type " + primitiveTypeInfo.getPrimitiveCategory() +
              " for column " + partitionColumnName);
        }
      }
    }
  }

  /**
   * Determine whether a given column is a partition column
   * @param colNum column number in
   * {@link org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch}s created by this context.
   * @return true if it is a partition column, false otherwise
   */
  public final boolean isPartitionCol(int colNum) {
    return colNum >= dataColumnCount && colNum < rowColumnTypeInfos.length;
  }

}
