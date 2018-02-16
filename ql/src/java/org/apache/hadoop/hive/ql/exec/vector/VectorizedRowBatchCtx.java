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
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedSupport.Support;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.IOPrepareCache;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.MapWork;
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
  private DataTypePhysicalVariation[] rowDataTypePhysicalVariations;
  private int[] dataColumnNums;
  private int dataColumnCount;
  private int partitionColumnCount;
  private int virtualColumnCount;
  private VirtualColumn[] neededVirtualColumns;
  /**
   * A record ID column is a virtual column, so it should be separated from normal data column
   * processes. A recordIdColumnVector contains RecordIdentifier information in a
   * StructColumnVector. It has three LongColumnVectors as its fields; original transaction IDs,
   * bucket IDs, and row IDs.
   */
  private StructColumnVector recordIdColumnVector;

  private String[] scratchColumnTypeNames;
  private DataTypePhysicalVariation[] scratchDataTypePhysicalVariations;


  /**
   * Constructor for VectorizedRowBatchCtx
   */
  public VectorizedRowBatchCtx() {
  }

  public VectorizedRowBatchCtx(
      String[] rowColumnNames,
      TypeInfo[] rowColumnTypeInfos,
      DataTypePhysicalVariation[] rowDataTypePhysicalVariations,
      int[] dataColumnNums,
      int partitionColumnCount,
      int virtualColumnCount,
      VirtualColumn[] neededVirtualColumns,
      String[] scratchColumnTypeNames,
      DataTypePhysicalVariation[] scratchDataTypePhysicalVariations) {
    this.rowColumnNames = rowColumnNames;
    this.rowColumnTypeInfos = rowColumnTypeInfos;
    if (rowDataTypePhysicalVariations == null) {
      this.rowDataTypePhysicalVariations = new DataTypePhysicalVariation[rowColumnTypeInfos.length];
      Arrays.fill(this.rowDataTypePhysicalVariations, DataTypePhysicalVariation.NONE);
    } else {
      this.rowDataTypePhysicalVariations = rowDataTypePhysicalVariations;
    }
    this.dataColumnNums = dataColumnNums;
    this.partitionColumnCount = partitionColumnCount;

    /*
     * Needed virtual columns are those used in the query.
     */
    if (neededVirtualColumns == null) {
      neededVirtualColumns = new VirtualColumn[0];
    } else {
      this.neededVirtualColumns = neededVirtualColumns;
    }

    /*
     * The virtual columns available under vectorization.  They may not actually
     * be used in this query.  Unused columns will be null, just like unused data and partition
     * columns are.
     */
    //
    this.virtualColumnCount = virtualColumnCount;

    this.scratchColumnTypeNames = scratchColumnTypeNames;
    if (scratchDataTypePhysicalVariations == null) {
      this.scratchDataTypePhysicalVariations = new DataTypePhysicalVariation[scratchColumnTypeNames.length];
      Arrays.fill(this.scratchDataTypePhysicalVariations, DataTypePhysicalVariation.NONE);
    } else {
      this.scratchDataTypePhysicalVariations = scratchDataTypePhysicalVariations;
    }

    dataColumnCount = rowColumnTypeInfos.length - partitionColumnCount - virtualColumnCount;
  }

  public String[] getRowColumnNames() {
    return rowColumnNames;
  }

  public TypeInfo[] getRowColumnTypeInfos() {
    return rowColumnTypeInfos;
  }

  public DataTypePhysicalVariation[] getRowdataTypePhysicalVariations() {
    return rowDataTypePhysicalVariations;
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

  public int getVirtualColumnCount() {
    return virtualColumnCount;
  }

  public VirtualColumn[] getNeededVirtualColumns() {
    return neededVirtualColumns;
  }

  public boolean isVirtualColumnNeeded(String virtualColumnName) {
    for (VirtualColumn neededVirtualColumn : neededVirtualColumns) {
      if (neededVirtualColumn.getName().equals(virtualColumnName)) {
        return true;
      }
    }
    return false;
  }

  public int findVirtualColumnNum(VirtualColumn virtualColumn) {
    // Virtual columns start after the last partition column.
    int resultColumnNum = dataColumnCount + partitionColumnCount;
    for (VirtualColumn neededVirtualColumn : neededVirtualColumns) {
      if (neededVirtualColumn.equals(virtualColumn)) {
        return resultColumnNum;
      }
      resultColumnNum++;
    }
    return -1;
  }

  public String[] getScratchColumnTypeNames() {
    return scratchColumnTypeNames;
  }

  public DataTypePhysicalVariation[] getScratchDataTypePhysicalVariations() {
    return scratchDataTypePhysicalVariations;
  }

  public StructColumnVector getRecordIdColumnVector() {
    return this.recordIdColumnVector;
  }

  public void setRecordIdColumnVector(StructColumnVector recordIdColumnVector) {
    this.recordIdColumnVector = recordIdColumnVector;
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
    virtualColumnCount = 0;
    neededVirtualColumns = new VirtualColumn[0];
    dataColumnCount = rowColumnTypeInfos.length;

    // Scratch column information.
    this.scratchColumnTypeNames = scratchColumnTypeNames;
    final int scratchSize = scratchColumnTypeNames.length;
    scratchDataTypePhysicalVariations = new DataTypePhysicalVariation[scratchSize];
    Arrays.fill(scratchDataTypePhysicalVariations, DataTypePhysicalVariation.NONE);
  }

  /**
   * Initializes the VectorizedRowBatch context based on an scratch column type names and
   * object inspector.
   * @param structObjectInspector
   * @param scratchColumnTypeNames
   *          Object inspector that shapes the column types
   * @throws HiveException
   */
  public void init(StructObjectInspector structObjectInspector, String[] scratchColumnTypeNames,
      DataTypePhysicalVariation[] scratchDataTypePhysicalVariations)
          throws HiveException {

    // Row column information.
    rowColumnNames = VectorizedBatchUtil.columnNamesFromStructObjectInspector(structObjectInspector);
    rowColumnTypeInfos = VectorizedBatchUtil.typeInfosFromStructObjectInspector(structObjectInspector);
    dataColumnNums = null;
    partitionColumnCount = 0;
    virtualColumnCount = 0;
    neededVirtualColumns = new VirtualColumn[0];
    dataColumnCount = rowColumnTypeInfos.length;

    // Scratch column information.
    this.scratchColumnTypeNames = scratchColumnTypeNames;
    this.scratchDataTypePhysicalVariations = scratchDataTypePhysicalVariations;
  }

  public static void getPartitionValues(VectorizedRowBatchCtx vrbCtx, Configuration hiveConf,
      FileSplit split, Object[] partitionValues) throws IOException {
    // TODO: this is invalid for SMB. Keep this for now for legacy reasons. See the other overload.
    MapWork mapWork = Utilities.getMapWork(hiveConf);
    getPartitionValues(vrbCtx, mapWork, split, partitionValues);
  }

  public static void getPartitionValues(VectorizedRowBatchCtx vrbCtx,
      MapWork mapWork, FileSplit split, Object[] partitionValues)
      throws IOException {
    Map<Path, PartitionDesc> pathToPartitionInfo = mapWork.getPathToPartitionInfo();

    PartitionDesc partDesc = HiveFileFormatUtils
        .getFromPathRecursively(pathToPartitionInfo,
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

  private ColumnVector createColumnVectorFromRowColumnTypeInfos(int columnNum) {
    TypeInfo typeInfo = rowColumnTypeInfos[columnNum];
    final DataTypePhysicalVariation dataTypePhysicalVariation;
    if (rowDataTypePhysicalVariations != null) {
      dataTypePhysicalVariation = rowDataTypePhysicalVariations[columnNum];
    } else {
      dataTypePhysicalVariation = DataTypePhysicalVariation.NONE;
    }
    return VectorizedBatchUtil.createColumnVector(typeInfo, dataTypePhysicalVariation);
  }

  /**
   * Creates a Vectorized row batch and the column vectors.
   *
   * @return VectorizedRowBatch
   * @throws HiveException
   */
  public VectorizedRowBatch createVectorizedRowBatch()
  {
    final int nonScratchColumnCount = rowColumnTypeInfos.length;
    final int totalColumnCount =
        nonScratchColumnCount + scratchColumnTypeNames.length;
    VectorizedRowBatch result = new VectorizedRowBatch(totalColumnCount);

    if (dataColumnNums == null) {
        // All data and partition columns.
      for (int i = 0; i < nonScratchColumnCount; i++) {
        result.cols[i] = createColumnVectorFromRowColumnTypeInfos(i);
      }
    } else {
      // Create only needed/included columns data columns.
      for (int i = 0; i < dataColumnNums.length; i++) {
        int columnNum = dataColumnNums[i];
        Preconditions.checkState(columnNum < nonScratchColumnCount);
        result.cols[columnNum] =
            createColumnVectorFromRowColumnTypeInfos(columnNum);
      }
      // Always create partition and virtual columns.
      final int partitionEndColumnNum = dataColumnCount + partitionColumnCount;
      for (int partitionColumnNum = dataColumnCount; partitionColumnNum < partitionEndColumnNum; partitionColumnNum++) {
        result.cols[partitionColumnNum] =
            VectorizedBatchUtil.createColumnVector(rowColumnTypeInfos[partitionColumnNum]);
      }
      final int virtualEndColumnNum = partitionEndColumnNum + virtualColumnCount;
      for (int virtualColumnNum = partitionEndColumnNum; virtualColumnNum < virtualEndColumnNum; virtualColumnNum++) {
        String virtualColumnName = rowColumnNames[virtualColumnNum];
        if (!isVirtualColumnNeeded(virtualColumnName)) {
          continue;
        }
        result.cols[virtualColumnNum] =
            VectorizedBatchUtil.createColumnVector(rowColumnTypeInfos[virtualColumnNum]);
      }
    }

    for (int i = 0; i < scratchColumnTypeNames.length; i++) {
      String typeName = scratchColumnTypeNames[i];
      DataTypePhysicalVariation dataTypePhysicalVariation = scratchDataTypePhysicalVariations[i];
      result.cols[nonScratchColumnCount + i] =
          VectorizedBatchUtil.createColumnVector(typeName, dataTypePhysicalVariation);
    }

    // UNDONE: Also remember virtualColumnCount...
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
    addPartitionColsToBatch(batch.cols, partitionValues);
  }

  public void addPartitionColsToBatch(ColumnVector[] cols, Object[] partitionValues)
  {
    if (partitionValues != null) {
      for (int i = 0; i < partitionColumnCount; i++) {
        Object value = partitionValues[i];

        int colIndex = dataColumnCount + i;
        String partitionColumnName = rowColumnNames[colIndex];
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) rowColumnTypeInfos[colIndex];
        switch (primitiveTypeInfo.getPrimitiveCategory()) {
        case BOOLEAN: {
          LongColumnVector lcv = (LongColumnVector) cols[colIndex];
          if (value == null) {
            lcv.noNulls = false;
            lcv.isNull[0] = true;
            lcv.isRepeating = true;
          } else {
            lcv.fill((Boolean) value == true ? 1 : 0);
          }
        }
        break;

        case BYTE: {
          LongColumnVector lcv = (LongColumnVector) cols[colIndex];
          if (value == null) {
            lcv.noNulls = false;
            lcv.isNull[0] = true;
            lcv.isRepeating = true;
          } else {
            lcv.fill((Byte) value);
          }
        }
        break;

        case SHORT: {
          LongColumnVector lcv = (LongColumnVector) cols[colIndex];
          if (value == null) {
            lcv.noNulls = false;
            lcv.isNull[0] = true;
            lcv.isRepeating = true;
          } else {
            lcv.fill((Short) value);
          }
        }
        break;

        case INT: {
          LongColumnVector lcv = (LongColumnVector) cols[colIndex];
          if (value == null) {
            lcv.noNulls = false;
            lcv.isNull[0] = true;
            lcv.isRepeating = true;
          } else {
            lcv.fill((Integer) value);
          }
        }
        break;

        case LONG: {
          LongColumnVector lcv = (LongColumnVector) cols[colIndex];
          if (value == null) {
            lcv.noNulls = false;
            lcv.isNull[0] = true;
            lcv.isRepeating = true;
          } else {
            lcv.fill((Long) value);
          }
        }
        break;

        case DATE: {
          LongColumnVector lcv = (LongColumnVector) cols[colIndex];
          if (value == null) {
            lcv.noNulls = false;
            lcv.isNull[0] = true;
            lcv.isRepeating = true;
          } else {
            lcv.fill(DateWritable.dateToDays((Date) value));
          }
        }
        break;

        case TIMESTAMP: {
          TimestampColumnVector lcv = (TimestampColumnVector) cols[colIndex];
          if (value == null) {
            lcv.noNulls = false;
            lcv.isNull[0] = true;
            lcv.isRepeating = true;
          } else {
            lcv.fill((Timestamp) value);
          }
        }
        break;

        case INTERVAL_YEAR_MONTH: {
          LongColumnVector lcv = (LongColumnVector) cols[colIndex];
          if (value == null) {
            lcv.noNulls = false;
            lcv.isNull[0] = true;
            lcv.isRepeating = true;
          } else {
            lcv.fill(((HiveIntervalYearMonth) value).getTotalMonths());
          }
        }

        case INTERVAL_DAY_TIME: {
          IntervalDayTimeColumnVector icv = (IntervalDayTimeColumnVector) cols[colIndex];
          if (value == null) {
            icv.noNulls = false;
            icv.isNull[0] = true;
            icv.isRepeating = true;
          } else {
            icv.fill(((HiveIntervalDayTime) value));
          }
        }

        case FLOAT: {
          DoubleColumnVector dcv = (DoubleColumnVector) cols[colIndex];
          if (value == null) {
            dcv.noNulls = false;
            dcv.isNull[0] = true;
            dcv.isRepeating = true;
          } else {
            dcv.fill((Float) value);
          }
        }
        break;

        case DOUBLE: {
          DoubleColumnVector dcv = (DoubleColumnVector) cols[colIndex];
          if (value == null) {
            dcv.noNulls = false;
            dcv.isNull[0] = true;
            dcv.isRepeating = true;
          } else {
            dcv.fill((Double) value);
          }
        }
        break;

        case DECIMAL: {
          DecimalColumnVector dv = (DecimalColumnVector) cols[colIndex];
          if (value == null) {
            dv.noNulls = false;
            dv.isNull[0] = true;
            dv.isRepeating = true;
          } else {
            dv.fill((HiveDecimal) value);
          }
        }
        break;

        case BINARY: {
            BytesColumnVector bcv = (BytesColumnVector) cols[colIndex];
            byte[] bytes = (byte[]) value;
            if (bytes == null) {
              bcv.noNulls = false;
              bcv.isNull[0] = true;
              bcv.isRepeating = true;
            } else {
              bcv.fill(bytes);
            }
          }
          break;

        case STRING:
        case CHAR:
        case VARCHAR: {
          BytesColumnVector bcv = (BytesColumnVector) cols[colIndex];
          String sVal = value.toString();
          if (sVal == null) {
            bcv.noNulls = false;
            bcv.isNull[0] = true;
            bcv.isRepeating = true;
          } else {
            bcv.fill(sVal.getBytes());
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
