/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.vector;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.ParquetRecordReaderBase;
import org.apache.hadoop.hive.ql.io.parquet.ProjectionPusher;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetTableUtils;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.parquet.ParquetRuntimeException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.InvalidSchemaException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.parquet.filter2.compat.RowGroupFilter.filterRowGroups;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.range;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;
import static org.apache.parquet.hadoop.ParquetInputFormat.getFilter;

/**
 * This reader is used to read a batch of record from inputsplit, part of the code is referred
 * from Apache Spark and Apache Parquet.
 */
public class VectorizedParquetRecordReader extends ParquetRecordReaderBase
  implements RecordReader<NullWritable, VectorizedRowBatch> {
  public static final Logger LOG = LoggerFactory.getLogger(VectorizedParquetRecordReader.class);

  private List<Integer> colsToInclude;

  protected MessageType fileSchema;
  protected MessageType requestedSchema;
  private List<String> columnNamesList;
  private List<TypeInfo> columnTypesList;
  private VectorizedRowBatchCtx rbCtx;
  private List<Integer> indexColumnsWanted;

  /**
   * For each request column, the reader to read this column. This is NULL if this column
   * is missing from the file, in which case we populate the attribute with NULL.
   */
  private VectorizedColumnReader[] columnReaders;

  /**
   * The number of rows that have been returned.
   */
  private long rowsReturned = 0;

  /**
   * The number of rows that have been reading, including the current in flight row group.
   */
  private long totalCountLoadedSoFar = 0;

  /**
   * The total number of rows this RecordReader will eventually read. The sum of the
   * rows of all the row groups.
   */
  protected long totalRowCount = 0;

  @VisibleForTesting
  public VectorizedParquetRecordReader(
      ParquetInputSplit inputSplit,
      JobConf conf) {
    try {
      serDeStats = new SerDeStats();
      projectionPusher = new ProjectionPusher();
      if (inputSplit != null) {
        initialize(inputSplit, conf);
        setTimeZoneConversion(jobConf, inputSplit.getPath());
      }
      colsToInclude = ColumnProjectionUtils.getReadColumnIDs(conf);
      rbCtx = Utilities.getVectorizedRowBatchCtx(conf);
    } catch (Throwable e) {
      LOG.error("Failed to create the vectorized reader due to exception " + e);
      throw new RuntimeException(e);
    }
  }

  public VectorizedParquetRecordReader(
    org.apache.hadoop.mapred.InputSplit oldInputSplit,
    JobConf conf) {
    try {
      serDeStats = new SerDeStats();
      projectionPusher = new ProjectionPusher();
      if (oldInputSplit != null) {
        initialize(getSplit(oldInputSplit, conf), conf);
        setTimeZoneConversion(jobConf, ((FileSplit) oldInputSplit).getPath());
      }
      colsToInclude = ColumnProjectionUtils.getReadColumnIDs(conf);
      rbCtx = Utilities.getVectorizedRowBatchCtx(conf);
    } catch (Throwable e) {
      LOG.error("Failed to create the vectorized reader due to exception " + e);
      throw new RuntimeException(e);
    }
  }

  public void initialize(
      ParquetInputSplit split,
      JobConf configuration) throws IOException, InterruptedException {

    jobConf = configuration;
    ParquetMetadata footer;
    List<BlockMetaData> blocks;
    boolean indexAccess =
      configuration.getBoolean(DataWritableReadSupport.PARQUET_COLUMN_INDEX_ACCESS, false);
    this.file = split.getPath();
    long[] rowGroupOffsets = split.getRowGroupOffsets();

    String columnNames = configuration.get(IOConstants.COLUMNS);
    columnNamesList = DataWritableReadSupport.getColumnNames(columnNames);
    String columnTypes = configuration.get(IOConstants.COLUMNS_TYPES);
    columnTypesList = DataWritableReadSupport.getColumnTypes(columnTypes);

    // if task.side.metadata is set, rowGroupOffsets is null
    if (rowGroupOffsets == null) {
      //TODO check whether rowGroupOffSets can be null
      // then we need to apply the predicate push down filter
      footer = readFooter(configuration, file, range(split.getStart(), split.getEnd()));
      MessageType fileSchema = footer.getFileMetaData().getSchema();
      FilterCompat.Filter filter = getFilter(configuration);
      blocks = filterRowGroups(filter, footer.getBlocks(), fileSchema);
    } else {
      // otherwise we find the row groups that were selected on the client
      footer = readFooter(configuration, file, NO_FILTER);
      Set<Long> offsets = new HashSet<>();
      for (long offset : rowGroupOffsets) {
        offsets.add(offset);
      }
      blocks = new ArrayList<>();
      for (BlockMetaData block : footer.getBlocks()) {
        if (offsets.contains(block.getStartingPos())) {
          blocks.add(block);
        }
      }
      // verify we found them all
      if (blocks.size() != rowGroupOffsets.length) {
        long[] foundRowGroupOffsets = new long[footer.getBlocks().size()];
        for (int i = 0; i < foundRowGroupOffsets.length; i++) {
          foundRowGroupOffsets[i] = footer.getBlocks().get(i).getStartingPos();
        }
        // this should never happen.
        // provide a good error message in case there's a bug
        throw new IllegalStateException(
          "All the offsets listed in the split should be found in the file."
            + " expected: " + Arrays.toString(rowGroupOffsets)
            + " found: " + blocks
            + " out of: " + Arrays.toString(foundRowGroupOffsets)
            + " in range " + split.getStart() + ", " + split.getEnd());
      }
    }

    for (BlockMetaData block : blocks) {
      this.totalRowCount += block.getRowCount();
    }
    this.fileSchema = footer.getFileMetaData().getSchema();

    MessageType tableSchema;
    if (indexAccess) {
      List<Integer> indexSequence = new ArrayList<>();

      // Generates a sequence list of indexes
      for(int i = 0; i < columnNamesList.size(); i++) {
        indexSequence.add(i);
      }

      tableSchema = DataWritableReadSupport.getSchemaByIndex(fileSchema, columnNamesList,
        indexSequence);
    } else {
      tableSchema = DataWritableReadSupport.getSchemaByName(fileSchema, columnNamesList,
        columnTypesList);
    }

    indexColumnsWanted = ColumnProjectionUtils.getReadColumnIDs(configuration);
    if (!ColumnProjectionUtils.isReadAllColumns(configuration) && !indexColumnsWanted.isEmpty()) {
      requestedSchema =
        DataWritableReadSupport.getSchemaByIndex(tableSchema, columnNamesList, indexColumnsWanted);
    } else {
      requestedSchema = fileSchema;
    }

    this.reader = new ParquetFileReader(
      configuration, footer.getFileMetaData(), file, blocks, requestedSchema.getColumns());
  }

  @Override
  public boolean next(
    NullWritable nullWritable,
    VectorizedRowBatch vectorizedRowBatch) throws IOException {
    return nextBatch(vectorizedRowBatch);
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public VectorizedRowBatch createValue() {
    return rbCtx.createVectorizedRowBatch();
  }

  @Override
  public long getPos() throws IOException {
    //TODO
    return 0;
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public float getProgress() throws IOException {
    //TODO
    return 0;
  }

  /**
   * Advances to the next batch of rows. Returns false if there are no more.
   */
  private boolean nextBatch(VectorizedRowBatch columnarBatch) throws IOException {
    columnarBatch.reset();
    if (rowsReturned >= totalRowCount) {
      return false;
    }
    checkEndOfRowGroup();

    int num = (int) Math.min(VectorizedRowBatch.DEFAULT_SIZE, totalCountLoadedSoFar - rowsReturned);
    for (int i = 0; i < columnReaders.length; ++i) {
      if (columnReaders[i] == null) {
        continue;
      }
      columnarBatch.cols[colsToInclude.get(i)].isRepeating = true;
      columnReaders[i].readBatch(num, columnarBatch.cols[colsToInclude.get(i)],
        columnTypesList.get(colsToInclude.get(i)));
    }
    rowsReturned += num;
    columnarBatch.size = num;
    return true;
  }

  private void checkEndOfRowGroup() throws IOException {
    if (rowsReturned != totalCountLoadedSoFar) {
      return;
    }
    PageReadStore pages = reader.readNextRowGroup();
    if (pages == null) {
      throw new IOException("expecting more rows but reached last block. Read "
        + rowsReturned + " out of " + totalRowCount);
    }
    List<ColumnDescriptor> columns = requestedSchema.getColumns();
    List<Type> types = requestedSchema.getFields();
    columnReaders = new VectorizedColumnReader[columns.size()];
    String timeZoneId = jobConf.get(ParquetTableUtils.PARQUET_INT96_WRITE_ZONE_PROPERTY);

    if (!ColumnProjectionUtils.isReadAllColumns(jobConf) && !indexColumnsWanted.isEmpty()) {
      for (int i = 0; i < types.size(); ++i) {
        columnReaders[i] =
          buildVectorizedParquetReader(columnTypesList.get(indexColumnsWanted.get(i)), types.get(i),
            pages, requestedSchema.getColumns(), timeZoneId, 0);
      }
    } else {
      for (int i = 0; i < types.size(); ++i) {
        columnReaders[i] = buildVectorizedParquetReader(columnTypesList.get(i), types.get(i), pages,
          requestedSchema.getColumns(), timeZoneId, 0);
      }
    }

    totalCountLoadedSoFar += pages.getRowCount();
  }

  private List<ColumnDescriptor> getAllColumnDescriptorByType(
    int depth,
    Type type,
    List<ColumnDescriptor> columns) throws ParquetRuntimeException {
    List<ColumnDescriptor> res = new ArrayList<>();
    for (ColumnDescriptor descriptor : columns) {
      if (depth >= descriptor.getPath().length) {
        throw new InvalidSchemaException("Corrupted Parquet schema");
      }
      if (type.getName().equals(descriptor.getPath()[depth])) {
        res.add(descriptor);
      }
    }
    return res;
  }

  // Build VectorizedParquetColumnReader via Hive typeInfo and Parquet schema
  private VectorizedColumnReader buildVectorizedParquetReader(
    TypeInfo typeInfo,
    Type type,
    PageReadStore pages,
    List<ColumnDescriptor> columnDescriptors,
    String conversionTimeZone,
    int depth) throws IOException {
    List<ColumnDescriptor> descriptors =
      getAllColumnDescriptorByType(depth, type, columnDescriptors);
    switch (typeInfo.getCategory()) {
    case PRIMITIVE:
      if (columnDescriptors == null || columnDescriptors.isEmpty()) {
        throw new RuntimeException(
          "Failed to find related Parquet column descriptor with type " + type);
      } else {
        return new VectorizedPrimitiveColumnReader(descriptors.get(0),
          pages.getPageReader(descriptors.get(0)), conversionTimeZone, type);
      }
    case STRUCT:
      StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
      List<VectorizedColumnReader> fieldReaders = new ArrayList<>();
      List<TypeInfo> fieldTypes = structTypeInfo.getAllStructFieldTypeInfos();
      List<Type> types = type.asGroupType().getFields();
      for (int i = 0; i < fieldTypes.size(); i++) {
        VectorizedColumnReader r =
          buildVectorizedParquetReader(fieldTypes.get(i), types.get(i), pages, descriptors,
              conversionTimeZone, depth + 1);
        if (r != null) {
          fieldReaders.add(r);
        } else {
          throw new RuntimeException(
            "Fail to build Parquet vectorized reader based on Hive type " + fieldTypes.get(i)
              .getTypeName() + " and Parquet type" + types.get(i).toString());
        }
      }
      return new VectorizedStructColumnReader(fieldReaders);
    case LIST:
    case MAP:
    case UNION:
    default:
      throw new RuntimeException("Unsupported category " + typeInfo.getCategory().name());
    }
  }
}
