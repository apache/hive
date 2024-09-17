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

package org.apache.hadoop.hive.ql.io.parquet.vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.FileMetadataCache;
import org.apache.hadoop.hive.common.io.encoded.MemoryBufferOrBuffers;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapCacheAwareFs;
import org.apache.hadoop.hive.llap.LlapHiveUtils;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.BucketIdentifier;
import org.apache.hadoop.hive.ql.io.HdfsUtils;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.RowPositionAwareVectorizedRecordReader;
import org.apache.hadoop.hive.ql.io.SyntheticFileId;
import org.apache.hadoop.hive.ql.io.parquet.ParquetRecordReaderBase;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.parquet.ParquetRuntimeException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.format.converter.ParquetMetadataConverter.MetadataFilter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.InvalidSchemaException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

/**
 * This reader is used to read a batch of record from inputsplit, part of the code is referred
 * from Apache Spark and Apache Parquet.
 */
public class VectorizedParquetRecordReader extends ParquetRecordReaderBase
  implements RecordReader<NullWritable, VectorizedRowBatch>, RowPositionAwareVectorizedRecordReader {
  public static final Logger LOG = LoggerFactory.getLogger(VectorizedParquetRecordReader.class);

  private List<Integer> colsToInclude;

  protected MessageType fileSchema;
  protected MessageType requestedSchema;
  private List<String> columnNamesList;
  private List<TypeInfo> columnTypesList;
  private VectorizedRowBatchCtx rbCtx;
  private Object[] partitionValues;
  private boolean addPartitionCols = true;
  private Path cacheFsPath;
  private static final int MAP_DEFINITION_LEVEL_MAX = 3;

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
  private ZoneId writerTimezone;
  private final BucketIdentifier bucketIdentifier;

  // number of rows returned with the last batch
  private int lastReturnedRowCount = -1;

  // row number (in the file) of the first row returned in the last batch
  private long currentRowNumInRowGroup = -1;

  // index of the current rowgroup, incremented after reader.readNextRowGroup() calls
  private int currentRowGroupIndex = -1;

  private Map<Integer, Long> rowGroupNumToRowPos = new HashMap<>();

  // LLAP cache integration
  // TODO: also support fileKey in splits, like OrcSplit does
  private Object cacheKey = null;
  private CacheTag cacheTag = null;

  public VectorizedParquetRecordReader(InputSplit oldInputSplit, JobConf conf) throws IOException {
    this(oldInputSplit, conf, null, null, null);
  }

  public VectorizedParquetRecordReader(InputSplit oldInputSplit, JobConf conf, FileMetadataCache metadataCache,
      DataCache dataCache, Configuration cacheConf, ParquetMetadata parquetMetadata) throws IOException {
    super(conf, oldInputSplit);
    try {
      this.metadataCache = metadataCache;
      this.cache = dataCache;
      this.cacheConf = cacheConf;

      if (metadataCache != null) {
        cacheKey = SyntheticFileId.fromJobConf(conf);
        if (cacheKey == null) {
          cacheKey = LlapHiveUtils.createFileIdUsingFS(filePath.getFileSystem(conf), filePath, cacheConf);
        }
        // createFileIdUsingFS() might yield to null in certain configurations
        if (cacheKey != null) {
          cacheTag = cacheTagOfParquetFile(filePath, cacheConf, conf);
          // If we are going to use cache, change the path to depend on file ID for extra consistency.
          if (cacheKey instanceof Long && HiveConf.getBoolVar(
              cacheConf, ConfVars.LLAP_IO_USE_FILEID_PATH)) {
            filePath = HdfsUtils.getFileIdPath(filePath, (long)cacheKey);
          }
        }
      }

      setupMetadataAndParquetSplit(conf, parquetMetadata);

      colsToInclude = ColumnProjectionUtils.getReadColumnIDs(conf);
      //initialize the rowbatchContext
      rbCtx = Utilities.getVectorizedRowBatchCtx(jobConf);

      if (parquetInputSplit != null) {
        initialize(parquetInputSplit, conf);
      }
      initPartitionValues(fileSplit, conf);
      bucketIdentifier = BucketIdentifier.from(conf, filePath);
    } catch (Throwable e) {
      LOG.error("Failed to create the vectorized reader due to exception " + e);
      throw new RuntimeException(e);
    }
  }

  public VectorizedParquetRecordReader(InputSplit oldInputSplit, JobConf conf, FileMetadataCache metadataCache,
      DataCache dataCache, Configuration cacheConf) throws IOException {
    this(oldInputSplit, conf, metadataCache, dataCache, cacheConf, null);
  }

  private void initPartitionValues(FileSplit fileSplit, JobConf conf) throws IOException {
     int partitionColumnCount = rbCtx.getPartitionColumnCount();
     if (partitionColumnCount > 0) {
       partitionValues = new Object[partitionColumnCount];
       VectorizedRowBatchCtx.getPartitionValues(rbCtx, conf, fileSplit, partitionValues);
     } else {
       partitionValues = null;
     }
  }

  @Override
  protected ParquetMetadata getParquetMetadata(Path path, JobConf conf) throws IOException {
    return readSplitFooter(conf, filePath, cacheKey, NO_FILTER, cacheTag);
  }

  @SuppressWarnings("deprecation")
  public void initialize(
      ParquetInputSplit split,
    JobConf configuration) throws IOException, InterruptedException, HiveException {

    List<BlockMetaData> blocks;

    boolean indexAccess =
      configuration.getBoolean(DataWritableReadSupport.PARQUET_COLUMN_INDEX_ACCESS, false);
    long[] rowGroupOffsets = split.getRowGroupOffsets();

    String columnNames = configuration.get(IOConstants.COLUMNS);
    columnNamesList = DataWritableReadSupport.getColumnNames(columnNames);
    String columnTypes = configuration.get(IOConstants.COLUMNS_TYPES);
    columnTypesList = DataWritableReadSupport.getColumnTypes(columnTypes);

    Set<Long> offsets = new HashSet<>();
    for (long offset : rowGroupOffsets) {
      offsets.add(offset);
    }
    blocks = new ArrayList<>();
    long allRowsInFile = 0;
    int blockIndex = 0;
    for (BlockMetaData block : parquetMetadata.getBlocks()) {
      if (offsets.contains(block.getStartingPos())) {
        rowGroupNumToRowPos.put(blockIndex++, allRowsInFile);
        blocks.add(block);
      }
      allRowsInFile += block.getRowCount();
    }
    // verify we found them all
    if (blocks.size() != rowGroupOffsets.length) {
      long[] foundRowGroupOffsets = new long[parquetMetadata.getBlocks().size()];
      for (int i = 0; i < foundRowGroupOffsets.length; i++) {
        foundRowGroupOffsets[i] = parquetMetadata.getBlocks().get(i).getStartingPos();
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

    for (BlockMetaData block : blocks) {
      this.totalRowCount += block.getRowCount();
    }
    this.fileSchema = parquetMetadata.getFileMetaData().getSchema();
    this.writerTimezone = DataWritableReadSupport
        .getWriterTimeZoneId(parquetMetadata.getFileMetaData().getKeyValueMetaData());

    colsToInclude = ColumnProjectionUtils.getReadColumnIDs(configuration);
    requestedSchema = DataWritableReadSupport
      .getRequestedSchema(indexAccess, columnNamesList, columnTypesList, fileSchema, configuration);

    Path path = wrapPathForCache(filePath, cacheKey, configuration, blocks, cacheTag);
    this.reader = new ParquetFileReader(
      configuration, parquetMetadata.getFileMetaData(), path, blocks, requestedSchema.getColumns());
  }

  private Path wrapPathForCache(Path path, Object fileKey, JobConf configuration,
      List<BlockMetaData> blocks, CacheTag tag) throws IOException {
    if (fileKey == null || cache == null) {
      return path;
    }
    HashSet<ColumnPath> includedCols = new HashSet<>();
    for (ColumnDescriptor col : requestedSchema.getColumns()) {
      includedCols.add(ColumnPath.get(col.getPath()));
    }
    // We could make some assumptions given how the reader currently does the work (consecutive
    // chunks, etc.; blocks and columns stored in offset order in the lists), but we won't -
    // just save all the chunk boundaries and lengths for now.
    TreeMap<Long, Long> chunkIndex = new TreeMap<>();
    for (BlockMetaData block : blocks) {
      for (ColumnChunkMetaData mc : block.getColumns()) {
        if (!includedCols.contains(mc.getPath())) continue;
        chunkIndex.put(mc.getStartingPos(), mc.getStartingPos() + mc.getTotalSize());
      }
    }
    // Register the cache-aware path so that Parquet reader would go thru it.
    configuration.set("fs." + LlapCacheAwareFs.SCHEME + ".impl",
        LlapCacheAwareFs.class.getCanonicalName());
    path = LlapCacheAwareFs.registerFile(cache, path, fileKey, chunkIndex, configuration, tag);
    this.cacheFsPath = path;
    return path;
  }

  private ParquetMetadata readSplitFooter(JobConf configuration, final Path file,
      Object cacheKey, MetadataFilter filter, CacheTag tag) throws IOException {
    if (cacheKey == null || metadataCache == null) {
      // Non-LLAP case
      FileSystem fs = file.getFileSystem(configuration);
      FileStatus stat = fs.getFileStatus(file);
      return readFooterFromFile(file, fs, stat, filter);
    } else {
      MemoryBufferOrBuffers footerData = LlapProxy.getIo().getParquetFooterBuffersFromCache(file, configuration, cacheKey);
      return ParquetFileReader.readFooter(new ParquetFooterInputFromCache(footerData), filter);
    }
  }

  private ParquetMetadata readFooterFromFile(final Path file, final FileSystem fs,
      final FileStatus stat, MetadataFilter filter) throws IOException {
    InputFile inputFile = new InputFile() {
      @Override
      public SeekableInputStream newStream() throws IOException {
        return HadoopStreams.wrap(fs.open(file));
      }
      @Override
      public long getLength() {
        return stat.getLen();
      }
    };
    return ParquetFileReader.readFooter(inputFile, filter);
  }

  public static CacheTag cacheTagOfParquetFile(Path path, Configuration cacheConf, JobConf jobConf) {
    MapWork mapWork = LlapHiveUtils.findMapWork(jobConf);
    if (!HiveConf.getBoolVar(cacheConf, ConfVars.LLAP_TRACK_CACHE_USAGE) || mapWork == null) {
      return null;
    }
    PartitionDesc partitionDesc = LlapHiveUtils.partitionDescForPath(path, mapWork.getPathToPartitionInfo());
    return LlapHiveUtils.getDbAndTableNameForMetrics(path, true, partitionDesc);
  }


  private FileMetadataCache metadataCache;
  private DataCache cache;
  private Configuration cacheConf;

  @Override
  public boolean next(
    NullWritable nullWritable,
    VectorizedRowBatch vectorizedRowBatch) throws IOException {
    boolean hasMore = nextBatch(vectorizedRowBatch);
    if (bucketIdentifier != null) {
      rbCtx.setBucketAndWriteIdOf(vectorizedRowBatch, bucketIdentifier);
    }
    return hasMore;
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
    if (cacheFsPath != null) {
      LlapCacheAwareFs.unregisterFile(cacheFsPath);
    }
    if (reader != null) {
      reader.close();
    }
  }

  @Override
  public float getProgress() throws IOException {
    //TODO
    return 0;
  }

  @Override
  public long getRowNumber() throws IOException {
    return rowGroupNumToRowPos.get(currentRowGroupIndex) + currentRowNumInRowGroup;
  }

  /**
   * Advances to the next batch of rows. Returns false if there are no more.
   */
  private boolean nextBatch(VectorizedRowBatch columnarBatch) throws IOException {
    currentRowNumInRowGroup += lastReturnedRowCount;

    VectorizedBatchUtil.resetNonPartitionColumns(columnarBatch);
    if (rowsReturned >= totalRowCount) {
      return false;
    }

    // Add partition cols if necessary (see VectorizedOrcInputFormat for details).
    if (addPartitionCols) {
      if (partitionValues != null) {
        rbCtx.addPartitionColsToBatch(columnarBatch, partitionValues);
      }
      addPartitionCols = false;
    }
    checkEndOfRowGroup();

    int num = (int) Math.min(VectorizedRowBatch.DEFAULT_SIZE, totalCountLoadedSoFar - rowsReturned);
    if (colsToInclude.size() > 0) {
      for (int i = 0; i < columnReaders.length; ++i) {
        if (columnReaders[i] == null) {
          continue;
        }
        columnarBatch.cols[colsToInclude.get(i)].isRepeating = true;
        columnReaders[i].readBatch(num, columnarBatch.cols[colsToInclude.get(i)],
            columnTypesList.get(colsToInclude.get(i)));
      }
    }
    lastReturnedRowCount = num;
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

    if (!ColumnProjectionUtils.isReadAllColumns(jobConf)) {
      //certain queries like select count(*) from table do not have
      //any projected columns and still have isReadAllColumns as false
      //in such cases columnReaders are not needed
      //However, if colsToInclude is not empty we should initialize each columnReader
      if(!colsToInclude.isEmpty()) {
        for (int i = 0; i < types.size(); ++i) {
          columnReaders[i] =
              buildVectorizedParquetReader(columnTypesList.get(colsToInclude.get(i)), types.get(i),
                  pages, requestedSchema.getColumns(), skipTimestampConversion, writerTimezone, skipProlepticConversion,
                  legacyConversionEnabled, 0);
        }
      }
    } else {
      for (int i = 0; i < types.size(); ++i) {
        columnReaders[i] = buildVectorizedParquetReader(columnTypesList.get(i), types.get(i), pages,
          requestedSchema.getColumns(), skipTimestampConversion, writerTimezone, skipProlepticConversion,
          legacyConversionEnabled, 0);
      }
    }

    currentRowNumInRowGroup = 0;
    currentRowGroupIndex++;

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

  // TODO support only non nested case
  private PrimitiveType getElementType(Type type) {
    if (type.isPrimitive()) {
      return type.asPrimitiveType();
    }
    if (type.asGroupType().getFields().size() > 1) {
      throw new RuntimeException(
          "Current Parquet Vectorization reader doesn't support nested type");
    }

    Type childType = type.asGroupType().getFields().get(0);

    // Parquet file generated using thrift may have child type as PrimitiveType
    if (childType.isPrimitive()) {
      return childType.asPrimitiveType();
    } else {
      return childType.asGroupType().getFields().get(0).asPrimitiveType();
    }
  }

  // Build VectorizedParquetColumnReader via Hive typeInfo and Parquet schema
  private VectorizedColumnReader buildVectorizedParquetReader(
    TypeInfo typeInfo,
    Type type,
    PageReadStore pages,
    List<ColumnDescriptor> columnDescriptors,
    boolean skipTimestampConversion,
    ZoneId writerTimezone,
    boolean skipProlepticConversion,
    boolean legacyConversionEnabled,
    int depth) throws IOException {
    List<ColumnDescriptor> descriptors =
      getAllColumnDescriptorByType(depth, type, columnDescriptors);
    switch (typeInfo.getCategory()) {
    case PRIMITIVE:
      if (columnDescriptors == null || columnDescriptors.isEmpty()) {
        throw new RuntimeException(
          "Failed to find related Parquet column descriptor with type " + type);
      }
      if (fileSchema.getColumns().contains(descriptors.get(0))) {
        return new VectorizedPrimitiveColumnReader(descriptors.get(0),
            pages.getPageReader(descriptors.get(0)), skipTimestampConversion, writerTimezone, skipProlepticConversion,
            legacyConversionEnabled, type, typeInfo);
      } else {
        // Support for schema evolution
        return new VectorizedDummyColumnReader();
      }
    case STRUCT:
      StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
      List<VectorizedColumnReader> fieldReaders = new ArrayList<>();
      List<TypeInfo> fieldTypes = structTypeInfo.getAllStructFieldTypeInfos();
      List<Type> types = type.asGroupType().getFields();
      for (int i = 0; i < fieldTypes.size(); i++) {
        VectorizedColumnReader r =
          buildVectorizedParquetReader(fieldTypes.get(i), types.get(i), pages, descriptors,
            skipTimestampConversion, writerTimezone, skipProlepticConversion, legacyConversionEnabled, depth + 1);
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
      checkListColumnSupport(((ListTypeInfo) typeInfo).getListElementTypeInfo());
      if (columnDescriptors == null || columnDescriptors.isEmpty()) {
        throw new RuntimeException(
            "Failed to find related Parquet column descriptor with type " + type);
      }

      return new VectorizedListColumnReader(descriptors.get(0),
          pages.getPageReader(descriptors.get(0)), skipTimestampConversion, writerTimezone, skipProlepticConversion,
          legacyConversionEnabled, getElementType(type), typeInfo);
    case MAP:
      if (columnDescriptors == null || columnDescriptors.isEmpty()) {
        throw new RuntimeException(
            "Failed to find related Parquet column descriptor with type " + type);
      }

      // to handle the different Map definition in Parquet, eg:
      // definition has 1 group:
      //   repeated group map (MAP_KEY_VALUE)
      //     {required binary key (UTF8); optional binary value (UTF8);}
      // definition has 2 groups:
      //   optional group m1 (MAP) {
      //     repeated group map (MAP_KEY_VALUE)
      //       {required binary key (UTF8); optional binary value (UTF8);}
      //   }
      int nestGroup = 0;
      GroupType groupType = type.asGroupType();
      // if FieldCount == 2, get types for key & value,
      // otherwise, continue to get the group type until MAP_DEFINITION_LEVEL_MAX.
      while (groupType.getFieldCount() < 2) {
        if (nestGroup > MAP_DEFINITION_LEVEL_MAX) {
          throw new RuntimeException(
              "More than " + MAP_DEFINITION_LEVEL_MAX + " level is found in Map definition, " +
                  "Failed to get the field types for Map with type " + type);
        }
        groupType = groupType.getFields().get(0).asGroupType();
        nestGroup++;
      }
      List<Type> kvTypes = groupType.getFields();
      VectorizedListColumnReader keyListColumnReader = new VectorizedListColumnReader(
          descriptors.get(0), pages.getPageReader(descriptors.get(0)), skipTimestampConversion,
          writerTimezone, skipProlepticConversion, legacyConversionEnabled, kvTypes.get(0), typeInfo);
      VectorizedListColumnReader valueListColumnReader = new VectorizedListColumnReader(
          descriptors.get(1), pages.getPageReader(descriptors.get(1)), skipTimestampConversion,
          writerTimezone, skipProlepticConversion, legacyConversionEnabled, kvTypes.get(1), typeInfo);
      return new VectorizedMapColumnReader(keyListColumnReader, valueListColumnReader);
    case UNION:
    default:
      throw new RuntimeException("Unsupported category " + typeInfo.getCategory().name());
    }
  }

  /**
   * Check if the element type in list is supported by vectorization read.
   * Supported type: INT, BYTE, SHORT, DATE, INTERVAL_YEAR_MONTH, LONG, BOOLEAN, DOUBLE, BINARY, STRING, CHAR, VARCHAR,
   *                 FLOAT, DECIMAL
   */
  private void checkListColumnSupport(TypeInfo elementType) {
    if (elementType instanceof PrimitiveTypeInfo) {
      switch (((PrimitiveTypeInfo)elementType).getPrimitiveCategory()) {
        case INTERVAL_DAY_TIME:
        case TIMESTAMP:
          throw new RuntimeException("Unsupported primitive type used in list:: " + elementType);
      }
    } else {
      throw new RuntimeException("Unsupported type used in list:" + elementType);
    }
  }
}
