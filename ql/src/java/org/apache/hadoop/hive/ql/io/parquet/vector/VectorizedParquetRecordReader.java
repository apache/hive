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
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.FileMetadataCache;
import org.apache.hadoop.hive.common.io.encoded.MemoryBufferOrBuffers;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapCacheAwareFs;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.HdfsUtils;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.ParquetRecordReaderBase;
import org.apache.hadoop.hive.ql.io.parquet.ProjectionPusher;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.parquet.ParquetRuntimeException;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.format.converter.ParquetMetadataConverter.MetadataFilter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.parquet.filter2.compat.RowGroupFilter.filterRowGroups;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.range;
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
  private Object[] partitionValues;
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

  public VectorizedParquetRecordReader(
      org.apache.hadoop.mapred.InputSplit oldInputSplit, JobConf conf) {
    this(oldInputSplit, conf, null, null, null);
  }

  public VectorizedParquetRecordReader(
      org.apache.hadoop.mapred.InputSplit oldInputSplit, JobConf conf,
      FileMetadataCache metadataCache, DataCache dataCache, Configuration cacheConf) {
    try {
      this.metadataCache = metadataCache;
      this.cache = dataCache;
      this.cacheConf = cacheConf;
      serDeStats = new SerDeStats();
      projectionPusher = new ProjectionPusher();
      colsToInclude = ColumnProjectionUtils.getReadColumnIDs(conf);
      //initialize the rowbatchContext
      jobConf = conf;
      rbCtx = Utilities.getVectorizedRowBatchCtx(jobConf);
      ParquetInputSplit inputSplit = getSplit(oldInputSplit, conf);
      if (inputSplit != null) {
        initialize(inputSplit, conf);
      }
      initPartitionValues((FileSplit) oldInputSplit, conf);
    } catch (Throwable e) {
      LOG.error("Failed to create the vectorized reader due to exception " + e);
      throw new RuntimeException(e);
    }
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

  @SuppressWarnings("deprecation")
  public void initialize(
    InputSplit oldSplit,
    JobConf configuration) throws IOException, InterruptedException {
    // the oldSplit may be null during the split phase
    if (oldSplit == null) {
      return;
    }
    ParquetMetadata footer;
    List<BlockMetaData> blocks;
    ParquetInputSplit split = (ParquetInputSplit) oldSplit;
    boolean indexAccess =
      configuration.getBoolean(DataWritableReadSupport.PARQUET_COLUMN_INDEX_ACCESS, false);
    this.file = split.getPath();
    long[] rowGroupOffsets = split.getRowGroupOffsets();

    String columnNames = configuration.get(IOConstants.COLUMNS);
    columnNamesList = DataWritableReadSupport.getColumnNames(columnNames);
    String columnTypes = configuration.get(IOConstants.COLUMNS_TYPES);
    columnTypesList = DataWritableReadSupport.getColumnTypes(columnTypes);

    // if task.side.metadata is set, rowGroupOffsets is null
    Object cacheKey = null;
    // TODO: also support fileKey in splits, like OrcSplit does
    if (metadataCache != null) {
      cacheKey = HdfsUtils.getFileId(file.getFileSystem(configuration), file,
        HiveConf.getBoolVar(cacheConf, ConfVars.LLAP_CACHE_ALLOW_SYNTHETIC_FILEID),
        HiveConf.getBoolVar(cacheConf, ConfVars.LLAP_CACHE_DEFAULT_FS_FILE_ID));
    }
    if (cacheKey != null) {
      // If we are going to use cache, change the path to depend on file ID for extra consistency.
      FileSystem fs = file.getFileSystem(configuration);
      if (cacheKey instanceof Long && HiveConf.getBoolVar(
          cacheConf, ConfVars.LLAP_IO_USE_FILEID_PATH)) {
        file = HdfsUtils.getFileIdPath(fs, file, (long)cacheKey);
      }
    }

    if (rowGroupOffsets == null) {
      //TODO check whether rowGroupOffSets can be null
      // then we need to apply the predicate push down filter
      footer = readSplitFooter(
          configuration, file, cacheKey, range(split.getStart(), split.getEnd()));
      MessageType fileSchema = footer.getFileMetaData().getSchema();
      FilterCompat.Filter filter = getFilter(configuration);
      blocks = filterRowGroups(filter, footer.getBlocks(), fileSchema);
    } else {
      // otherwise we find the row groups that were selected on the client
      footer = readSplitFooter(configuration, file, cacheKey, NO_FILTER);
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

    colsToInclude = ColumnProjectionUtils.getReadColumnIDs(configuration);
    requestedSchema = DataWritableReadSupport
      .getRequestedSchema(indexAccess, columnNamesList, columnTypesList, fileSchema, configuration);
 
    Path path = wrapPathForCache(file, cacheKey, configuration, blocks);
    this.reader = new ParquetFileReader(
      configuration, footer.getFileMetaData(), path, blocks, requestedSchema.getColumns());
  }

  private Path wrapPathForCache(Path path, Object fileKey, JobConf configuration,
      List<BlockMetaData> blocks) throws IOException {
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
    path = LlapCacheAwareFs.registerFile(cache, path, fileKey, chunkIndex, configuration);
    this.cacheFsPath = path;
    return path;
  }

  private ParquetMetadata readSplitFooter(
      JobConf configuration, final Path file, Object cacheKey, MetadataFilter filter) throws IOException {
    MemoryBufferOrBuffers footerData = (cacheKey == null || metadataCache == null) ? null
        : metadataCache.getFileMetadata(cacheKey);
    if (footerData != null) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Found the footer in cache for " + cacheKey);
      }
      try {
        return ParquetFileReader.readFooter(new ParquetFooterInputFromCache(footerData), filter);
      } finally {
        metadataCache.decRefBuffer(footerData);
      }
    }
    final FileSystem fs = file.getFileSystem(configuration);
    final FileStatus stat = fs.getFileStatus(file);
    if (cacheKey == null || metadataCache == null) {
      return readFooterFromFile(file, fs, stat, filter);
    }

    // To avoid reading the footer twice, we will cache it first and then read from cache.
    // Parquet calls protobuf methods directly on the stream and we can't get bytes after the fact.
    try (SeekableInputStream stream = HadoopStreams.wrap(fs.open(file))) {
      long footerLengthIndex = stat.getLen()
          - ParquetFooterInputFromCache.FOOTER_LENGTH_SIZE - ParquetFileWriter.MAGIC.length;
      stream.seek(footerLengthIndex);
      int footerLength = BytesUtils.readIntLittleEndian(stream);
      stream.seek(footerLengthIndex - footerLength);
      if (LOG.isInfoEnabled()) {
        LOG.info("Caching the footer of length " + footerLength + " for " + cacheKey);
      }
      footerData = metadataCache.putFileMetadata(cacheKey, footerLength, stream);
      try {
        return ParquetFileReader.readFooter(new ParquetFooterInputFromCache(footerData), filter);
      } finally {
        metadataCache.decRefBuffer(footerData);
      }
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
      public long getLength() throws IOException {
        return stat.getLen();
      }
    };
    return ParquetFileReader.readFooter(inputFile, filter);
  }


  private FileMetadataCache metadataCache;
  private DataCache cache;
  private Configuration cacheConf;

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

  /**
   * Advances to the next batch of rows. Returns false if there are no more.
   */
  private boolean nextBatch(VectorizedRowBatch columnarBatch) throws IOException {
    columnarBatch.reset();
    if (rowsReturned >= totalRowCount) {
      return false;
    }

    // Add partition cols if necessary (see VectorizedOrcInputFormat for details).
    if (partitionValues != null) {
      rbCtx.addPartitionColsToBatch(columnarBatch, partitionValues);
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
                  pages, requestedSchema.getColumns(), skipTimestampConversion, 0);
        }
      }
    } else {
      for (int i = 0; i < types.size(); ++i) {
        columnReaders[i] = buildVectorizedParquetReader(columnTypesList.get(i), types.get(i), pages,
          requestedSchema.getColumns(), skipTimestampConversion, 0);
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

  // TODO support only non nested case
  private PrimitiveType getElementType(Type type) {
    if (type.isPrimitive()) {
      return type.asPrimitiveType();
    }
    if (type.asGroupType().getFields().size() > 1) {
      throw new RuntimeException(
          "Current Parquet Vectorization reader doesn't support nested type");
    }
    return type.asGroupType().getFields().get(0).asGroupType().getFields().get(0)
        .asPrimitiveType();
  }

  // Build VectorizedParquetColumnReader via Hive typeInfo and Parquet schema
  private VectorizedColumnReader buildVectorizedParquetReader(
    TypeInfo typeInfo,
    Type type,
    PageReadStore pages,
    List<ColumnDescriptor> columnDescriptors,
    boolean skipTimestampConversion,
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
          pages.getPageReader(descriptors.get(0)), skipTimestampConversion, type, typeInfo);
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
            skipTimestampConversion, depth + 1);
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
          pages.getPageReader(descriptors.get(0)), skipTimestampConversion, getElementType(type),
          typeInfo);
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
          kvTypes.get(0), typeInfo);
      VectorizedListColumnReader valueListColumnReader = new VectorizedListColumnReader(
          descriptors.get(1), pages.getPageReader(descriptors.get(1)), skipTimestampConversion,
          kvTypes.get(1), typeInfo);
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
