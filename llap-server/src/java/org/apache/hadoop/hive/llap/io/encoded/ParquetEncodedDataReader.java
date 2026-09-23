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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.llap.io.encoded;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.common.io.Allocator.BufferObjectFactory;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.common.io.DataCache.BooleanRef;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.common.io.encoded.MemoryBufferOrBuffers;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.ConsumerFeedback;
import org.apache.hadoop.hive.llap.ParquetCacheLayout;
import org.apache.hadoop.hive.llap.ParquetRangeBuffers;
import org.apache.hadoop.hive.llap.LlapHiveUtils;
import org.apache.hadoop.hive.llap.cache.BufferUsageManager;
import org.apache.hadoop.hive.llap.cache.LlapDataBuffer;
import org.apache.hadoop.hive.llap.cache.LowLevelCache;
import org.apache.hadoop.hive.llap.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.llap.counters.LlapIOCounters;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.llap.io.decode.ParquetEncodedDataConsumer;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.SyntheticFileId;
import org.apache.hadoop.hive.ql.io.orc.encoded.CacheChunk;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.vector.ParquetFooterInputFromCache;
import org.apache.hadoop.hive.ql.io.orc.encoded.StoppableAllocator;
import org.apache.hadoop.hive.ql.io.parquet.ParquetRecordReaderBase;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetRecordReader;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.metadata.RowLineageUtils;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.functional.FutureIO;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.RowGroupFilter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.tez.common.CallableWithNdc;

/**
 * Reads one Parquet split through the LLAP cache on an IO thread. Row groups whose first data
 * page falls in the split are selected and filtered by their statistics; for each one the
 * projected column chunks are looked up in the cache, the missing ranges are requested in one
 * vectored read, and the chunks are handed to the consumer to decode. The next row group's
 * request is issued before the current one is decoded so its transfer overlaps the decode, as
 * long as the two together stay within this thread's share of the cache. Every buffer in a
 * consumed batch carries exactly one ref, owned by the batch until returnData.
 */
public class ParquetEncodedDataReader extends CallableWithNdc<Void>
    implements ConsumerFeedback<ParquetEncodedColumnBatch> {

  private static final BufferObjectFactory DATA_BUFFER_FACTORY = LlapDataBuffer::new;

  private final LowLevelCache lowLevelCache;
  private final BufferUsageManager bufferManager;
  private final Configuration daemonConf;
  private final ParquetCacheLayout layout;
  private final JobConf jobConf;
  private final FileSplit split;
  private final ParquetEncodedDataConsumer consumer;
  private final QueryFragmentCounters counters;
  private final UserGroupInformation ugi;
  private final Path path;
  private final boolean cacheOnly;
  /** Bytes of column chunks one IO thread may hold across the row group in decode and the next. */
  private final long lookaheadBudget;

  private Object fileKey;
  private CacheTag cacheTag;
  private ParquetMetadata footer;
  private MessageType requestedSchema;
  private final AtomicBoolean isStopped = new AtomicBoolean(false);

  public ParquetEncodedDataReader(LowLevelCache lowLevelCache, BufferUsageManager bufferManager,
      Configuration daemonConf, Configuration jobConf, FileSplit split,
      ParquetEncodedDataConsumer consumer, QueryFragmentCounters counters) throws IOException {
    this.lowLevelCache = lowLevelCache;
    this.bufferManager = bufferManager;
    this.daemonConf = daemonConf;
    this.layout = new ParquetCacheLayout(bufferManager.getAllocator(), daemonConf);
    this.jobConf = (JobConf) jobConf;
    this.split = split;
    this.consumer = consumer;
    this.counters = counters;
    this.path = split.getPath();
    this.ugi = UserGroupInformation.getCurrentUser();
    this.cacheOnly = HiveConf.getBoolVar(jobConf, ConfVars.LLAP_IO_CACHE_ONLY);
    this.lookaheadBudget = HiveConf.getSizeVar(daemonConf, ConfVars.LLAP_IO_MEMORY_MAX_SIZE)
        / Math.max(1, HiveConf.getIntVar(daemonConf, ConfVars.LLAP_IO_THREADPOOL_SIZE));
  }

  /** Reads the footer once (through the LLAP footer cache when the file has a usable key). */
  public ParquetMetadata loadFooter() throws IOException {
    fileKey = SyntheticFileId.fromJobConf(jobConf);
    if (fileKey == null) {
      fileKey = LlapHiveUtils.createFileIdUsingFS(path.getFileSystem(jobConf), path, daemonConf);
    }
    if (fileKey != null) {
      cacheTag = VectorizedParquetRecordReader.cacheTagOfParquetFile(path, daemonConf, jobConf);
      // Bumps METADATA_CACHE_HIT / METADATA_CACHE_MISS so the LLAP IO summary accounts for the
      // Parquet footer lookup the same way it does for ORC's file tail.
      BooleanRef cacheHit = new BooleanRef();
      MemoryBufferOrBuffers footerData =
          LlapProxy.getIo().getParquetFooterBuffersFromCache(path, jobConf, fileKey, cacheHit);
      counters.incrCounter(cacheHit.value
          ? LlapIOCounters.METADATA_CACHE_HIT : LlapIOCounters.METADATA_CACHE_MISS);
      footer = ParquetFileReader.readFooter(
          new ParquetFooterInputFromCache(footerData), ParquetMetadataConverter.NO_FILTER);
    } else {
      // Fallback path: no cache key, so we read status + footer straight from HDFS. Time both
      // under HDFS_TIME_NS to match how ORC accounts for cache-miss footer reads.
      final FileSystem fs = path.getFileSystem(jobConf);
      long hdfsStart = counters.startTimeCounter();
      final FileStatus stat = fs.getFileStatus(path);
      counters.recordHdfsTime(hdfsStart);
      InputFile inputFile = new InputFile() {
        @Override
        public SeekableInputStream newStream() throws IOException {
          return HadoopStreams.wrap(fs.open(path));
        }
        @Override
        public long getLength() {
          return stat.getLen();
        }
      };
      hdfsStart = counters.startTimeCounter();
      footer = ParquetFileReader.readFooter(inputFile, ParquetMetadataConverter.NO_FILTER);
      counters.recordHdfsTime(hdfsStart);
    }
    requestedSchema = DataWritableReadSupport.getRequestedSchema(
        jobConf.getBoolean(DataWritableReadSupport.PARQUET_COLUMN_INDEX_ACCESS, false),
        DataWritableReadSupport.getColumnNames(jobConf.get(IOConstants.COLUMNS)),
        DataWritableReadSupport.getColumnTypes(jobConf.get(IOConstants.COLUMNS_TYPES)),
        footer.getFileMetaData().getSchema(), jobConf);
    return footer;
  }

  @Override
  protected Void callInternal() throws IOException, InterruptedException {
    return ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      long startTime = counters.startTimeCounter();
      try {
        performDataRead();
        consumer.setDone();
      } catch (Exception e) {
        // A shutdown-triggered InterruptedException must not be silently reported as an ordinary
        // consumer error: preserve the interrupt on the thread so any caller sees it.
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        consumer.setError(e);
      } finally {
        counters.incrWallClockCounter(LlapIOCounters.TOTAL_IO_TIME_NS, startTime);
      }
      return null;
    });
  }

  private void performDataRead() throws IOException, InterruptedException {
    // The LLAP IO summary keys off TABLE / FILE / STRIPES; set them the same way ORC does so a
    // native Parquet fragment shows up in the summary with the same fields populated.
    if (cacheTag != null) {
      counters.setDesc(QueryFragmentCounters.Desc.TABLE, cacheTag.getTableName());
    }
    counters.setDesc(QueryFragmentCounters.Desc.FILE, path
        + (fileKey == null ? "" : " (" + fileKey + ")"));

    MessageType fileSchema = footer.getFileMetaData().getSchema();
    int[] projected = projectedLeaves(requestedSchema, fileSchema);
    consumer.setFileMetadata(footer, requestedSchema, path);

    final Allocator allocator = bufferManager.getAllocator();
    final long splitStart = split.getStart();
    final long splitEnd = splitStart + split.getLength();
    final List<BlockMetaData> blocks = footer.getBlocks();
    List<BlockMetaData> selected = new ArrayList<>();
    for (BlockMetaData block : blocks) {
      long firstDataPage = block.getColumns().getFirst().getFirstDataPageOffset();
      if (firstDataPage >= splitStart && firstDataPage < splitEnd) {
        selected.add(block);
      }
    }
    FilterPredicate predicate = ParquetRecordReaderBase.toFilterPredicate(jobConf, fileSchema);
    if (predicate != null) {
      selected = RowGroupFilter.filterRowGroups(FilterCompat.get(predicate), selected, fileSchema);
    }
    Map<BlockMetaData, Integer> rowGroupOf = new IdentityHashMap<>();
    for (int i = 0; i < blocks.size(); ++i) {
      rowGroupOf.put(blocks.get(i), i);
    }
    // STRIPES is the ORC term; for Parquet the analog is row groups. Reuse the same descriptor so
    // the summary layout stays common and we don't have to teach the reporter about a new field.
    counters.setDesc(QueryFragmentCounters.Desc.STRIPES, "0," + selected.size());
    counters.incrCounter(LlapIOCounters.SELECTED_ROWGROUPS, selected.size());

    FileSystem fs = path.getFileSystem(jobConf);
    try (FSDataInputStream fileStream = openFile(fs)) {
      ParquetRangeBuffers buffers = ParquetRangeBuffers.forStream(fileStream);
      Deque<Fetch> inFlight = new ArrayDeque<>();
      try {
        for (int i = 0; i < selected.size() && !isStopped.get(); ++i) {
          if (inFlight.isEmpty()) {
            inFlight.add(startFetch(fileStream, buffers, allocator, projected, selected.get(i),
                rowGroupOf.get(selected.get(i))));
          }
          // The next row group's requests go out now so its transfer overlaps this one's decode.
          if (i + 1 < selected.size() && !isStopped.get()
              && bytes(inFlight.peek()) + bytes(projected, selected.get(i + 1)) <= lookaheadBudget) {
            inFlight.add(startFetch(fileStream, buffers, allocator, projected, selected.get(i + 1),
                rowGroupOf.get(selected.get(i + 1))));
          }
          finishFetch(allocator, buffers, inFlight.poll());
        }
      } finally {
        for (Fetch fetch : inFlight) {
          abandon(allocator, fetch);
        }
      }
    }
  }

  /** Whether the projection reaches into a group type, which this reader does not decode. */
  public boolean projectsNestedTypes() {
    for (Type field : requestedSchema.getFields()) {
      if (!field.isPrimitive()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Whether the query needs row-lineage virtual columns ({@code ROW__LINEAGE__ID} /
   * {@code LAST__UPDATED__SEQUENCE__NUMBER}) that the fallback reader augments the requested schema
   * with via {@link RowLineageUtils#getRequestedSchemaWithRowLineageColumns}. The native pipeline
   * doesn't propagate that augmentation through {@code Includes} yet, so we defer to the fallback
   * reader instead of quietly emitting nulls in the lineage slots.
   */
  public boolean needsRowLineage() {
    VectorizedRowBatchCtx rbCtx = Utilities.getVectorizedRowBatchCtx(jobConf);
    if (rbCtx == null) {
      return false;
    }
    MessageType fileSchema = footer.getFileMetaData().getSchema();
    return RowLineageUtils.isRowLineageColumnPresent(rbCtx, fileSchema, VirtualColumn.ROW_LINEAGE_ID)
        || RowLineageUtils.isRowLineageColumnPresent(rbCtx, fileSchema,
            VirtualColumn.LAST_UPDATED_SEQUENCE_NUMBER);
  }

  /**
   * Leaf-column positions of the requested fields; {@code BlockMetaData.getColumns()} is a flat list
   * of leaves in file-schema order, so a top-level primitive at ordinal {@code k} in the file schema
   * may sit at a very different leaf index (e.g. a nested group of two leaves before it shifts it
   * from {@code 1} to {@code 2}). We only reach this method when every requested field is a
   * top-level primitive ({@link #projectsNestedTypes()} would have forced a fallback otherwise), so
   * each requested field maps to exactly one leaf whose path is a single segment.
   */
  static int[] projectedLeaves(MessageType requestedSchema, MessageType fileSchema) {
    List<ColumnDescriptor> allLeaves = fileSchema.getColumns();
    List<Integer> selected = new ArrayList<>();
    for (Type field : requestedSchema.getFields()) {
      if (!fileSchema.containsField(field.getName())) {
        continue;
      }
      for (int i = 0; i < allLeaves.size(); ++i) {
        String[] path = allLeaves.get(i).getPath();
        // Single-segment path == top-level primitive; skip leaves buried inside a group.
        if (path.length == 1 && path[0].equals(field.getName())) {
          selected.add(i);
          break;
        }
      }
    }
    return selected.stream().mapToInt(Integer::intValue).toArray();
  }

  private static long bytes(int[] projected, BlockMetaData block) {
    long total = 0;
    for (int leaf : projected) {
      total += block.getColumns().get(leaf).getTotalSize();
    }
    return total;
  }

  private static long bytes(Fetch fetch) {
    long total = 0;
    for (ColumnChunkMetaData chunk : fetch.batch.chunks()) {
      total += chunk.getTotalSize();
    }
    return total;
  }

  /** One row group on its way in: buffers planned, missing ranges requested, not yet decoded. */
  private static final class Fetch {
    private final ParquetEncodedColumnBatch batch = new ParquetEncodedColumnBatch();
    private final List<ColumnPlan> columns = new ArrayList<>();
    private final List<Part> misses = new ArrayList<>();
    private final List<Run> runs = new ArrayList<>();
  }

  /** The buffers covering one column chunk, in file order, and which stretches of them are new. */
  private record ColumnPlan(List<Part> parts, List<MissRun> missRuns) {
    ColumnPlan() {
      this(new ArrayList<>(), new ArrayList<>());
    }
  }

  /** One cache buffer's worth of a column chunk: a hit handed back by the cache, or a miss to fill. */
  private static final class Part {
    private MemoryBuffer buffer;
    private final DiskRange range;
    /** We hold one ref to release; until then a miss is a raw allocation to free. */
    private boolean owned;

    Part(MemoryBuffer buffer, DiskRange range, boolean owned) {
      this.buffer = buffer;
      this.range = range;
      this.owned = owned;
    }
  }

  /** {@code count} consecutive parts covering one contiguous missing sub-range; cached as a unit. */
  private record MissRun(int firstPart, int count) {
  }

  /** One vectored range and the cache buffers it fills, in file order. */
  private record Run(FileRange range, List<Part> parts) {
  }

  private Fetch startFetch(FSDataInputStream fileStream, ParquetRangeBuffers buffers, Allocator allocator,
      int[] projected, BlockMetaData block, int rg)
      throws IOException {
    Fetch fetch = new Fetch();
    ColumnChunkMetaData[] chunks = new ColumnChunkMetaData[projected.length];
    for (int pc = 0; pc < projected.length; ++pc) {
      chunks[pc] = block.getColumns().get(projected[pc]);
    }
    fetch.batch.init(fileKey, rg, chunks);
    try {
      for (ColumnChunkMetaData chunk : chunks) {
        ColumnPlan column = new ColumnPlan();
        fetch.columns.add(column);
        planColumnChunk(allocator, chunk.getStartingPos(),
            chunk.getStartingPos() + chunk.getTotalSize(), column, fetch.misses);
      }
      // The vectored dispatch is where non-async FS impls actually read; count it under HDFS_TIME.
      long hdfsStart = counters.startTimeCounter();
      requestMisses(fileStream, buffers, fetch, layout.maxRangeBytes());
      counters.recordHdfsTime(hdfsStart);
    } catch (Exception e) {
      abandon(allocator, fetch);
      throw e;
    }
    return fetch;
  }

  private void finishFetch(Allocator allocator, ParquetRangeBuffers buffers, Fetch fetch)
      throws IOException, InterruptedException {
    try {
      // Awaiting the vectored futures is where the miss bytes actually arrive from HDFS.
      long hdfsStart = counters.startTimeCounter();
      receiveMisses(buffers, fetch);
      counters.recordHdfsTime(hdfsStart);
      for (ColumnPlan column : fetch.columns) {
        putColumn(allocator, column);
      }
      for (int pc = 0; pc < fetch.columns.size(); ++pc) {
        assemble(fetch.batch, pc, fetch.columns.get(pc).parts);
      }
      // consumeData returns the batch on success; after a throw it is still ours.
      consumer.consumeData(fetch.batch);
    } catch (Exception e) {
      abandon(allocator, fetch);
      throw e;
    }
  }

  /** Drops a fetch that will not be decoded: outstanding requests are cancelled, buffers released. */
  private void abandon(Allocator allocator, Fetch fetch) {
    for (Run run : fetch.runs) {
      run.range.getData().cancel(true);
    }
    for (ColumnPlan column : fetch.columns) {
      for (Part part : column.parts) {
        if (part.owned) {
          bufferManager.decRefBuffer(part.buffer);
        } else {
          allocator.deallocate(part.buffer);
        }
      }
    }
  }

  /** Package-private so a test can observe the read pattern. */
  FSDataInputStream openFile(FileSystem fs) throws IOException {
    return fs.open(path);
  }

  /** Lets the allocator abandon a wait for memory once the fragment is cancelled. */
  private void allocateMultiple(Allocator allocator, MemoryBuffer[] dest, int size) {
    if (allocator instanceof StoppableAllocator stoppable) {
      stoppable.allocateMultiple(dest, size, DATA_BUFFER_FACTORY, isStopped);
    } else {
      allocator.allocateMultiple(dest, size, DATA_BUFFER_FACTORY);
    }
  }

  /**
   * Works out which buffers cover column chunk {@code [start, end)} without touching the file: cache
   * hits as returned by getFileData, and freshly allocated power-of-two buffers for everything
   * missing. Misses are appended to {@code allMisses} so the whole row group can be read at once.
   */
  private void planColumnChunk(Allocator allocator, long start, long end,
      ColumnPlan column, List<Part> allMisses) throws IOException {
    DiskRangeList head = new DiskRangeList(start, end);
    if (fileKey != null) {
      head = lowLevelCache.getFileData(fileKey, head, 0, ParquetCacheLayout.CACHE_CHUNK_FACTORY,
          counters, new BooleanRef());
    }
    DiskRangeList current = head;
    try {
      for (; current != null; current = current.next) {
        if (current.hasData()) {
          column.parts.add(new Part(((CacheChunk) current).getBuffer(), current, true));
        } else {
          planMissRun(allocator, current, column, allMisses);
        }
      }
    } catch (Exception e) {
      // Hits past the failure point are still locked by getFileData; the caller only knows about
      // the parts already recorded.
      for (current = current.next; current != null; current = current.next) {
        if (current.hasData()) {
          bufferManager.decRefBuffer(((CacheChunk) current).getBuffer());
        }
      }
      throw e;
    }
  }

  /**
   * Records one missing sub-range as a run of freshly allocated power-of-two buffers, and appends
   * each part to {@code allMisses} so the caller can request them all in one vectored call.
   */
  private void planMissRun(Allocator allocator, DiskRangeList missing, ColumnPlan column,
      List<Part> allMisses) throws IOException {
    LlapHiveUtils.throwIfCacheOnlyRead(cacheOnly);
    int[] sizes = layout.bufferSizes(missing.getEnd() - missing.getOffset());
    column.missRuns.add(new MissRun(column.parts.size(), sizes.length));
    long partFrom = missing.getOffset();
    for (int size : sizes) {
      MemoryBuffer[] one = new MemoryBuffer[1];
      allocateMultiple(allocator, one, size);
      // The cache accounts and serves the bytes up to the buffer's limit.
      ByteBuffer raw = one[0].getByteBufferRaw();
      raw.limit(raw.position() + size);
      Part part = new Part(one[0], new DiskRange(partFrom, partFrom + size), false);
      column.parts.add(part);
      allMisses.add(part);
      partFrom += size;
    }
  }

  /**
   * Requests every missing buffer of a row group in one vectored call, one range per run of
   * adjacent buffers. Column chunks sit back to back in the file, so a projection that keeps
   * neighbouring columns reads them together; runs are capped so the row group arrives as
   * several concurrent requests rather than one. A filesystem without a vectored implementation
   * reads the ranges in turn.
   */
  private static void requestMisses(FSDataInputStream fileStream, ParquetRangeBuffers buffers, Fetch fetch,
      int maxRange) throws IOException {
    List<Part> misses = fetch.misses;
    if (misses.isEmpty()) {
      return;
    }
    misses.sort(Comparator.comparingLong(part -> part.range.getOffset()));
    List<Run> runs = new ArrayList<>();
    int i = 0;
    while (i < misses.size()) {
      long from = misses.get(i).range.getOffset();
      long to = misses.get(i).range.getEnd();
      int j = i + 1;
      while (j < misses.size()) {
        DiskRange next = misses.get(j).range;
        if (next.getOffset() != to || next.getEnd() - from > maxRange) {
          break;
        }
        to = next.getEnd();
        ++j;
      }
      runs.add(new Run(FileRange.createFileRange(from, (int) (to - from)), misses.subList(i, j)));
      i = j;
    }
    List<FileRange> ranges = new ArrayList<>(runs.size());
    for (Run run : runs) {
      ranges.add(run.range);
    }
    fileStream.readVectored(ranges, buffers::allocate, buffers::release);
    // Only requests the stream accepted are the fetch's to wait for or cancel.
    fetch.runs.addAll(runs);
  }

  /** Waits for the requested runs and copies each into the cache buffers it covers. */
  private static void receiveMisses(ParquetRangeBuffers buffers, Fetch fetch) throws IOException {
    for (Run run : fetch.runs) {
      ByteBuffer data = FutureIO.awaitFuture(run.range.getData());
      for (Part part : run.parts) {
        int length = part.range.getLength();
        ByteBuffer src = data.duplicate();
        src.position(data.position() + (int) (part.range.getOffset() - run.range.getOffset()));
        src.limit(src.position() + length);
        part.buffer.getByteBufferRaw().duplicate().put(src);
      }
      buffers.release(data);
    }
  }

  /** Hands one column's freshly read buffers to the cache; every part then carries a ref we own. */
  private void putColumn(Allocator allocator, ColumnPlan column) {
    for (MissRun run : column.missRuns) {
      List<Part> parts = column.parts.subList(run.firstPart, run.firstPart + run.count);
      if (fileKey == null) {
        for (Part part : parts) {
          bufferManager.incRefBuffer(part.buffer);
          part.owned = true;
        }
        // No cache key => no putFileData; each part is already on the ref-counted side
        // (incRefBuffer + owned=true), so cleanup will decRef rather than deallocate. Skip the
        // cache-put loop.
        continue;
      }
      // One range at a time so a mid-run throw from putFileData (e.g. the length-mismatch guard
      // in LowLevelCacheImpl) leaves ownership clean: parts already handled are marked owned so
      // cleanup calls decRefBuffer on cache-owned memory, while the current and later ones stay
      // raw allocations that cleanup can safely deallocate.
      for (Part part : parts) {
        MemoryBuffer fresh = part.buffer;
        MemoryBuffer[] pair = new MemoryBuffer[] {fresh};
        DiskRange[] range = new DiskRange[] {part.range};
        lowLevelCache.putFileData(fileKey, range, pair, 0, Priority.NORMAL, counters, cacheTag);
        if (pair[0] != fresh) {
          // The cache kept its own buffer (locked for us) and unlocked ours without freeing it.
          allocator.deallocate(fresh);
          part.buffer = pair[0];
        }
        part.owned = true;
      }
    }
  }

  private static void assemble(ParquetEncodedColumnBatch batch, int pc, List<Part> parts) {
    int n = parts.size();
    MemoryBuffer[] columnBuffers = new MemoryBuffer[n];
    long[] bufferOffsets = new long[n];
    int[] bufferLengths = new int[n];
    batch.columnBuffers()[pc] = columnBuffers;
    batch.bufferOffsets()[pc] = bufferOffsets;
    batch.bufferLengths()[pc] = bufferLengths;
    for (int i = 0; i < n; ++i) {
      Part part = parts.get(i);
      columnBuffers[i] = part.buffer;
      bufferOffsets[i] = part.range.getOffset();
      bufferLengths[i] = part.range.getLength();
    }
  }

  @Override
  public void returnData(ParquetEncodedColumnBatch batch) {
    for (MemoryBuffer[] column : batch.columnBuffers()) {
      for (MemoryBuffer buffer : column) {
        bufferManager.decRefBuffer(buffer);
      }
    }
  }

  @Override
  public void pause() {
    // The reader has no pausable state: the IO thread runs one row group at a time and yields on
    // finishFetch() naturally. Kept to satisfy the ConsumerFeedback contract.
  }

  @Override
  public void unpause() {
    // See pause(): no state to resume.
  }

  @Override
  public void stop() {
    isStopped.set(true);
  }
}
