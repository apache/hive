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

package org.apache.hadoop.hive.llap;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.ql.io.orc.encoded.CacheChunk;
import org.apache.hadoop.util.Progressable;
import org.apache.orc.impl.RecordReaderUtils;

/**
 * A shim {@link FileSystem} that transparently interposes on reads so that byte ranges backed by
 * the LLAP {@link DataCache} are served from memory and only genuine misses go to the underlying
 * (real) file system.
 *
 * <h3>What it does</h3>
 * It exposes a virtual {@code llapcache://} URI in place of the real file path. Reads issued
 * against that URI are routed through {@link CacheAwareInputStream}, which consults the
 * {@link DataCache} for each requested chunk: hits are copied straight out of cache buffers,
 * misses are fetched from the wrapped {@link FileSystem}, handed back to the caller, and
 * simultaneously inserted into the cache so subsequent reads of the same chunk are served from
 * memory. All non-read {@code FileSystem} operations (open/append/create/delete/rename/
 * getFileStatus/listStatus/mkdirs) are simply delegated to the wrapped file system after
 * translating the virtual path back to the real one.
 *
 * <h3>How it works</h3>
 * The reader that wants caching calls {@link #registerFile(DataCache, Path, Object, TreeMap,
 * Configuration, CacheTag, ParquetCacheLayout)} with the real path, a stable {@code fileKey}, a chunk
 * index (a {@code TreeMap} of {@code startOffset -> endOffset} boundaries covering the ranges the
 * reader intends to read - typically the column-chunk ranges of the projected columns), and the
 * daemon's cache layout, the one the native reader caches with. The
 * shim assigns a unique {@code splitId}, stashes the per-split state in a static map, registers
 * itself as the handler for the {@code llapcache} scheme via {@code fs.llapcache.impl}, and
 * returns a virtual {@code llapcache://llapcache/&lt;splitId&gt;} path. The caller hands this
 * virtual path to the underlying format library (e.g. the Parquet reader); when the library
 * opens the path and issues positioned reads, this class receives them, splits them along the
 * pre-registered chunk boundaries, and either serves each chunk from cache or reads-through and
 * populates it. When the reader is finished it must call {@link #unregisterFile(Path)} to drop
 * the per-split state.
 *
 * <h3>When it is used</h3>
 * Today the only caller is the vectorized Parquet path in
 * {@code VectorizedParquetRecordReader.wrapPathForCache}, which engages the shim when LLAP IO
 * is enabled and a {@link DataCache} was injected into the reader via
 * {@code LlapCacheOnlyInputFormatInterface.injectCaches}. Concretely that means:
 * <ul>
 *   <li>LLAP daemon splits where the native Parquet cache pipeline
 *       ({@code ParquetColumnVectorProducer} / {@code ParquetEncodedDataReader}) declines the
 *       split and {@code LlapInputFormat} falls back to the source
 *       {@code VectorizedParquetInputFormat} - e.g. nested-type projections or setup errors.
 *   <li>Iceberg vectorized Parquet reads via {@code HiveVectorizedReader.parquetRecordReader},
 *       which always uses {@code VectorizedParquetInputFormat} directly and injects caches when
 *       LLAP is on - the native Parquet cache pipeline is not reachable from that entry point.
 * </ul>
 * Non-LLAP execution (no daemon, no {@code DataCache} injected) skips this shim entirely and
 * reads directly from the real file system. The chunk boundaries come from the caller and the
 * buffer layout from {@link ParquetCacheLayout}, which the native Parquet reader shares so each
 * finds the other's buffers; nothing else format-specific belongs here.
 */
public class LlapCacheAwareFs extends FileSystem {
  public static final String SCHEME = "llapcache";
  private static AtomicLong currentSplitId = new AtomicLong(-1);
  private URI uri;

  // We store the chunk indices by split file; that way if several callers are reading
  // the same file they can separately store and remove the relevant parts of the index.
  private static final ConcurrentHashMap<Long, CacheAwareInputStream> files =
      new ConcurrentHashMap<>();

  public static Path registerFile(DataCache cache, Path path, Object fileKey,
      TreeMap<Long, Long> index, Configuration conf, CacheTag tag, ParquetCacheLayout layout)
      throws IOException {
    long splitId = currentSplitId.incrementAndGet();
    CacheAwareInputStream stream = new CacheAwareInputStream(
        cache, conf, index, path, fileKey, -1, tag, layout);
    if (files.putIfAbsent(splitId, stream) != null) {
      throw new IOException("Record already exists for " + splitId);
    }
    conf.set("fs." + LlapCacheAwareFs.SCHEME + ".impl", LlapCacheAwareFs.class.getCanonicalName());
    return new Path(SCHEME + "://" + SCHEME + "/" + splitId);
  }

  public static void unregisterFile(Path cachePath) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Unregistering " + cachePath);
    }
    files.remove(extractSplitId(cachePath));
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    this.uri = URI.create(SCHEME + "://" + SCHEME);
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    return new FSDataInputStream(getCtx(path).cloneWithBufferSize(bufferSize));
  }

  private LlapCacheAwareFs.CacheAwareInputStream getCtx(Path path) {
    return files.get(extractSplitId(path));
  }

  private static long extractSplitId(Path path) {
    String pathOnly = path.toUri().getPath();
    if (pathOnly.startsWith("/")) {
      pathOnly = pathOnly.substring(1);
    }
    return Long.parseLong(pathOnly);
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public Path getWorkingDirectory() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setWorkingDirectory(Path arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public FSDataOutputStream append(Path arg0, int arg1, Progressable arg2) throws IOException {
    LlapCacheAwareFs.CacheAwareInputStream ctx = getCtx(arg0);
    return ctx.getFs().append(ctx.path, arg1, arg2);
  }

  @Override
  public FSDataOutputStream create(Path arg0, FsPermission arg1, boolean arg2, int arg3,
      short arg4, long arg5, Progressable arg6) throws IOException {
    LlapCacheAwareFs.CacheAwareInputStream ctx = getCtx(arg0);
    return ctx.getFs().create(ctx.path, arg1, arg2, arg3, arg4, arg5, arg6);
  }

  @Override
  public boolean delete(Path arg0, boolean arg1) throws IOException {
    LlapCacheAwareFs.CacheAwareInputStream ctx = getCtx(arg0);
    return ctx.getFs().delete(ctx.path, arg1);

  }

  @Override
  public FileStatus getFileStatus(Path arg0) throws IOException {
    LlapCacheAwareFs.CacheAwareInputStream ctx = getCtx(arg0);
    FileStatus fileStatus = ctx.getFs().getFileStatus(ctx.path);
    // We replace the path in the file status by the input path as Parquet
    // may use the path in the file status to open the file
    fileStatus.setPath(arg0);
    return fileStatus;
  }

  @Override
  public FileStatus[] listStatus(Path arg0) throws FileNotFoundException, IOException {
    LlapCacheAwareFs.CacheAwareInputStream ctx = getCtx(arg0);
    return ctx.getFs().listStatus(ctx.path);
  }

  @Override
  public boolean mkdirs(Path arg0, FsPermission arg1) throws IOException {
    LlapCacheAwareFs.CacheAwareInputStream ctx = getCtx(arg0);
    return ctx.getFs().mkdirs(ctx.path, arg1);
  }

  @Override
  public boolean rename(Path arg0, Path arg1) throws IOException {
    LlapCacheAwareFs.CacheAwareInputStream ctx1 = getCtx(arg0), ctx2 = getCtx(arg1);
    return ctx1.getFs().rename(ctx1.path, ctx2.path);
  }

  /** Package-private so a test can check which cache misses it accepts. */
  static class CacheAwareInputStream extends InputStream
      implements Seekable, PositionedReadable {
    private final TreeMap<Long, Long> chunkIndex;
    private final Path path;
    private final Object fileKey;
    private final CacheTag tag;
    private final Configuration conf;
    private final DataCache cache;
    private final int bufferSize;
    private final ParquetCacheLayout layout;
    private long position = 0;

    public CacheAwareInputStream(DataCache cache, Configuration conf,
        TreeMap<Long, Long> chunkIndex, Path path, Object fileKey, int bufferSize, CacheTag tag,
        ParquetCacheLayout layout) {
      this.cache = cache;
      this.fileKey = fileKey;
      this.chunkIndex = chunkIndex;
      this.path = path;
      this.conf = conf;
      this.bufferSize = bufferSize;
      this.tag = tag;
      // The daemon's layout, not one built from conf: conf is the job's, which need not carry the
      // daemon's hive.llap.io.encode.alloc.size, and both Parquet readers must cut a chunk into the
      // same cache pieces.
      this.layout = layout;
    }

    public LlapCacheAwareFs.CacheAwareInputStream cloneWithBufferSize(int bufferSize) {
      return new CacheAwareInputStream(cache, conf, chunkIndex, path, fileKey, bufferSize, tag, layout);
    }

    @Override
    public int read() throws IOException {
      // This is not called by ConsecutiveChunk stuff in Parquet.
      // If this were used, it might make sense to make it faster.
      byte[] theByte = new byte[1];
      int result = read(theByte, 0, 1);
      if (result != 1) throw new EOFException();
      return theByte[0] & 0xFF;
    }

    @Override
    public int read(byte[] array, final int arrayOffset, final int len) throws IOException {
      long readStartPos = position;
      DiskRangeList drl = new DiskRangeList(readStartPos, readStartPos + len);
      DataCache.BooleanRef gotAllData = new DataCache.BooleanRef();
      drl = cache.getFileData(
          fileKey, drl, 0, ParquetCacheLayout.CACHE_CHUNK_FACTORY, gotAllData);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Buffers after cache " + RecordReaderUtils.stringifyDiskRanges(drl));
      }
      if (gotAllData.value) {
        long sizeRead = 0;
        while (drl != null) {
          assert drl.hasData();
          long from = drl.getOffset(), to = drl.getEnd();
          int offsetFromReadStart = (int)(from - readStartPos), candidateSize = (int)(to - from);
          ByteBuffer data = drl.getData().duplicate();
          data.get(array, arrayOffset + offsetFromReadStart, candidateSize);
          cache.releaseBuffer(((CacheChunk)drl).getBuffer());
          sizeRead += candidateSize;
          drl = drl.next;
        }
        validateAndUpdatePosition(len, sizeRead);
        return len;
      }
      // We have some disk data. Separate it by column chunk and put into cache.

      // We started with a single DRL, so we assume there will be no consecutive missing blocks
      // after the cache has inserted cache data. We also assume all the missing parts will
      // represent one or several column chunks, since we always cache on column chunk boundaries.
      DiskRangeList current = drl;
      FileSystem fs = path.getFileSystem(conf);
      try (FSDataInputStream is = fs.open(path, bufferSize)) {
        Allocator allocator = cache.getAllocator();
        long sizeRead = 0;
        while (current != null) {
          DiskRangeList candidate = current;
          current = current.next;
          long from = candidate.getOffset(), to = candidate.getEnd();
          // The offset in the destination array for the beginning of this missing range.
          int offsetFromReadStart = (int)(from - readStartPos), candidateSize = (int)(to - from);
          if (candidate.hasData()) {
            ByteBuffer data = candidate.getData().duplicate();
            data.get(array, arrayOffset + offsetFromReadStart, candidateSize);
            cache.releaseBuffer(((CacheChunk)candidate).getBuffer());
            sizeRead += candidateSize;
            continue;
          }
          // The data is not in cache.

          // Account for potential partial chunks.
          SortedMap<Long, Long> chunksInThisRead = getAndValidateMissingChunks(from, to);

          is.seek(from);
          is.readFully(array, arrayOffset + offsetFromReadStart, candidateSize);
          sizeRead += candidateSize;
          // Now copy missing chunks (and parts of chunks) into cache buffers.
          if (fileKey == null || cache == null) continue;
          int extraDiskDataOffset = 0;
          // TODO: should we try to make a giant array for one cache call to avoid overhead?
          for (Map.Entry<Long, Long> missingChunk : chunksInThisRead.entrySet()) {
            long chunkFrom = Math.max(from, missingChunk.getKey()),
                chunkTo = Math.min(to, missingChunk.getValue()),
                chunkLength = chunkTo - chunkFrom;
            // TODO: if we allow partial reads (right now we disable this), we'd have to handle it here.
            //       chunksInThisRead should probably be changed to be a struct array indicating both
            //       partial and full sizes for each chunk; then the partial ones could be merged
            //       with the previous partial ones, and any newly-full chunks put in the cache.

            // The same decomposition the native Parquet reader caches with, so a chunk cached by
            // either reader is found whole by the other, and a gap left by evicting part of one is
            // re-cached under the keys the first writer used.
            int[] sizes = layout.bufferSizes(chunkLength);
            MemoryBuffer[] parts = new MemoryBuffer[sizes.length];
            // One putFileData per part rather than one for the chunk: a mid-run throw from the
            // cache would otherwise leave the parts before it locked and the parts after it raw
            // allocations, with nothing in the cleanup below able to tell the two apart.
            int cachedCount = 0;
            try {
              long partFrom = chunkFrom;
              for (int i = 0; i < sizes.length; ++i) {
                MemoryBuffer[] one = new MemoryBuffer[1];
                // Note: we don't use StoppableAllocator here - this is not on an IO thread.
                allocator.allocateMultiple(one, sizes[i], cache.getDataBufferFactory());
                MemoryBuffer fresh = one[0];
                parts[i] = fresh;
                // The allocator rounds the request up to its own power of two, so the copy fills -
                // and the cache is told about - the bytes we asked for, not the buffer's capacity.
                ByteBuffer bb = fresh.getByteBufferRaw();
                bb.limit(bb.position() + sizes[i]);
                DiskRange[] range = {copyDiskDataToCacheBuffer(array,
                    arrayOffset + offsetFromReadStart + extraDiskDataOffset, sizes[i], bb, partFrom) };
                extraDiskDataOffset += sizes[i];
                partFrom += sizes[i];
                cache.putFileData(fileKey, range, one, 0, tag);
                if (one[0] != fresh) {
                  // The cache kept its own buffer (locked for us) and unlocked ours without freeing it.
                  allocator.deallocate(fresh);
                  parts[i] = one[0];
                }
                ++cachedCount;
              }
            } finally {
              // We do not use the new cache buffers for the actual read, given the way read() API is.
              // Therefore, we don't need to handle cache collisions - just decref every buffer the
              // cache took, and free the one allocation it never got to see.
              for (int i = 0; i < parts.length; ++i) {
                if (parts[i] == null) {
                  continue;
                } else if (i < cachedCount) {
                  cache.releaseBuffer(parts[i]);
                } else {
                  allocator.deallocate(parts[i]);
                }
              }
            }
          }
        }
        validateAndUpdatePosition(len, sizeRead);
      }
      return len;
    }

    private void validateAndUpdatePosition(int len, long sizeRead) {
      if (sizeRead != len) {
        throw new AssertionError("Reading at " + position + " for " + len + ": "
            + sizeRead + " bytes copied");
      }
      position += len;
    }

    /** Copies one part's bytes into its cache buffer and returns the range it now holds. */
    private DiskRange copyDiskDataToCacheBuffer(byte[] diskData, int offsetInDiskData,
        int sizeToCopy, ByteBuffer cacheBuffer, long cacheRangeStart) {
      int bbPos = cacheBuffer.position();
      long cacheRangeEnd = cacheRangeStart + sizeToCopy;
      if (LOG.isTraceEnabled()) {
        LOG.trace("Caching [" + cacheRangeStart + ", " + cacheRangeEnd + ")");
      }
      cacheBuffer.put(diskData, offsetInDiskData, sizeToCopy);
      cacheBuffer.position(bbPos);
      return new DiskRange(cacheRangeStart, cacheRangeEnd);
    }

    /**
     * The registered chunks a cache miss covering {@code [from, to)} falls into.
     *
     * <p>This checks only that the miss lies within chunks the caller registered, which is an
     * invariant of this class: the chunk index and the offsets the format library reads come from
     * the same footer. It deliberately says nothing about where the miss starts or ends inside a
     * chunk - the cache may have evicted any subset of a chunk's buffers, and whatever gap that
     * leaves is re-cached on {@link ParquetCacheLayout}'s boundaries by the caller.
     */
    SortedMap<Long, Long> getAndValidateMissingChunks(long from, long to) {
      Map.Entry<Long, Long> firstMissing = chunkIndex.floorEntry(from);
      if (firstMissing == null) {
        throw new AssertionError("No lower bound for start offset " + from);
      }
      if (firstMissing.getValue() <= from) {
        // The data does not belong to a recognized chunk.
        throw new AssertionError("Lower bound for start offset " + from + " is ["
            + firstMissing.getKey() + ", " + firstMissing.getValue() + ")");
      }
      SortedMap<Long, Long> missingChunks = chunkIndex.subMap(firstMissing.getKey(), to);
      if (missingChunks.isEmpty()) {
        throw new AssertionError("No chunks for [" + from + ", " + to + ")");
      }
      long lastMissingOffset = missingChunks.lastKey(),
          lastMissingEnd = missingChunks.get(lastMissingOffset);
      if (lastMissingEnd < to) {
        // The data runs past the last chunk that was registered for it.
        throw new AssertionError("Lower bound for end offset " + to + " is ["
            + lastMissingOffset + ", " + lastMissingEnd + ")");
      }
      return missingChunks;
    }

    public FileSystem getFs() throws IOException {
      return path.getFileSystem(conf);
    }

    @Override
    public long getPos() throws IOException {
      return position;
    }

    @Override
    public void seek(long position) throws IOException {
      this.position = position;
    }

    @Override
    @Private
    public boolean seekToNewSource(long arg0) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int read(long arg0, byte[] arg1, int arg2, int arg3) throws IOException {
      seek(arg0);
      return read(arg1, arg2, arg3);
    }

    @Override
    public void readFully(long arg0, byte[] arg1) throws IOException {
      read(arg0, arg1, 0, arg1.length);
    }

    @Override
    public void readFully(long arg0, byte[] arg1, int arg2, int arg3) throws IOException {
      read(arg0, arg1, 0, arg1.length);
    }
  }
}
