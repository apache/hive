/*
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
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.ql.io.orc.encoded.CacheChunk;
import org.apache.hadoop.util.Progressable;
import org.apache.orc.impl.RecordReaderUtils;

/**
 * This is currently only used by Parquet; however, a generally applicable approach is used -
 * you pass in a set of offset pairs for a file, and the file is cached with these boundaries.
 * Don't add anything format specific here.
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
      TreeMap<Long, Long> index, Configuration conf, String tag) throws IOException {
    long splitId = currentSplitId.incrementAndGet();
    CacheAwareInputStream stream = new CacheAwareInputStream(
        cache, conf, index, path, fileKey, -1, tag);
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

  private static class CacheAwareInputStream extends InputStream
      implements Seekable, PositionedReadable {
    private final TreeMap<Long, Long> chunkIndex;
    private final Path path;
    private final Object fileKey;
    private final String tag;
    private final Configuration conf;
    private final DataCache cache;
    private final int bufferSize;
    private long position = 0;

    public CacheAwareInputStream(DataCache cache, Configuration conf,
        TreeMap<Long, Long> chunkIndex, Path path, Object fileKey, int bufferSize, String tag) {
      this.cache = cache;
      this.fileKey = fileKey;
      this.chunkIndex = chunkIndex;
      this.path = path;
      this.conf = conf;
      this.bufferSize = bufferSize;
      this.tag = tag;
    }

    public LlapCacheAwareFs.CacheAwareInputStream cloneWithBufferSize(int bufferSize) {
      return new CacheAwareInputStream(cache, conf, chunkIndex, path, fileKey, bufferSize, tag);
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
      drl = cache.getFileData(fileKey, drl, 0, new DataCache.DiskRangeListFactory() {
        @Override
        public DiskRangeList createCacheChunk(
            MemoryBuffer buffer, long startOffset, long endOffset) {
          return new CacheChunk(buffer, startOffset, endOffset);
        }
      }, gotAllData);
      if (LOG.isInfoEnabled()) {
        LOG.info("Buffers after cache " + RecordReaderUtils.stringifyDiskRanges(drl));
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
      int maxAlloc = cache.getAllocator().getMaxAllocation();
      // We have some disk data. Separate it by column chunk and put into cache.

      // We started with a single DRL, so we assume there will be no consecutive missing blocks
      // after the cache has inserted cache data. We also assume all the missing parts will
      // represent one or several column chunks, since we always cache on column chunk boundaries.
      DiskRangeList current = drl;
      FileSystem fs = path.getFileSystem(conf);
      FSDataInputStream is = fs.open(path, bufferSize);
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
        SortedMap<Long, Long> chunksInThisRead = getAndValidateMissingChunks(maxAlloc, from, to);

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
          MemoryBuffer[] largeBuffers = null, smallBuffer = null, newCacheData = null;
          try {
            int largeBufCount = (int) (chunkLength / maxAlloc);
            int smallSize = (int) (chunkLength % maxAlloc);
            int chunkPartCount = largeBufCount + ((smallSize > 0) ? 1 : 0);
            DiskRange[] cacheRanges = new DiskRange[chunkPartCount];
            int extraOffsetInChunk = 0;
            if (maxAlloc < chunkLength) {
              largeBuffers = new MemoryBuffer[largeBufCount];
              allocator.allocateMultiple(largeBuffers, maxAlloc, cache.getDataBufferFactory());
              for (int i = 0; i < largeBuffers.length; ++i) {
                // By definition here we copy up to the limit of the buffer.
                ByteBuffer bb = largeBuffers[i].getByteBufferRaw();
                int remaining = bb.remaining();
                assert remaining == maxAlloc;
                copyDiskDataToCacheBuffer(array,
                    arrayOffset + offsetFromReadStart + extraDiskDataOffset,
                    remaining, bb, cacheRanges, i, chunkFrom + extraOffsetInChunk);
                extraDiskDataOffset += remaining;
                extraOffsetInChunk += remaining;
              }
            }
            newCacheData = largeBuffers;
            largeBuffers = null;
            if (smallSize > 0) {
              smallBuffer = new MemoryBuffer[1];
              allocator.allocateMultiple(smallBuffer, smallSize, cache.getDataBufferFactory());
              ByteBuffer bb = smallBuffer[0].getByteBufferRaw();
              copyDiskDataToCacheBuffer(array,
                  arrayOffset + offsetFromReadStart + extraDiskDataOffset,
                  smallSize, bb, cacheRanges, largeBufCount, chunkFrom + extraOffsetInChunk);
              extraDiskDataOffset += smallSize;
              extraOffsetInChunk += smallSize; // Not strictly necessary, noone will look at it.
              if (newCacheData == null) {
                newCacheData = smallBuffer;
              } else {
                // TODO: add allocate overload with an offset and length
                MemoryBuffer[] combinedCacheData = new MemoryBuffer[largeBufCount + 1];
                System.arraycopy(newCacheData, 0, combinedCacheData, 0, largeBufCount);
                newCacheData = combinedCacheData;
                newCacheData[largeBufCount] = smallBuffer[0];
              }
              smallBuffer = null;
            }
            cache.putFileData(fileKey, cacheRanges, newCacheData, 0, tag);
          } finally {
            // We do not use the new cache buffers for the actual read, given the way read() API is.
            // Therefore, we don't need to handle cache collisions - just decref all the buffers.
            if (newCacheData != null) {
              for (MemoryBuffer buffer : newCacheData) {
                if (buffer == null) continue;
                cache.releaseBuffer(buffer);
              }
            }
            // If we have failed before building newCacheData, deallocate other the allocated.
            if (largeBuffers != null) {
              for (MemoryBuffer buffer : largeBuffers) {
                if (buffer == null) continue;
                allocator.deallocate(buffer);
              }
            }
            if (smallBuffer != null && smallBuffer[0] != null) {
              allocator.deallocate(smallBuffer[0]);
            }
          }
        }
      }
      validateAndUpdatePosition(len, sizeRead);
      return len;
    }

    private void validateAndUpdatePosition(int len, long sizeRead) {
      if (sizeRead != len) {
        throw new AssertionError("Reading at " + position + " for " + len + ": "
            + sizeRead + " bytes copied");
      }
      position += len;
    }

    private void copyDiskDataToCacheBuffer(byte[] diskData, int offsetInDiskData, int sizeToCopy,
        ByteBuffer cacheBuffer, DiskRange[] cacheRanges, int cacheRangeIx, long cacheRangeStart) {
      int bbPos = cacheBuffer.position();
      long cacheRangeEnd = cacheRangeStart + sizeToCopy;
      if (LOG.isTraceEnabled()) {
        LOG.trace("Caching [" + cacheRangeStart + ", " + cacheRangeEnd + ")");
      }
      cacheRanges[cacheRangeIx] = new DiskRange(cacheRangeStart, cacheRangeEnd);
      cacheBuffer.put(diskData, offsetInDiskData, sizeToCopy);
      cacheBuffer.position(bbPos);
    }

    private SortedMap<Long, Long> getAndValidateMissingChunks(
        int maxAlloc, long from, long to) {
      Map.Entry<Long, Long> firstMissing = chunkIndex.floorEntry(from);
      if (firstMissing == null) {
        throw new AssertionError("No lower bound for start offset " + from);
      }
      if (firstMissing.getValue() <= from
          || ((from - firstMissing.getKey()) % maxAlloc) != 0) {
        // The data does not belong to a recognized chunk, or is split wrong.
        throw new AssertionError("Lower bound for start offset " + from + " is ["
            + firstMissing.getKey() + ", " + firstMissing.getValue() + ")");
      }
      SortedMap<Long, Long> missingChunks = chunkIndex.subMap(firstMissing.getKey(), to);
      if (missingChunks.isEmpty()) {
        throw new AssertionError("No chunks for [" + from + ", " + to + ")");
      }
      long lastMissingOffset = missingChunks.lastKey(),
          lastMissingEnd = missingChunks.get(lastMissingOffset);
      if (lastMissingEnd < to
          || (to != lastMissingEnd && ((to - lastMissingOffset) % maxAlloc) != 0)) {
        // The data does not belong to a recognized chunk, or is split wrong.
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
