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

package org.apache.hadoop.hive.llap.io.encoded;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.llap.Consumer;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.cache.Cache;
import org.apache.hadoop.hive.llap.io.api.EncodedColumn;
import org.apache.hadoop.hive.llap.io.api.EncodedColumn.ColumnBuffer;
import org.apache.hadoop.hive.llap.io.api.cache.LowLevelCache;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.io.api.orc.OrcBatchKey;
import org.apache.hadoop.hive.llap.io.api.orc.OrcCacheKey;
import org.apache.hadoop.hive.llap.io.metadata.OrcMetadataCache;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Type;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.io.orc.StripeInformation;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

public class OrcEncodedDataProducer implements EncodedDataProducer<OrcBatchKey> {
  private FileSystem cachedFs = null;
  private Configuration conf;
  private OrcMetadataCache metadataCache;
  // TODO: it makes zero sense to have both at the same time and duplicate data. Add "cache mode".
  private final Cache<OrcCacheKey> cache;
  private final LowLevelCache lowLevelCache;

  private class OrcEncodedDataReader implements EncodedDataReader<OrcBatchKey>,
    Consumer<EncodedColumn<OrcBatchKey>> {
    private final FileSplit split;
    private List<Integer> columnIds;
    private final SearchArgument sarg;
    private final Consumer<EncodedColumn<OrcBatchKey>> consumer;


    // Read state.
    private int stripeIxFrom, stripeIxTo;
    private Reader orcReader;
    private final String internedFilePath;
    /**
     * readState[stripeIx'][colIx'] - bitmask (as long array) of rg-s that are done.
     * Bitmasks are all well-known size so we don't bother with BitSets and such.
     * Each long has natural bit indexes used, so rightmost bits are filled first.
     */
    private long[][][] readState;
    private int[] rgsPerStripe = null;
    private boolean isStopped = false, isPaused = false;

    public OrcEncodedDataReader(InputSplit split, List<Integer> columnIds,
        SearchArgument sarg, Consumer<EncodedColumn<OrcBatchKey>> consumer) {
      this.split = (FileSplit)split;
      this.internedFilePath = this.split.getPath().toString().intern();
      this.columnIds = columnIds;
      if (this.columnIds != null) {
        Collections.sort(this.columnIds);
      }
      this.sarg = sarg;
      this.consumer = consumer;
    }

    @Override
    public void stop() {
      isStopped = true;
      // TODO: stop fetching if still in progress
    }

    @Override
    public void pause() {
      isPaused = true;
      // TODO: pause fetching
    }

    @Override
    public void unpause() {
      isPaused = false;
      // TODO: unpause fetching
    }

    @Override
    public Void call() throws IOException {
      LlapIoImpl.LOG.info("Processing split for " + internedFilePath);
      if (isStopped) return null;
      List<StripeInformation> stripes = metadataCache.getStripes(internedFilePath);
      List<Type> types = metadataCache.getTypes(internedFilePath);
      orcReader = null;
      if (stripes == null || types == null) {
        orcReader = createOrcReader(split);
        stripes = metadataCache.getStripes(internedFilePath);
        types = metadataCache.getTypes(internedFilePath);
      }

      if (columnIds == null) {
        columnIds = new ArrayList<Integer>(types.size());
        for (int i = 1; i < types.size(); ++i) {
          columnIds.add(i);
        }
      }
      determineWhatToRead(stripes);
      if (isStopped) return null;
      List<Integer>[] stripeColsToRead = produceDataFromCache();
      // readState now contains some 1s for column x rgs that were fetched from cache.
      // TODO: I/O threadpool would be here (or below); for now, linear
      for (int stripeIxMod = 0; stripeIxMod < readState.length; ++stripeIxMod) {
        List<Integer> colsToRead = stripeColsToRead == null ? null : stripeColsToRead[stripeIxMod];
        long[][] colRgs = readState[stripeIxMod];
        if (colsToRead == null) {
          colsToRead = columnIds;
        }
        if (colsToRead.isEmpty()) continue; // All the data for this stripe was in cache.
        if (colsToRead.size() != colRgs.length) {
          // We are reading subset of the original columns, remove unnecessary bitmasks.
          long[][] colRgs2 = new long[colsToRead.size()][];
          for (int i = 0, i2 = -1; i < colRgs.length; ++i) {
            if (colRgs[i] == null) continue;
            colRgs2[++i2] = colRgs[i];
          }
          colRgs = colRgs2;
        }
        int stripeIx = stripeIxFrom + stripeIxMod;
        StripeInformation si = stripes.get(stripeIx);
        int rgCount = rgsPerStripe[stripeIxMod];
        boolean[] includes = OrcInputFormat.genIncludedColumns(types, colsToRead, true);
        if (orcReader == null) {
          orcReader = createOrcReader(split);
        }
        RecordReader stripeReader = orcReader.rows(si.getOffset(), si.getLength(), includes);
        // In case if we have high-level cache, we will intercept the data and add it there;
        // otherwise just pass the data directly to the consumer.
        Consumer<EncodedColumn<OrcBatchKey>> consumer = (cache == null) ? this.consumer : this;
        stripeReader.readEncodedColumns(colRgs, rgCount, consumer, lowLevelCache);
        stripeReader.close();
      }

      consumer.setDone();
      if (DebugUtils.isTraceMttEnabled()) {
        LlapIoImpl.LOG.info("done processing " + split);
      }
      return null;
    }

    @Override
    public void returnData(ColumnBuffer data) {
      lowLevelCache.releaseBuffers(data.cacheBuffers);
    }

    private void determineWhatToRead(List<StripeInformation> stripes) {
      // The unit of caching for ORC is (stripe x column) (see OrcBatchKey).
      long offset = split.getStart(), maxOffset = offset + split.getLength();
      stripeIxFrom = stripeIxTo = -1;
      int stripeIx = 0;
      if (LlapIoImpl.LOG.isDebugEnabled()) {
        String tmp = "FileSplit {" + split.getStart() + ", " + split.getLength() + "}; stripes ";
        for (StripeInformation stripe : stripes) {
          tmp += "{" + stripe.getOffset() + ", " + stripe.getLength() + "}, ";
        }
        LlapIoImpl.LOG.debug(tmp);
      }

      List<Integer> stripeRgCounts = new ArrayList<Integer>(stripes.size());
      for (StripeInformation stripe : stripes) {
        long stripeStart = stripe.getOffset();
        if (offset > stripeStart) continue;
        if (stripeIxFrom == -1) {
          if (DebugUtils.isTraceEnabled()) {
            LlapIoImpl.LOG.info("Including from " + stripeIx
                + " (" + stripeStart + " >= " + offset + ")");
          }
          stripeIxFrom = stripeIx;
        }
        if (stripeStart >= maxOffset) {
          if (DebugUtils.isTraceEnabled()) {
            LlapIoImpl.LOG.info("Including until " + stripeIxTo
                + " (" + stripeStart + " >= " + maxOffset + ")");
          }
          stripeIxTo = stripeIx;
          break;
        }
        int rgCount = (int)Math.ceil(
            (double)stripe.getNumberOfRows() / orcReader.getRowIndexStride());
        stripeRgCounts.add(rgCount);
        ++stripeIx;
      }
      if (stripeIxTo == -1) {
        if (DebugUtils.isTraceEnabled()) {
          LlapIoImpl.LOG.info("Including until " + stripeIx + " (end of file)");
        }
        stripeIxTo = stripeIx;
      }
      readState = new long[stripeRgCounts.size()][][];
      for (int i = 0; i < stripeRgCounts.size(); ++i) {
        int bitmaskSize = align64(stripeRgCounts.get(i)) >>> 6;
        readState[i] = new long[columnIds.size()][];
        for (int j = 0; j < columnIds.size(); ++j) {
          readState[i][j] = new long[bitmaskSize];
        }
      }
      // TODO: HERE, we need to apply sargs and mark RGs that are filtered as 1s
      rgsPerStripe = new int[stripeRgCounts.size()];
      for (int i = 0; i < rgsPerStripe.length; ++i) {
         rgsPerStripe[i] = stripeRgCounts.get(i);
      }
    }

    // TODO: split by stripe? we do everything by stripe, and it might be faster
    private List<Integer>[] produceDataFromCache() {
      if (cache == null) return null;
      OrcCacheKey key = new OrcCacheKey(internedFilePath, -1, -1, -1);
      @SuppressWarnings("unchecked") // No generics arrays - "J" in "Java" stands for "joke".
      List<Integer>[] stripeColsNotInCache = new List[readState.length];
      for (int stripeIxMod = 0; stripeIxMod < readState.length; ++stripeIxMod) {
        key.stripeIx = stripeIxFrom + stripeIxMod;
        long[][] cols = readState[stripeIxMod];
        int rgCount = rgsPerStripe[stripeIxMod];
        for (int colIxMod = 0; colIxMod < cols.length; ++colIxMod) {
          key.colIx = columnIds.get(colIxMod);
          long[] doneMask = cols[colIxMod];
          boolean areAllRgsInCache = true;
          for (int rgIx = 0; rgIx < rgCount; ++rgIx) {
            int maskIndex = rgIx >>> 6, maskBit = 1 << (rgIx & 63);
            if ((doneMask[maskIndex] & maskBit) != 0) continue; // RG eliminated by SARG
            key.rgIx = rgIx;
            ColumnBuffer cached = cache.get(key);
            if (cached == null) {
              areAllRgsInCache = false;
              continue;
            }
            // TODO: pool of EncodedColumn-s objects. Someone will need to return them though.
            EncodedColumn<OrcBatchKey> col = new EncodedColumn<OrcBatchKey>(
                key.copyToPureBatchKey(), key.colIx, cached);
            consumer.consumeData(col);
            doneMask[maskIndex] = doneMask[maskIndex] | maskBit;
          }
          boolean hasFetchList = stripeColsNotInCache[stripeIxMod] != null;
          if (areAllRgsInCache) {
            cols[colIxMod] = null; // No need for bitmask, all rgs are done.
            if (!hasFetchList) {
              // All rgs for this stripe x column were fetched from cache. If this is the first
              // such column, create custom, smaller list of columns to fetch later for this
              // stripe (default is all the columns originally requested). Add all previous
              // columns, need to fetch them since this is the first column.
              stripeColsNotInCache[stripeIxMod] = new ArrayList<Integer>(cols.length);
              if (stripeIxMod > 0) {
                stripeColsNotInCache[stripeIxMod].addAll(columnIds.subList(0, colIxMod));
              }
            }
          } else if (hasFetchList) {
            // Only a subset of original columnIds need to be fetched for this stripe;
            // add the current one to this sublist.
            stripeColsNotInCache[stripeIxMod].add(columnIds.get(colIxMod));
          }
        }
      }
      return stripeColsNotInCache;
    }

    @Override
    public void setDone() {
      consumer.setDone();
    }

    @Override
    public void consumeData(EncodedColumn<OrcBatchKey> data) {
      // Store object in cache; create new key object - cannot be reused.
      assert cache != null;
      OrcCacheKey key = new OrcCacheKey(data.batchKey, data.columnIndex);
      ColumnBuffer cached = cache.cacheOrGet(key, data.columnData);
      if (data.columnData != cached) {
        lowLevelCache.releaseBuffers(data.columnData.cacheBuffers);
        data.columnData = cached;
      }
      consumer.consumeData(data);
    }

    @Override
    public void setError(Throwable t) {
      consumer.setError(t);
    }
  }

  private Reader createOrcReader(FileSplit fileSplit) throws IOException {
    FileSystem fs = cachedFs;
    Path path = fileSplit.getPath();
    if ("pfile".equals(path.toUri().getScheme())) {
      fs = path.getFileSystem(conf); // Cannot use cached FS due to hive tests' proxy FS.
    }
    if (metadataCache == null) {
      metadataCache = new OrcMetadataCache(cachedFs, path, conf);
    }
    return OrcFile.createReader(path, OrcFile.readerOptions(conf).filesystem(fs));
  }

  private static int align64(int number) {
    return ((number + 63) & ~63);
  }

  public OrcEncodedDataProducer(LowLevelCache lowLevelCache, Cache<OrcCacheKey> cache,
      Configuration conf) throws IOException {
    // We assume all splits will come from the same FS.
    this.cachedFs = FileSystem.get(conf);
    this.cache = cache;
    this.lowLevelCache = lowLevelCache;
    this.conf = conf;
    this.metadataCache = null;
  }

  @Override
  public EncodedDataReader<OrcBatchKey> getReader(InputSplit split, List<Integer> columnIds,
      SearchArgument sarg, Consumer<EncodedColumn<OrcBatchKey>> consumer) {
    return new OrcEncodedDataReader(split, columnIds, sarg, consumer);
  }
}
