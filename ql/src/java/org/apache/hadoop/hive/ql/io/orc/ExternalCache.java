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

package org.apache.hadoop.hive.ql.io.orc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.MetadataPpdResult;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.HdfsUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat.FooterCache;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.shims.HadoopShims.HdfsFileStatusWithId;
import org.apache.orc.impl.OrcTail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;

/** Metastore-based footer cache storing serialized footers. Also has a local cache. */
public class ExternalCache implements FooterCache {
  private static final Logger LOG = LoggerFactory.getLogger(ExternalCache.class);

  private final LocalCache localCache;
  private final ExternalFooterCachesByConf externalCacheSrc;
  private boolean isWarnLogged = false;

  // Configuration and things set from it.
  private HiveConf conf;
  private boolean isInTest;
  private SearchArgument sarg;
  private ByteBuffer sargIsOriginal, sargNotIsOriginal;
  private boolean isPpdEnabled;

  public ExternalCache(LocalCache lc, ExternalFooterCachesByConf efcf) {
    localCache = lc;
    externalCacheSrc = efcf;
  }

  @Override
  public void put(OrcInputFormat.FooterCacheKey key, OrcTail orcTail) throws IOException {
    localCache.put(key.getPath(), orcTail);
    if (key.getFileId() != null) {
      try {
        externalCacheSrc.getCache(conf).putFileMetadata(Lists.newArrayList(key.getFileId()),
            Lists.newArrayList(orcTail.getSerializedTail()));
      } catch (HiveException e) {
        throw new IOException(e);
      }
    }
  }

  @Override
  public boolean isBlocking() {
    return true;
  }

  @Override
  public boolean hasPpd() {
    return isPpdEnabled;
  }

  public void configure(HiveConf queryConfig) {
    this.conf = queryConfig;
    this.sarg = ConvertAstToSearchArg.createFromConf(conf);
    this.isPpdEnabled = HiveConf.getBoolVar(conf, ConfVars.HIVEOPTINDEXFILTER)
        && HiveConf.getBoolVar(conf, ConfVars.HIVE_ORC_MS_FOOTER_CACHE_PPD);
    this.isInTest = HiveConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST);
    this.sargIsOriginal = this.sargNotIsOriginal = null;
  }

  @Override
  public void getAndValidate(List<HdfsFileStatusWithId> files, boolean isOriginal,
      OrcTail[] result, ByteBuffer[] ppdResult) throws IOException, HiveException {
    assert result.length == files.size();
    assert ppdResult == null || ppdResult.length == files.size();
    // First, check the local cache.
    localCache.getAndValidate(files, isOriginal, result, ppdResult);

    // posMap is an unfortunate consequence of batching/iterating thru MS results.
    HashMap<Long, Integer> posMap = new HashMap<Long, Integer>();
    // We won't do metastore-side PPD for the things we have locally.
    List<Long> fileIds = determineFileIdsToQuery(files, result, posMap);
     // Need to get a new one, see the comment wrt threadlocals.
    ExternalFooterCachesByConf.Cache cache = externalCacheSrc.getCache(conf);
    ByteBuffer serializedSarg = null;
    if (isPpdEnabled) {
      serializedSarg = getSerializedSargForMetastore(isOriginal);
    }
    if (serializedSarg != null) {
      Iterator<Entry<Long, MetadataPpdResult>> iter = cache.getFileMetadataByExpr(
          fileIds, serializedSarg, false); // don't fetch the footer, PPD happens in MS.
      while (iter.hasNext()) {
        Entry<Long, MetadataPpdResult> e = iter.next();
        int ix = getAndVerifyIndex(posMap, files, result, e.getKey());
        processPpdResult(e.getValue(), files.get(ix), ix, result, ppdResult);
      }
    } else {
      // Only populate corrupt IDs for the things we couldn't deserialize if we are not using
      // ppd. We assume that PPD makes sure the cached values are correct (or fails otherwise);
      // also, we don't use the footers in PPD case.
      List<Long> corruptIds = null;
      Iterator<Entry<Long, ByteBuffer>> iter = cache.getFileMetadata(fileIds);
      while (iter.hasNext()) {
        Entry<Long, ByteBuffer> e = iter.next();
        int ix = getAndVerifyIndex(posMap, files, result, e.getKey());
        if (!processBbResult(e.getValue(), ix, files.get(ix), result))  {
          if (corruptIds == null) {
            corruptIds = new ArrayList<>();
          }
          corruptIds.add(e.getKey());
        }
      }
      if (corruptIds != null) {
        cache.clearFileMetadata(corruptIds);
      }
    }
  }

  private int getAndVerifyIndex(HashMap<Long, Integer> posMap,
      List<HdfsFileStatusWithId> files, OrcTail[] result, Long fileId) {
    int ix = posMap.get(fileId);
    assert result[ix] == null;
    assert fileId != null && fileId.equals(files.get(ix).getFileId());
    return ix;
  }

  private boolean processBbResult(
      ByteBuffer bb, int ix, HdfsFileStatusWithId file, OrcTail[] result) throws IOException {
    if (bb == null) {
      return true;
    }
    result[ix] = createOrcTailFromMs(file, bb);
    if (result[ix] == null) {
      return false;
    }

    localCache.put(file.getFileStatus().getPath(), result[ix]);
    return true;
  }

  private void processPpdResult(MetadataPpdResult mpr, HdfsFileStatusWithId file,
      int ix, OrcTail[] result, ByteBuffer[] ppdResult) throws IOException {
    if (mpr == null)
     {
      return; // This file is unknown to metastore.
    }

    ppdResult[ix] = mpr.isSetIncludeBitset() ? mpr.bufferForIncludeBitset() : NO_SPLIT_AFTER_PPD;
    if (mpr.isSetMetadata()) {
      result[ix] = createOrcTailFromMs(file, mpr.bufferForMetadata());
      if (result[ix] != null) {
        localCache.put(file.getFileStatus().getPath(), result[ix]);
      }
    }
  }

  private List<Long> determineFileIdsToQuery(
      List<HdfsFileStatusWithId> files, OrcTail[] result, HashMap<Long, Integer> posMap) {
    for (int i = 0; i < result.length; ++i) {
      if (result[i] != null) {
        continue;
      }
      HdfsFileStatusWithId file = files.get(i);
      final FileStatus fs = file.getFileStatus();
      Long fileId = file.getFileId();
      if (fileId == null) {
        if (!isInTest) {
          if (!isWarnLogged || LOG.isDebugEnabled()) {
            LOG.warn("Not using metastore cache because fileId is missing: " + fs.getPath());
            isWarnLogged = true;
          }
          continue;
        }
        fileId = generateTestFileId(fs, files, i);
        LOG.info("Generated file ID " + fileId + " at " + i);
      }
      posMap.put(fileId, i);
    }
    return Lists.newArrayList(posMap.keySet());
  }

  private Long generateTestFileId(final FileStatus fs, List<HdfsFileStatusWithId> files, int i) {
    final Long fileId = HdfsUtils.createTestFileId(fs.getPath().toUri().getPath(), fs, false, null);
    files.set(i, new HdfsFileStatusWithId() {
      @Override
      public FileStatus getFileStatus() {
        return fs;
      }

      @Override
      public Long getFileId() {
        return fileId;
      }
    });
    return fileId;
  }

  private ByteBuffer getSerializedSargForMetastore(boolean isOriginal) {
    if (sarg == null) {
      return null;
    }
    ByteBuffer serializedSarg = isOriginal ? sargIsOriginal : sargNotIsOriginal;
    if (serializedSarg != null) {
      return serializedSarg;
    }
    SearchArgument sarg2 = sarg;
    Kryo kryo = SerializationUtilities.borrowKryo();
    try {
      if ((isOriginal ? sargNotIsOriginal : sargIsOriginal) == null) {
        sarg2 = kryo.copy(sarg2); // In case we need it for the other case.
      }
      translateSargToTableColIndexes(sarg2, conf, OrcInputFormat.getRootColumn(isOriginal));
      ExternalCache.Baos baos = new Baos();
      Output output = new Output(baos);
      kryo.writeObject(output, sarg2);
      output.flush();
      serializedSarg = baos.get();
      if (isOriginal) {
        sargIsOriginal = serializedSarg;
      } else {
        sargNotIsOriginal = serializedSarg;
      }
    } finally {
      SerializationUtilities.releaseKryo(kryo);
    }
    return serializedSarg;
  }

  /**
   * Modifies the SARG, replacing column names with column indexes in target table schema. This
   * basically does the same thing as all the shennannigans with included columns, except for the
   * last step where ORC gets direct subtypes of root column and uses the ordered match to map
   * table columns to file columns. The numbers put into predicate leaf should allow to go into
   * said subtypes directly by index to get the proper index in the file.
   * This won't work with schema evolution, although it's probably much easier to reason about
   * if schema evolution was to be supported, because this is a clear boundary between table
   * schema columns and all things ORC. None of the ORC stuff is used here and none of the
   * table schema stuff is used after that - ORC doesn't need a bunch of extra crap to apply
   * the SARG thus modified.
   */
  public static void translateSargToTableColIndexes(
      SearchArgument sarg, Configuration conf, int rootColumn) {
    String nameStr = OrcInputFormat.getNeededColumnNamesString(conf),
        idStr = OrcInputFormat.getSargColumnIDsString(conf);
    String[] knownNames = nameStr.split(",");
    String[] idStrs = (idStr == null) ? null : idStr.split(",");
    assert idStrs == null || knownNames.length == idStrs.length;
    HashMap<String, Integer> nameIdMap = new HashMap<>();
    for (int i = 0; i < knownNames.length; ++i) {
      Integer newId = (idStrs != null) ? Integer.parseInt(idStrs[i]) : i;
      Integer oldId = nameIdMap.put(knownNames[i], newId);
      if (oldId != null && oldId.intValue() != newId.intValue()) {
        throw new RuntimeException("Multiple IDs for " + knownNames[i] + " in column strings: ["
            + idStr + "], [" + nameStr + "]");
      }
    }
    List<PredicateLeaf> leaves = sarg.getLeaves();
    for (int i = 0; i < leaves.size(); ++i) {
      PredicateLeaf pl = leaves.get(i);
      Integer colId = nameIdMap.get(pl.getColumnName());
      String newColName = RecordReaderImpl.encodeTranslatedSargColumn(rootColumn, colId);
      SearchArgumentFactory.setPredicateLeafColumn(pl, newColName);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("SARG translated into " + sarg);
    }
  }

  private static OrcTail createOrcTailFromMs(
      HdfsFileStatusWithId file, ByteBuffer bb) throws IOException {
    if (bb == null) {
      return null;
    }
    FileStatus fs = file.getFileStatus();
    ByteBuffer copy = bb.duplicate();
    try {
      OrcTail orcTail = ReaderImpl.extractFileTail(copy, fs.getLen(), fs.getModificationTime());
      // trigger lazy read of metadata to make sure serialized data is not corrupted and readable
      orcTail.getStripeStatistics();
      return orcTail;
    } catch (Exception ex) {
      byte[] data = new byte[bb.remaining()];
      System.arraycopy(bb.array(), bb.arrayOffset() + bb.position(), data, 0, data.length);
      String msg = "Failed to parse the footer stored in cache for file ID "
          + file.getFileId() + " " + bb + " [ " + Hex.encodeHexString(data) + " ]";
      LOG.error(msg, ex);
      return null;
    }
  }

  private static final class Baos extends ByteArrayOutputStream {
    public ByteBuffer get() {
      return ByteBuffer.wrap(buf, 0, count);
    }
  }


  /** An abstraction for testing ExternalCache in OrcInputFormat. */
  public interface ExternalFooterCachesByConf {
    public interface Cache {
      Iterator<Map.Entry<Long, MetadataPpdResult>> getFileMetadataByExpr(List<Long> fileIds,
          ByteBuffer serializedSarg, boolean doGetFooters) throws HiveException;
      void clearFileMetadata(List<Long> fileIds) throws HiveException;
      Iterator<Map.Entry<Long, ByteBuffer>>  getFileMetadata(List<Long> fileIds)
          throws HiveException;
      void putFileMetadata(
          ArrayList<Long> keys, ArrayList<ByteBuffer> values) throws HiveException;
    }

    public Cache getCache(HiveConf conf) throws IOException;
  }
}
