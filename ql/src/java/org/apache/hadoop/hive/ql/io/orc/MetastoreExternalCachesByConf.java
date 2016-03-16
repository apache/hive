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

package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetadataPpdResult;
import org.apache.hadoop.hive.ql.io.orc.ExternalCache.ExternalFooterCachesByConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * An implementation of external cache and factory based on metastore.
 */
public class MetastoreExternalCachesByConf implements ExternalFooterCachesByConf {
  public static class HBaseCache implements ExternalFooterCachesByConf.Cache {
    private Hive hive;

    public HBaseCache(Hive hive) {
      this.hive = hive;
    }

    @Override
    public Iterator<Entry<Long, MetadataPpdResult>> getFileMetadataByExpr(
        List<Long> fileIds, ByteBuffer sarg, boolean doGetFooters) throws HiveException {
      return hive.getFileMetadataByExpr(fileIds, sarg, doGetFooters).iterator();
    }

    @Override
    public void clearFileMetadata(List<Long> fileIds) throws HiveException {
      hive.clearFileMetadata(fileIds);
    }

    @Override
    public Iterator<Entry<Long, ByteBuffer>> getFileMetadata(
        List<Long> fileIds) throws HiveException {
      return hive.getFileMetadata(fileIds).iterator();
    }

    @Override
    public void putFileMetadata(
        ArrayList<Long> fileIds, ArrayList<ByteBuffer> metadata) throws HiveException {
      hive.putFileMetadata(fileIds, metadata);
    }
  }

  @Override
  public ExternalFooterCachesByConf.Cache getCache(HiveConf conf) throws IOException {
    // TODO: we wish we could cache the Hive object, but it's not thread safe, and each
    //       threadlocal we "cache" would need to be reinitialized for every query. This is
    //       a huge PITA. Hive object will be cached internally, but the compat check will be
    //       done every time inside get().
    try {
      return new HBaseCache(Hive.getWithFastCheck(conf));
    } catch (HiveException e) {
      throw new IOException(e);
    }
  }
}