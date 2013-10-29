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
package org.apache.hadoop.hive.ql.exec.mr;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.ObjectInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.shims.ShimLoader;

/**
 * HashTableLoader for MR loads the hashtable for MapJoins from local disk (hashtables
 * are distributed by using the DistributedCache.
 *
 */
public class HashTableLoader implements org.apache.hadoop.hive.ql.exec.HashTableLoader {

  private static final Log LOG = LogFactory.getLog(MapJoinOperator.class.getName());

  public HashTableLoader() {
  }

  @Override
  public void load(ExecMapperContext context,
      Configuration hconf,
      MapJoinDesc desc,
      byte posBigTable,
      MapJoinTableContainer[] mapJoinTables,
      MapJoinTableContainerSerDe[] mapJoinTableSerdes) throws HiveException {

    String baseDir = null;
    Path currentInputPath = context.getCurrentInputPath();
    LOG.info("******* Load from HashTable File: input : " + currentInputPath);
    String fileName = context.getLocalWork().getBucketFileName(currentInputPath.toString());
    try {
      if (ShimLoader.getHadoopShims().isLocalMode(hconf)) {
        baseDir = context.getLocalWork().getTmpFileURI();
      } else {
        Path[] localArchives;
        String stageID = context.getLocalWork().getStageID();
        String suffix = Utilities.generateTarFileName(stageID);
        FileSystem localFs = FileSystem.getLocal(hconf);
        localArchives = DistributedCache.getLocalCacheArchives(hconf);
        Path archive;
        for (int j = 0; j < localArchives.length; j++) {
          archive = localArchives[j];
          if (!archive.getName().endsWith(suffix)) {
            continue;
          }
          Path archiveLocalLink = archive.makeQualified(localFs);
          baseDir = archiveLocalLink.toUri().getPath();
        }
      }
      for (int pos = 0; pos < mapJoinTables.length; pos++) {
        if (pos == posBigTable) {
          continue;
        }
        if(baseDir == null) {
          throw new IllegalStateException("baseDir cannot be null");
        }
        String filePath = Utilities.generatePath(baseDir, desc.getDumpFilePrefix(), (byte)pos, fileName);
        Path path = new Path(filePath);
        LOG.info("\tLoad back 1 hashtable file from tmp file uri:" + path);
        ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(
            new FileInputStream(path.toUri().getPath()), 4096));
        try{
          mapJoinTables[pos] = mapJoinTableSerdes[pos].load(in);
        } finally {
          in.close();
        }
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

}
