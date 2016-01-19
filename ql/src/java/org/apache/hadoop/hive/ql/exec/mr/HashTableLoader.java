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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.HashTableSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TemporaryHashSinkOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.JobConf;

/**
 * HashTableLoader for MR loads the hashtable for MapJoins from local disk (hashtables
 * are distributed by using the DistributedCache.
 *
 */
public class HashTableLoader implements org.apache.hadoop.hive.ql.exec.HashTableLoader {

  private static final Logger LOG = LoggerFactory.getLogger(MapJoinOperator.class.getName());

  private ExecMapperContext context;
  private Configuration hconf;

  private MapJoinOperator joinOp;
  private MapJoinDesc desc;

  @Override
  public void init(ExecMapperContext context, MapredContext mrContext, Configuration hconf,
      MapJoinOperator joinOp) {
    this.context = context;
    this.hconf = hconf;
    this.joinOp = joinOp;
    this.desc = joinOp.getConf();
  }

  @Override
  public void load(
      MapJoinTableContainer[] mapJoinTables,
      MapJoinTableContainerSerDe[] mapJoinTableSerdes) throws HiveException {

    String currentInputPath = context.getCurrentInputPath().toString();
    LOG.info("******* Load from HashTable for input file: " + currentInputPath);
    MapredLocalWork localWork = context.getLocalWork();
    try {
      if (localWork.getDirectFetchOp() != null) {
        loadDirectly(mapJoinTables, currentInputPath);
      }
      Path baseDir = getBaseDir(localWork);
      if (baseDir == null) {
        return;
      }
      String fileName = localWork.getBucketFileName(currentInputPath);
      for (int pos = 0; pos < mapJoinTables.length; pos++) {
        if (pos == desc.getPosBigTable() || mapJoinTables[pos] != null) {
          continue;
        }
        Path path = Utilities.generatePath(baseDir, desc.getDumpFilePrefix(), (byte)pos, fileName);
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

  private Path getBaseDir(MapredLocalWork localWork) throws Exception {
    if (ShimLoader.getHadoopShims().isLocalMode(hconf)) {
      return localWork.getTmpPath();
    }
    Path[] localArchives = DistributedCache.getLocalCacheArchives(hconf);
    if (localArchives != null) {
      String stageID = localWork.getStageID();
      String suffix = Utilities.generateTarFileName(stageID);
      FileSystem localFs = FileSystem.getLocal(hconf);
      for (int j = 0; j < localArchives.length; j++) {
        Path archive = localArchives[j];
        if (!archive.getName().endsWith(suffix)) {
          continue;
        }
        return archive.makeQualified(localFs);
      }
    }
    return null;
  }

  private void loadDirectly(MapJoinTableContainer[] mapJoinTables, String inputFileName)
      throws Exception {
    MapredLocalWork localWork = context.getLocalWork();
    List<Operator<?>> directWorks = localWork.getDirectFetchOp().get(joinOp);
    if (directWorks == null || directWorks.isEmpty()) {
      return;
    }
    JobConf job = new JobConf(hconf);
    MapredLocalTask localTask = new MapredLocalTask(localWork, job, false);

    HashTableSinkOperator sink = new TemporaryHashSinkOperator(new CompilationOpContext(), desc);
    sink.setParentOperators(new ArrayList<Operator<? extends OperatorDesc>>(directWorks));

    for (Operator<?> operator : directWorks) {
      if (operator != null) {
        operator.setChildOperators(Arrays.<Operator<? extends OperatorDesc>>asList(sink));
      }
    }
    localTask.setExecContext(context);
    localTask.startForward(inputFileName);

    MapJoinTableContainer[] tables = sink.getMapJoinTables();
    for (int i = 0; i < sink.getNumParent(); i++) {
      if (sink.getParentOperators().get(i) != null) {
        mapJoinTables[i] = tables[i];
      }
    }

    Arrays.fill(tables, null);
  }
}
