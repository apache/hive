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
package org.apache.hadoop.hive.ql.exec.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.HashTableSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TemporaryHashSinkOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.mr.MapredLocalTask;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinBytesTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectSerDeContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BucketMapJoinContext;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SparkBucketMapJoinContext;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;

/**
 * HashTableLoader for Spark to load the hashtable for MapJoins.
 */
public class HashTableLoader implements org.apache.hadoop.hive.ql.exec.HashTableLoader {

  private static final Logger LOG = LoggerFactory.getLogger(HashTableLoader.class.getName());

  private ExecMapperContext context;
  private Configuration hconf;

  private MapJoinOperator joinOp;
  private MapJoinDesc desc;

  private boolean useFastContainer = false;

  @Override
  public void init(ExecMapperContext context, MapredContext mrContext, Configuration hconf,
      MapJoinOperator joinOp) {
    this.context = context;
    this.hconf = hconf;
    this.joinOp = joinOp;
    this.desc = joinOp.getConf();
    if (desc.getVectorMode() && HiveConf.getBoolVar(
        hconf, HiveConf.ConfVars.HIVE_VECTORIZATION_MAPJOIN_NATIVE_FAST_HASHTABLE_ENABLED)) {
      if (joinOp instanceof VectorizationOperator) {
        VectorMapJoinDesc vectorDesc = (VectorMapJoinDesc) ((VectorizationOperator) joinOp).getVectorDesc();
        useFastContainer = vectorDesc != null && vectorDesc.getHashTableImplementationType() ==
            VectorMapJoinDesc.HashTableImplementationType.FAST;
      }
    }
  }

  @Override
  public void load(MapJoinTableContainer[] mapJoinTables,
      MapJoinTableContainerSerDe[] mapJoinTableSerdes)
      throws HiveException {

    // Note: it's possible that a MJ operator is in a ReduceWork, in which case the
    // currentInputPath will be null. But, since currentInputPath is only interesting
    // for bucket join case, and for bucket join the MJ operator will always be in
    // a MapWork, this should be OK.
    String currentInputPath =
        context.getCurrentInputPath() == null ? null : context.getCurrentInputPath().toString();

    LOG.info("******* Load from HashTable for input file: " + currentInputPath);
    MapredLocalWork localWork = context.getLocalWork();
    try {
      if (localWork.getDirectFetchOp() != null) {
        loadDirectly(mapJoinTables, currentInputPath);
      }
      // All HashTables share the same base dir,
      // which is passed in as the tmp path
      Path baseDir = localWork.getTmpPath();
      if (baseDir == null) {
        return;
      }
      FileSystem fs = FileSystem.get(baseDir.toUri(), hconf);
      BucketMapJoinContext mapJoinCtx = localWork.getBucketMapjoinContext();
      boolean firstContainer = true;
      boolean useOptimizedContainer = !useFastContainer && HiveConf.getBoolVar(
          hconf, HiveConf.ConfVars.HIVEMAPJOINUSEOPTIMIZEDTABLE);
      for (int pos = 0; pos < mapJoinTables.length; pos++) {
        if (pos == desc.getPosBigTable() || mapJoinTables[pos] != null) {
          continue;
        }
        if (useOptimizedContainer) {
          MapJoinObjectSerDeContext keyCtx = mapJoinTableSerdes[pos].getKeyContext();
          ObjectInspector keyOI = keyCtx.getSerDe().getObjectInspector();
          if (!MapJoinBytesTableContainer.isSupportedKey(keyOI)) {
            if (firstContainer) {
              LOG.warn("Not using optimized table container." +
                  "Only a subset of mapjoin keys is supported.");
              useOptimizedContainer = false;
              HiveConf.setBoolVar(hconf, HiveConf.ConfVars.HIVEMAPJOINUSEOPTIMIZEDTABLE, false);
            } else {
              throw new HiveException("Only a subset of mapjoin keys is supported.");
            }
          }
        }
        firstContainer = false;
        String bigInputPath = currentInputPath;
        if (currentInputPath != null && mapJoinCtx != null) {
          if (!desc.isBucketMapJoin()) {
            bigInputPath = null;
          } else {
            Set<String> aliases =
              ((SparkBucketMapJoinContext) mapJoinCtx).getPosToAliasMap().get(pos);
            String alias = aliases.iterator().next();
            // Any one small table input path
            String smallInputPath =
              mapJoinCtx.getAliasBucketFileNameMapping().get(alias).get(bigInputPath).get(0);
            bigInputPath = mapJoinCtx.getMappingBigFile(alias, smallInputPath);
          }
        }
        String fileName = localWork.getBucketFileName(bigInputPath);
        Path path = Utilities.generatePath(baseDir, desc.getDumpFilePrefix(), (byte) pos, fileName);
        mapJoinTables[pos] = load(fs, path, mapJoinTableSerdes[pos]);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  private MapJoinTableContainer load(FileSystem fs, Path path,
      MapJoinTableContainerSerDe mapJoinTableSerde) throws HiveException {
    LOG.info("\tLoad back all hashtable files from tmp folder uri:" + path);
    if (!SparkUtilities.isDedicatedCluster(hconf)) {
      return useFastContainer ? mapJoinTableSerde.loadFastContainer(desc, fs, path, hconf) :
          mapJoinTableSerde.load(fs, path, hconf);
    }
    MapJoinTableContainer mapJoinTable = SmallTableCache.get(path);
    if (mapJoinTable == null) {
      synchronized (path.toString().intern()) {
        mapJoinTable = SmallTableCache.get(path);
        if (mapJoinTable == null) {
          mapJoinTable = useFastContainer ?
              mapJoinTableSerde.loadFastContainer(desc, fs, path, hconf) :
              mapJoinTableSerde.load(fs, path, hconf);
          SmallTableCache.cache(path, mapJoinTable);
        }
      }
    }
    return mapJoinTable;
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
