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
package org.apache.hadoop.hive.ql.exec;

import java.io.BufferedOutputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Set;

import org.apache.commons.io.FileExistsException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinPersistableTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BucketMapJoinContext;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.SparkBucketMapJoinContext;
import org.apache.hadoop.hive.ql.plan.SparkHashTableSinkDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkHashTableSinkOperator
    extends TerminalOperator<SparkHashTableSinkDesc> implements Serializable {
  private static final long serialVersionUID = 1L;
  private final String CLASS_NAME = this.getClass().getName();
  private final transient PerfLogger perfLogger = SessionState.getPerfLogger();
  protected static final Logger LOG = LoggerFactory.getLogger(SparkHashTableSinkOperator.class.getName());
  public static final String DFS_REPLICATION_MAX = "dfs.replication.max";
  private int minReplication = 10;

  private final HashTableSinkOperator htsOperator;

  /** Kryo ctor. */
  protected SparkHashTableSinkOperator() {
    super();
    htsOperator = null; // Kryo will set this; or so we hope.
  }

  public SparkHashTableSinkOperator(CompilationOpContext ctx) {
    super(ctx);
    htsOperator = new HashTableSinkOperator(ctx);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    ObjectInspector[] inputOIs = new ObjectInspector[conf.getTagLength()];
    byte tag = conf.getTag();
    inputOIs[tag] = inputObjInspectors[0];
    conf.setTagOrder(new Byte[]{ tag });
    int dfsMaxReplication = hconf.getInt(DFS_REPLICATION_MAX, minReplication);
    // minReplication value should not cross the value of dfs.replication.max
    minReplication = Math.min(minReplication, dfsMaxReplication);
    htsOperator.setConf(conf);
    htsOperator.initialize(hconf, inputOIs);
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    // Ignore the tag passed in, which should be 0, not what we want
    htsOperator.process(row, conf.getTag());
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    try {
      MapJoinPersistableTableContainer[] mapJoinTables = htsOperator.mapJoinTables;
      byte tag = conf.getTag();
      if (mapJoinTables == null || mapJoinTables.length < tag
          || mapJoinTables[tag] == null) {
        LOG.debug("mapJoinTable is null");
      } else if (abort) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Aborting, skip dumping side-table for tag: " + tag);
        }
      } else {
        String method = PerfLogger.SPARK_FLUSH_HASHTABLE + getName();
        perfLogger.PerfLogBegin(CLASS_NAME, method);
        try {
          flushToFile(mapJoinTables[tag], tag);
        } finally {
          perfLogger.PerfLogEnd(CLASS_NAME, method);
        }
      }
      super.closeOp(abort);
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  protected void flushToFile(MapJoinPersistableTableContainer tableContainer,
      byte tag) throws Exception {
    MapredLocalWork localWork = getExecContext().getLocalWork();
    BucketMapJoinContext mapJoinCtx = localWork.getBucketMapjoinContext();
    Path inputPath = getExecContext().getCurrentInputPath();
    String bigInputPath = null;
    if (inputPath != null && mapJoinCtx != null) {
      Set<String> aliases =
        ((SparkBucketMapJoinContext)mapJoinCtx).getPosToAliasMap().get((int)tag);
      bigInputPath = mapJoinCtx.getMappingBigFile(
        aliases.iterator().next(), inputPath.toString());
    }

    // get tmp file URI
    Path tmpURI = localWork.getTmpHDFSPath();
    LOG.info("Temp URI for side table: " + tmpURI);
    // get current bucket file name
    String fileName = localWork.getBucketFileName(bigInputPath);
    // get the tmp URI path; it will be a hdfs path if not local mode
    String dumpFilePrefix = conf.getDumpFilePrefix();
    Path path = Utilities.generatePath(tmpURI, dumpFilePrefix, tag, fileName);
    FileSystem fs = path.getFileSystem(htsOperator.getConfiguration());
    short replication = fs.getDefaultReplication(path);

    fs.mkdirs(path);  // Create the folder and its parents if not there
    while (true) {
      path = new Path(path, getOperatorId()
        + "-" + Math.abs(Utilities.randGen.nextInt()));
      try {
        // This will guarantee file name uniqueness.
        if (fs.createNewFile(path)) {
          break;
        }
      } catch (FileExistsException e) {
        // No problem, use a new name
      }
    }
    // TODO find out numOfPartitions for the big table
    int numOfPartitions = replication;
    replication = (short) Math.max(minReplication, numOfPartitions);
    htsOperator.console.printInfo(Utilities.now() + "\tDump the side-table for tag: " + tag
      + " with group count: " + tableContainer.size() + " into file: " + path);
    try {
      // get the hashtable file and path
      OutputStream os = null;
      ObjectOutputStream out = null;
      MapJoinTableContainerSerDe mapJoinTableSerde = htsOperator.mapJoinTableSerdes[tag];
      try {
        os = fs.create(path, replication);
        out = new ObjectOutputStream(new BufferedOutputStream(os, 4096));
        mapJoinTableSerde.persist(out, tableContainer);
      } finally {
        if (out != null) {
          out.close();
        } else if (os != null) {
          os.close();
        }
      }

      FileStatus status = fs.getFileStatus(path);
      htsOperator.console.printInfo(Utilities.now() + "\tUploaded 1 File to: " + path
        + " (" + status.getLen() + " bytes)");
    } catch (Exception e) {
      // Failed to dump the side-table, remove the partial file
      try {
        fs.delete(path, false);
      } catch (Exception ex) {
        LOG.warn("Got exception in deleting partial side-table dump for tag: "
          + tag + ", file " + path, ex);
      }
      throw e;
    }
    tableContainer.clear();
  }

  /**
   * Implements the getName function for the Node Interface.
   *
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return SparkHashTableSinkOperator.getOperatorName();
  }

  public static String getOperatorName() {
    return HashTableSinkOperator.getOperatorName();
  }

  @Override
  public OperatorType getType() {
    return OperatorType.HASHTABLESINK;
  }
}
