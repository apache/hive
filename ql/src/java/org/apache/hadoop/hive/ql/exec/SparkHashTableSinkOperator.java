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
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Set;

import org.apache.commons.io.FileExistsException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinPersistableTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BucketMapJoinContext;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.SparkBucketMapJoinContext;
import org.apache.hadoop.hive.ql.plan.SparkHashTableSinkDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class SparkHashTableSinkOperator
    extends TerminalOperator<SparkHashTableSinkDesc> implements Serializable {
  private static final int MIN_REPLICATION = 10;
  private static final long serialVersionUID = 1L;
  private final String CLASS_NAME = this.getClass().getName();
  private final PerfLogger perfLogger = PerfLogger.getPerfLogger();
  protected static final Log LOG = LogFactory.getLog(SparkHashTableSinkOperator.class.getName());

  private HashTableSinkOperator htsOperator;

  // The position of this table
  private byte tag;

  public SparkHashTableSinkOperator() {
    htsOperator = new HashTableSinkOperator();
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    ObjectInspector[] inputOIs = new ObjectInspector[conf.getTagLength()];
    inputOIs[tag] = inputObjInspectors[0];
    conf.setTagOrder(new Byte[]{ tag });
    htsOperator.setConf(conf);
    htsOperator.initialize(hconf, inputOIs);
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    // Ignore the tag passed in, which should be 0, not what we want
    htsOperator.processOp(row, this.tag);
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    try {
      MapJoinPersistableTableContainer[] mapJoinTables = htsOperator.mapJoinTables;
      if (mapJoinTables == null || mapJoinTables.length < tag
          || mapJoinTables[tag] == null) {
        LOG.debug("mapJoinTable is null");
      } else {
        flushToFile(mapJoinTables[tag], tag);
      }
      super.closeOp(abort);
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  protected void flushToFile(MapJoinPersistableTableContainer tableContainer,
      byte tag) throws IOException, HiveException {
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_FLUSH_HASHTABLE + this.getName());
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
      // TODO find out numOfPartitions for the big table
      int numOfPartitions = replication;
      replication = (short) Math.min(MIN_REPLICATION, numOfPartitions);
    }
    htsOperator.console.printInfo(Utilities.now() + "\tDump the side-table for tag: " + tag
      + " with group count: " + tableContainer.size() + " into file: " + path);
    // get the hashtable file and path
    // get the hashtable file and path
    OutputStream os = null;
    ObjectOutputStream out = null;
    try {
      os = fs.create(path, replication);
      out = new ObjectOutputStream(new BufferedOutputStream(os, 4096));
      MapJoinTableContainerSerDe mapJoinTableSerde = htsOperator.mapJoinTableSerdes[tag];
      mapJoinTableSerde.persist(out, tableContainer);
    } finally {
      if (out != null) {
        out.close();
      } else if (os != null) {
        os.close();
      }
    }
    tableContainer.clear();
    FileStatus status = fs.getFileStatus(path);
    htsOperator.console.printInfo(Utilities.now() + "\tUploaded 1 File to: " + path
      + " (" + status.getLen() + " bytes)");
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_FLUSH_HASHTABLE + this.getName());
  }

  public void setTag(byte tag) {
    this.tag = tag;
  }

  /**
   * Implements the getName function for the Node Interface.
   *
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return HashTableSinkOperator.getOperatorName();
  }

  @Override
  public OperatorType getType() {
    return OperatorType.HASHTABLESINK;
  }
}
