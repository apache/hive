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

package org.apache.hadoop.hive.ql.parse.spark;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.spark.SparkPartitionPruningSinkDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * This operator gets partition info from the upstream operators, and write them
 * to HDFS. This will later be read at the driver, and used for pruning the partitions
 * for the big table side.
 */
public class SparkPartitionPruningSinkOperator extends Operator<SparkPartitionPruningSinkDesc> {

  @SuppressWarnings("deprecation")
  protected transient Serializer serializer;
  protected transient DataOutputBuffer buffer;
  protected static final Logger LOG = LoggerFactory.getLogger(SparkPartitionPruningSinkOperator.class);

  /** Kryo ctor. */
  @VisibleForTesting
  public SparkPartitionPruningSinkOperator() {
    super();
  }

  public SparkPartitionPruningSinkOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  @SuppressWarnings("deprecation")
  public void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    serializer = (Serializer) ReflectionUtils.newInstance(
        conf.getTable().getDeserializerClass(), null);
    buffer = new DataOutputBuffer();
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    ObjectInspector rowInspector = inputObjInspectors[0];
    try {
      Writable writableRow = serializer.serialize(row, rowInspector);
      writableRow.write(buffer);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    if (!abort) {
      try {
        flushToFile();
      } catch (Exception e) {
        throw new HiveException(e);
      }
    }
  }

  private void flushToFile() throws IOException {
    // write an intermediate file to the specified path
    // the format of the path is: tmpPath/targetWorkId/sourceWorkId/randInt
    Path path = conf.getPath();
    FileSystem fs = path.getFileSystem(this.getConfiguration());
    fs.mkdirs(path);

    while (true) {
      path = new Path(path, String.valueOf(Utilities.randGen.nextInt()));
      if (!fs.exists(path)) {
        break;
      }
    }

    short numOfRepl = fs.getDefaultReplication(path);

    ObjectOutputStream out = null;
    FSDataOutputStream fsout = null;

    try {
      fsout = fs.create(path, numOfRepl);
      out = new ObjectOutputStream(new BufferedOutputStream(fsout, 4096));
      out.writeUTF(conf.getTargetColumnName());
      buffer.writeTo(out);
    } catch (Exception e) {
      try {
        fs.delete(path, false);
      } catch (Exception ex) {
        LOG.warn("Exception happened while trying to clean partial file.");
      }
      throw e;
    } finally {
      if (out != null) {
        LOG.info("Flushed to file: " + path);
        out.close();
      } else if (fsout != null) {
        fsout.close();
      }
    }
  }

  @Override
  public OperatorType getType() {
    return OperatorType.SPARKPRUNINGSINK;
  }

  @Override
  public String getName() {
    return SparkPartitionPruningSinkOperator.getOperatorName();
  }

  public static String getOperatorName() {
    return "SPARKPRUNINGSINK";
  }

}
