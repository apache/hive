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

package org.apache.hadoop.hive.ql.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat.CombineHiveInputSplit;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

/**
 * CombineHiveRecordReader.
 *
 * @param <K>
 * @param <V>
 */
public class CombineHiveRecordReader<K extends WritableComparable, V extends Writable>
    extends HiveContextAwareRecordReader<K, V> {
  private org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(CombineHiveRecordReader.class);

  private LinkedHashMap<Path, PartitionDesc> pathToPartInfo;

  public CombineHiveRecordReader(InputSplit split, Configuration conf,
      Reporter reporter, Integer partition, RecordReader preReader) throws IOException {
    super((JobConf)conf);
    CombineHiveInputSplit hsplit = split instanceof CombineHiveInputSplit ?
        (CombineHiveInputSplit) split :
        new CombineHiveInputSplit(jobConf, (CombineFileSplit) split);
    String inputFormatClassName = hsplit.inputFormatClassName();
    Class inputFormatClass = null;
    try {
      inputFormatClass = JavaUtils.loadClass(inputFormatClassName);
    } catch (ClassNotFoundException e) {
      throw new IOException("CombineHiveRecordReader: class not found "
          + inputFormatClassName);
    }
    InputFormat inputFormat = HiveInputFormat.getInputFormatFromCache(inputFormatClass, jobConf);
    try {
      // TODO: refactor this out
      if (pathToPartInfo == null) {
        MapWork mrwork;
        if (HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
          mrwork = (MapWork) Utilities.getMergeWork(jobConf);
          if (mrwork == null) {
            mrwork = Utilities.getMapWork(jobConf);
          }
        } else {
          mrwork = Utilities.getMapWork(jobConf);
        }
        pathToPartInfo = mrwork.getPathToPartitionInfo();
      }

      PartitionDesc part = extractSinglePartSpec(hsplit);
      inputFormat = HiveInputFormat.wrapForLlap(inputFormat, jobConf, part);
    } catch (HiveException e) {
      throw new IOException(e);
    }

    // create a split for the given partition
    FileSplit fsplit = new FileSplit(hsplit.getPaths()[partition], hsplit
        .getStartOffsets()[partition], hsplit.getLengths()[partition], hsplit
        .getLocations());

    this.setRecordReader(inputFormat.getRecordReader(fsplit, jobConf, reporter));

    this.initIOContext(fsplit, jobConf, inputFormatClass, this.recordReader);

    //If current split is from the same file as preceding split and the preceding split has footerbuffer,
    //the current split should use the preceding split's footerbuffer in order to skip footer correctly.
    if (preReader != null && preReader instanceof CombineHiveRecordReader
        && ((CombineHiveRecordReader)preReader).getFooterBuffer() != null) {
      if (partition != 0 && hsplit.getPaths()[partition -1].equals(hsplit.getPaths()[partition]))
        this.setFooterBuffer(((CombineHiveRecordReader)preReader).getFooterBuffer());
    }

  }

  private PartitionDesc extractSinglePartSpec(CombineHiveInputSplit hsplit) throws IOException {
    PartitionDesc part = null;
    Map<Map<Path,PartitionDesc>, Map<Path,PartitionDesc>> cache = new HashMap<>();
    for (Path path : hsplit.getPaths()) {
      PartitionDesc otherPart = HiveFileFormatUtils.getFromPathRecursively(
          pathToPartInfo, path, cache);
      LOG.debug("Found spec for " + path + " " + otherPart + " from " + pathToPartInfo);
      if (part == null) {
        part = otherPart;
      } else if (otherPart != part) { // Assume we should have the exact same object.
        // TODO: we could also compare the schema and SerDe, and pass only those to the call
        //       instead; most of the time these would be the same and LLAP IO can handle that.
        LOG.warn("Multiple partitions found; not going to pass a part spec to LLAP IO: {"
            + part.getPartSpec() + "} and {" + otherPart.getPartSpec() + "}");
        return null;
      }
    }
    return part;
  }

  @Override
  public void doClose() throws IOException {
    recordReader.close();
  }

  @Override
  public K createKey() {
    return (K) recordReader.createKey();
  }

  @Override
  public V createValue() {
    return (V) recordReader.createValue();
  }

  @Override
  public long getPos() throws IOException {
    return recordReader.getPos();
  }

  @Override
  public float getProgress() throws IOException {
    if (isSorted) {
      return super.getProgress();
    }

    return recordReader.getProgress();
  }

  @Override
  public boolean doNext(K key, V value) throws IOException {
    if (ExecMapper.getDone()) {
      return false;
    }
    return super.doNext(key, value);
  }
}
