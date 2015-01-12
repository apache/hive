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

package org.apache.hadoop.hive.ql.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Set;
import java.util.Map;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.IOPrepareCache;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

/**
 * Input format for doing queries that use indexes.
 * Uses a blockfilter file to specify the blocks to query.
 */
public class HiveIndexedInputFormat extends HiveInputFormat {
  public static final Log l4j = LogFactory.getLog("HiveIndexInputFormat");
  private final String indexFile;

  public HiveIndexedInputFormat() {
    super();
    indexFile = "hive.index.blockfilter.file";
  }

  public HiveIndexedInputFormat(String indexFileName) {
    indexFile = indexFileName;
  }

  public InputSplit[] doGetSplits(JobConf job, int numSplits) throws IOException {

    super.init(job);

    Path[] dirs = FileInputFormat.getInputPaths(job);
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }
    JobConf newjob = new JobConf(job);
    ArrayList<InputSplit> result = new ArrayList<InputSplit>();

    // for each dir, get the InputFormat, and do getSplits.
    PartitionDesc part;
    for (Path dir : dirs) {
      part = HiveFileFormatUtils
          .getPartitionDescFromPathRecursively(pathToPartitionInfo, dir,
              IOPrepareCache.get().allocatePartitionDescMap(), true);
      // create a new InputFormat instance if this is the first time to see this
      // class
      Class inputFormatClass = part.getInputFileFormatClass();
      InputFormat inputFormat = getInputFormatFromCache(inputFormatClass, job);
      Utilities.copyTableJobPropertiesToConf(part.getTableDesc(), newjob);

      FileInputFormat.setInputPaths(newjob, dir);
      newjob.setInputFormat(inputFormat.getClass());
      InputSplit[] iss = inputFormat.getSplits(newjob, numSplits / dirs.length);
      for (InputSplit is : iss) {
        result.add(new HiveInputSplit(is, inputFormatClass.getName()));
      }
    }
    return result.toArray(new HiveInputSplit[result.size()]);
  }

  public static List<String> getIndexFiles(String indexFileStr) {
    // tokenize and store string of form (path,)+
    if (indexFileStr == null) {
      return null;
    }
    String[] chunks = indexFileStr.split(",");
    return Arrays.asList(chunks);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    String indexFileStr = job.get(indexFile);
    l4j.info("index_file is " + indexFileStr);
    List<String> indexFiles = getIndexFiles(indexFileStr);

    HiveIndexResult hiveIndexResult = null;
    if (indexFiles != null) {
      boolean first = true;
      StringBuilder newInputPaths = new StringBuilder();
      try {
        hiveIndexResult = new HiveIndexResult(indexFiles, job);
      } catch (HiveException e) {
        l4j.error("Unable to read index..");
        throw new IOException(e);
      }

      Set<String> inputFiles = hiveIndexResult.buckets.keySet();
      if (inputFiles == null || inputFiles.size() <= 0) {
        // return empty splits if index results were empty
        return new InputSplit[0];
      }
      Iterator<String> iter = inputFiles.iterator();
      while(iter.hasNext()) {
        String path = iter.next();
        if (path.trim().equalsIgnoreCase("")) {
          continue;
        }
        if (!first) {
          newInputPaths.append(",");
        } else {
          first = false;
        }
        newInputPaths.append(path);
      }
      FileInputFormat.setInputPaths(job, newInputPaths.toString());
    } else {
      return super.getSplits(job, numSplits);
    }

    HiveInputSplit[] splits = (HiveInputSplit[]) this.doGetSplits(job, numSplits);

    ArrayList<HiveInputSplit> newSplits = new ArrayList<HiveInputSplit>(
        numSplits);

    long maxInputSize = HiveConf.getLongVar(job, ConfVars.HIVE_INDEX_COMPACT_QUERY_MAX_SIZE);
    if (maxInputSize < 0) {
      maxInputSize=Long.MAX_VALUE;
    }

    long sumSplitLengths = 0;
    for (HiveInputSplit split : splits) {
      l4j.info("split start : " + split.getStart());
      l4j.info("split end : " + (split.getStart() + split.getLength()));

      try {
        if (hiveIndexResult.contains(split)) {
          // we may miss a sync here
          HiveInputSplit newSplit = split;
          if (split.inputFormatClassName().contains("RCFile")
              || split.inputFormatClassName().contains("SequenceFile")) {
            if (split.getStart() > SequenceFile.SYNC_INTERVAL) {
              newSplit = new HiveInputSplit(new FileSplit(split.getPath(),
                  split.getStart() - SequenceFile.SYNC_INTERVAL,
                  split.getLength() + SequenceFile.SYNC_INTERVAL,
                  split.getLocations()),
                  split.inputFormatClassName());
            }
          }
          sumSplitLengths += newSplit.getLength();
          if (sumSplitLengths > maxInputSize) {
            throw new IOException(
                "Size of data to read during a compact-index-based query exceeded the maximum of "
                    + maxInputSize + " set in " + ConfVars.HIVE_INDEX_COMPACT_QUERY_MAX_SIZE.varname);
          }
          newSplits.add(newSplit);
        }
      } catch (HiveException e) {
        throw new RuntimeException(
            "Unable to get metadata for input table split" + split.getPath(), e);
      }
    }
    InputSplit retA[] = newSplits.toArray((new FileSplit[newSplits.size()]));
    l4j.info("Number of input splits: " + splits.length + " new input splits: "
        + retA.length + ", sum of split lengths: " + sumSplitLengths);
    return retA;
  }
}
