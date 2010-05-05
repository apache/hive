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

package org.apache.hadoop.hive.ql.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.ql.exec.ExecMapper;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveInputFormat.HiveInputSplit;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.HadoopShims.CombineFileInputFormatShim;
import org.apache.hadoop.hive.shims.HadoopShims.InputSplitShim;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * BucketizedHiveInputFormat serves the similar function as hiveInputFormat but
 * its getSplits() always group splits from one input file into one wrapper
 * split. It is useful for the applications that requires input files to fit in
 * one mapper.
 */
public class BucketizedHiveInputFormat<K extends WritableComparable, V extends Writable>
    extends HiveInputFormat<K, V> {

  public static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat");

  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {

    BucketizedHiveInputSplit hsplit = (BucketizedHiveInputSplit) split;

    String inputFormatClassName = null;
    Class inputFormatClass = null;
    try {
      inputFormatClassName = hsplit.inputFormatClassName();
      inputFormatClass = job.getClassByName(inputFormatClassName);
    } catch (Exception e) {
      throw new IOException("cannot find class " + inputFormatClassName);
    }

    // clone a jobConf for setting needed columns for reading
    JobConf cloneJobConf = new JobConf(job);
    initColumnsNeeded(cloneJobConf, inputFormatClass, hsplit.getPath()
        .toString(), hsplit.getPath().toUri().getPath());

    InputFormat inputFormat = getInputFormatFromCache(inputFormatClass,
        cloneJobConf);
    return new BucketizedHiveRecordReader(inputFormat, hsplit, cloneJobConf,
        reporter);
  }

  protected FileStatus[] listStatus(JobConf job, Path path) throws IOException {
    ArrayList<FileStatus> result = new ArrayList<FileStatus>();
    List<IOException> errors = new ArrayList<IOException>();

    FileSystem fs = path.getFileSystem(job);
    FileStatus[] matches = fs.globStatus(path);
    if (matches == null) {
      errors.add(new IOException("Input path does not exist: " + path));
    } else if (matches.length == 0) {
      errors.add(new IOException("Input Pattern " + path + " matches 0 files"));
    } else {
      for (FileStatus globStat : matches) {
        FileUtils.listStatusRecursively(fs, globStat, result);
      }
    }

    if (!errors.isEmpty()) {
      throw new InvalidInputException(errors);
    }
    LOG.info("Total input paths to process : " + result.size());
    return result.toArray(new FileStatus[result.size()]);

  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    init(job);

    Path[] dirs = FileInputFormat.getInputPaths(job);
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }
    JobConf newjob = new JobConf(job);
    ArrayList<InputSplit> result = new ArrayList<InputSplit>();

    int numOrigSplits = 0;
    // for each dir, get all files under the dir, do getSplits to each
    // individual file,
    // and then create a BucketizedHiveInputSplit on it
    for (Path dir : dirs) {
      PartitionDesc part = getPartitionDescFromPath(pathToPartitionInfo, dir);
      // create a new InputFormat instance if this is the first time to see this
      // class
      Class inputFormatClass = part.getInputFileFormatClass();
      InputFormat inputFormat = getInputFormatFromCache(inputFormatClass, job);
      newjob.setInputFormat(inputFormat.getClass());

      FileStatus[] listStatus = listStatus(newjob, dir);

      for (FileStatus status : listStatus) {
        LOG.info("block size: " + status.getBlockSize());
        LOG.info("file length: " + status.getLen());
        FileInputFormat.setInputPaths(newjob, status.getPath());
        InputSplit[] iss = inputFormat.getSplits(newjob, 0);
        if (iss != null && iss.length > 0) {
          numOrigSplits += iss.length;
          result.add(new BucketizedHiveInputSplit(iss, inputFormatClass
              .getName()));
        }
      }
    }
    LOG.info(result.size() + " bucketized splits generated from "
        + numOrigSplits + " original splits.");
    return result.toArray(new BucketizedHiveInputSplit[result.size()]);
  }
}
