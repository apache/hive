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

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.io.HiveIOExceptionHandlerUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * Symlink file is a text file which contains a list of filename / dirname.
 * This input method reads symlink files from specified job input paths and
 * takes the files / directories specified in those symlink files as
 * actual map-reduce input. The target input data should be in TextInputFormat.
 */
@SuppressWarnings("deprecation")
public class SymlinkTextInputFormat extends SymbolicInputFormat implements
    InputFormat<LongWritable, Text>, JobConfigurable,
    ContentSummaryInputFormat, ReworkMapredInputFormat {
  /**
   * This input split wraps the FileSplit generated from
   * TextInputFormat.getSplits(), while setting the original link file path
   * as job input path. This is needed because MapOperator relies on the
   * job input path to lookup correct child operators. The target data file
   * is encapsulated in the wrapped FileSplit.
   */
  public static class SymlinkTextInputSplit extends FileSplit {
    private final FileSplit split;

    public SymlinkTextInputSplit() {
      super((Path)null, 0, 0, (String[])null);
      split = new FileSplit((Path)null, 0, 0, (String[])null);
    }

    public SymlinkTextInputSplit(Path symlinkPath, FileSplit split) throws IOException {
      super(symlinkPath, 0, 0, split.getLocations());
      this.split = split;
    }

    /**
     * Gets the target split, i.e. the split of target data.
     */
    public FileSplit getTargetSplit() {
      return split;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      split.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      split.readFields(in);
    }
  }

  @Override
  public RecordReader<LongWritable, Text> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    InputSplit targetSplit = ((SymlinkTextInputSplit)split).getTargetSplit();

    // The target data is in TextInputFormat.
    TextInputFormat inputFormat = new TextInputFormat();
    inputFormat.configure(job);
    RecordReader innerReader = null;
    try {
      innerReader = inputFormat.getRecordReader(targetSplit, job,
          reporter);
    } catch (Exception e) {
      innerReader = HiveIOExceptionHandlerUtil
          .handleRecordReaderCreationException(e, job);
    }
    HiveRecordReader rr = new HiveRecordReader(innerReader, job);
    rr.initIOContext((FileSplit)targetSplit, job, TextInputFormat.class, innerReader);
    return rr;
  }
  
  /**
   * Parses all target paths from job input directory which contains symlink
   * files, and splits the target data using TextInputFormat.
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits)
      throws IOException {
    Path[] symlinksDirs = FileInputFormat.getInputPaths(job);
    if (symlinksDirs.length == 0) {
      throw new IOException("No input paths specified in job.");
    }

    // Get all target paths first, because the number of total target paths
    // is used to determine number of splits of each target path.
    List<Path> targetPaths = new ArrayList<Path>();
    List<Path> symlinkPaths = new ArrayList<Path>();
    try {
      getTargetPathsFromSymlinksDirs(
          job,
          symlinksDirs,
          targetPaths,
          symlinkPaths);
    } catch (Exception e) {
      throw new IOException(
          "Error parsing symlinks from specified job input path.", e);
    }
    if (targetPaths.size() == 0) {
      return new InputSplit[0];
    }

    // The input should be in TextInputFormat.
    TextInputFormat inputFormat = new TextInputFormat();
    JobConf newjob = new JobConf(job);
    newjob.setInputFormat(TextInputFormat.class);
    inputFormat.configure(newjob);

    List<InputSplit> result = new ArrayList<InputSplit>();

    // ceil(numSplits / numPaths), so we can get at least numSplits splits.
    int numPaths = targetPaths.size();
    int numSubSplits = (numSplits + numPaths - 1) / numPaths;

    // For each path, do getSplits().
    for (int i = 0; i < numPaths; ++i) {
      Path targetPath = targetPaths.get(i);
      Path symlinkPath = symlinkPaths.get(i);

      FileInputFormat.setInputPaths(newjob, targetPath);

      InputSplit[] iss = inputFormat.getSplits(newjob, numSubSplits);
      for (InputSplit is : iss) {
        result.add(new SymlinkTextInputSplit(symlinkPath, (FileSplit)is));
      }
    }
    return result.toArray(new InputSplit[result.size()]);
  }

  @Override
  public void configure(JobConf job) {
    // empty
  }

  /**
   * Given list of directories containing symlink files, read all target
   * paths from symlink files and return as targetPaths list. And for each
   * targetPaths[i], symlinkPaths[i] will be the path to the symlink file
   * containing the target path.
   */
  private static void getTargetPathsFromSymlinksDirs(
      Configuration conf, Path[] symlinksDirs,
      List<Path> targetPaths, List<Path> symlinkPaths) throws IOException {
    for (Path symlinkDir : symlinksDirs) {
      FileSystem fileSystem = symlinkDir.getFileSystem(conf);
      FileStatus[] symlinks = fileSystem.listStatus(symlinkDir, FileUtils.HIDDEN_FILES_PATH_FILTER);

      // Read paths from each symlink file.
      for (FileStatus symlink : symlinks) {
        BufferedReader reader = null;
        try {
          reader = new BufferedReader(
              new InputStreamReader(
                  fileSystem.open(symlink.getPath())));
          String line;
          while ((line = reader.readLine()) != null) {
            targetPaths.add(new Path(line));
            symlinkPaths.add(symlink.getPath());
          }
        } finally {
          org.apache.hadoop.io.IOUtils.closeStream(reader);
        }
      }
    }
  }

  @Override
  public ContentSummary getContentSummary(Path p, JobConf job)
      throws IOException {
    //length, file count, directory count
    long[] summary = {0, 0, 0};
    List<Path> targetPaths = new ArrayList<Path>();
    List<Path> symlinkPaths = new ArrayList<Path>();
    try {
      getTargetPathsFromSymlinksDirs(
          job,
          new Path[]{p},
          targetPaths,
          symlinkPaths);
    } catch (Exception e) {
      throw new IOException(
          "Error parsing symlinks from specified job input path.", e);
    }
    for(Path path : targetPaths) {
      FileSystem fs = path.getFileSystem(job);
      ContentSummary cs = fs.getContentSummary(path);
      summary[0] += cs.getLength();
      summary[1] += cs.getFileCount();
      summary[2] += cs.getDirectoryCount();
    }
    return new ContentSummary(summary[0], summary[1], summary[2]);
  }

}
