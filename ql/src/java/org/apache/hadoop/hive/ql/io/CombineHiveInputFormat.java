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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.shims.HadoopShims.CombineFileInputFormatShim;
import org.apache.hadoop.hive.shims.HadoopShims.InputSplitShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;




/**
 * CombineHiveInputFormat is a parameterized InputFormat which looks at the path
 * name and determine the correct InputFormat for that path name from
 * mapredPlan.pathToPartitionInfo(). It can be used to read files with different
 * input format in the same map-reduce job.
 */
public class CombineHiveInputFormat<K extends WritableComparable, V extends Writable>
    extends HiveInputFormat<K, V> {

  public static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.hive.ql.io.CombineHiveInputFormat");

  /**
   * CombineHiveInputSplit encapsulates an InputSplit with its corresponding
   * inputFormatClassName. A CombineHiveInputSplit comprises of multiple chunks
   * from different files. Since, they belong to a single directory, there is a
   * single inputformat for all the chunks.
   */
  public static class CombineHiveInputSplit implements InputSplitShim {

    String inputFormatClassName;
    InputSplitShim inputSplitShim;

    public CombineHiveInputSplit() throws IOException {
      this(ShimLoader.getHadoopShims().getCombineFileInputFormat()
          .getInputSplitShim());
    }

    public CombineHiveInputSplit(InputSplitShim inputSplitShim) throws IOException {
      this(inputSplitShim.getJob(), inputSplitShim);
    }

    public CombineHiveInputSplit(JobConf job, InputSplitShim inputSplitShim)
        throws IOException {
      this.inputSplitShim = inputSplitShim;
      if (job != null) {
        Map<String, PartitionDesc> pathToPartitionInfo = Utilities
            .getMapRedWork(job).getPathToPartitionInfo();

        // extract all the inputFormatClass names for each chunk in the
        // CombinedSplit.
        Path[] ipaths = inputSplitShim.getPaths();
        for (int i = 0; i < ipaths.length; i++) {
          PartitionDesc part = getPartitionDescFromPath(pathToPartitionInfo, ipaths[i]);

          // create a new InputFormat instance if this is the first time to see
          // this class
          if (i == 0) {
            inputFormatClassName = part.getInputFileFormatClass().getName();
          } else {
            assert inputFormatClassName.equals(part.getInputFileFormatClass()
                .getName());
          }
        }
      }
    }

    public InputSplitShim getInputSplitShim() {
      return inputSplitShim;
    }

    /**
     * Returns the inputFormat class name for the i-th chunk.
     */
    public String inputFormatClassName() {
      return inputFormatClassName;
    }

    public void setInputFormatClassName(String inputFormatClassName) {
      this.inputFormatClassName = inputFormatClassName;
    }

    public JobConf getJob() {
      return inputSplitShim.getJob();
    }

    public long getLength() {
      return inputSplitShim.getLength();
    }

    /** Returns an array containing the startoffsets of the files in the split. */
    public long[] getStartOffsets() {
      return inputSplitShim.getStartOffsets();
    }

    /** Returns an array containing the lengths of the files in the split. */
    public long[] getLengths() {
      return inputSplitShim.getLengths();
    }

    /** Returns the start offset of the i<sup>th</sup> Path. */
    public long getOffset(int i) {
      return inputSplitShim.getOffset(i);
    }

    /** Returns the length of the i<sup>th</sup> Path. */
    public long getLength(int i) {
      return inputSplitShim.getLength(i);
    }

    /** Returns the number of Paths in the split. */
    public int getNumPaths() {
      return inputSplitShim.getNumPaths();
    }

    /** Returns the i<sup>th</sup> Path. */
    public Path getPath(int i) {
      return inputSplitShim.getPath(i);
    }

    /** Returns all the Paths in the split. */
    public Path[] getPaths() {
      return inputSplitShim.getPaths();
    }

    /** Returns all the Paths where this input-split resides. */
    public String[] getLocations() throws IOException {
      return inputSplitShim.getLocations();
    }

    /**
     * Prints this obejct as a string.
     */
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(inputSplitShim.toString());
      sb.append("InputFormatClass: " + inputFormatClassName);
      sb.append("\n");
      return sb.toString();
    }

    /**
     * Writable interface.
     */
    public void readFields(DataInput in) throws IOException {
      inputSplitShim.readFields(in);
      inputFormatClassName = in.readUTF();
    }

    /**
     * Writable interface.
     */
    public void write(DataOutput out) throws IOException {
      inputSplitShim.write(out);

      if (inputFormatClassName == null) {
        Map<String, PartitionDesc> pathToPartitionInfo = Utilities
            .getMapRedWork(getJob()).getPathToPartitionInfo();

        // extract all the inputFormatClass names for each chunk in the
        // CombinedSplit.
        PartitionDesc part = getPartitionDescFromPath(pathToPartitionInfo,
            inputSplitShim.getPath(0));

        // create a new InputFormat instance if this is the first time to see
        // this class
        inputFormatClassName = part.getInputFileFormatClass().getName();
      }

      out.writeUTF(inputFormatClassName);
    }
  }

  /**
   * Create Hive splits based on CombineFileSplit.
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

    init(job);
    CombineFileInputFormatShim combine = ShimLoader.getHadoopShims()
        .getCombineFileInputFormat();

    if (combine == null) {
      return super.getSplits(job, numSplits);
    }

    if (combine.getInputPathsShim(job).length == 0) {
      throw new IOException("No input paths specified in job");
    }
    ArrayList<InputSplit> result = new ArrayList<InputSplit>();

    // combine splits only from same tables and same partitions. Do not combine splits from multiple
    // tables or multiple partitions.
    Path[] paths = combine.getInputPathsShim(job);
    for (Path path : paths) {
      LOG.info("CombineHiveInputSplit creating pool for " + path);

      PartitionDesc part = getPartitionDescFromPath(pathToPartitionInfo, path);
      TableDesc tableDesc = part.getTableDesc();
      if ((tableDesc != null) && tableDesc.isNonNative()) {
        return super.getSplits(job, numSplits);
      }

      // Use HiveInputFormat if any of the paths is not splittable
      Class inputFormatClass = part.getInputFileFormatClass();
      InputFormat inputFormat = getInputFormatFromCache(inputFormatClass, job);

      // Since there is no easy way of knowing whether MAPREDUCE-1597 is present in the tree or not,
      // we use a configuration variable for the same
      if (this.mrwork != null && !this.mrwork.getHadoopSupportsSplittable()) {
        // The following code should be removed, once
        // https://issues.apache.org/jira/browse/MAPREDUCE-1597 is fixed.
        // Hadoop does not handle non-splittable files correctly for CombineFileInputFormat,
        // so don't use CombineFileInputFormat for non-splittable files
        FileSystem inpFs = path.getFileSystem(job);

        if (inputFormat instanceof TextInputFormat) {
          Queue<Path> dirs = new LinkedList<Path>();
          FileStatus fStats = inpFs.getFileStatus(path);

          // If path is a directory
          if (fStats.isDir()) {
            dirs.offer(path);
          }
          else if ((new CompressionCodecFactory(job)).getCodec(path) != null) {
            return super.getSplits(job, numSplits);
          }

          while (dirs.peek() != null) {
            Path tstPath = dirs.remove();
            FileStatus[] fStatus = inpFs.listStatus(tstPath);
            for (int idx = 0; idx < fStatus.length; idx++) {
              if (fStatus[idx].isDir()) {
                dirs.offer(fStatus[idx].getPath());
              }
              else if ((new CompressionCodecFactory(job)).getCodec(fStatus[idx].getPath()) != null) {
                return super.getSplits(job, numSplits);
              }
            }
          }
        }
      }

      if (inputFormat instanceof SymlinkTextInputFormat) {
        return super.getSplits(job, numSplits);
      }

      combine.createPool(job, new CombineFilter(path));
    }
    InputSplitShim[] iss = combine.getSplits(job, 1);
    for (InputSplitShim is : iss) {
      CombineHiveInputSplit csplit = new CombineHiveInputSplit(job, is);
      result.add(csplit);
    }

    LOG.info("number of splits " + result.size());

    return result.toArray(new CombineHiveInputSplit[result.size()]);
  }

  /**
   * Create a generic Hive RecordReader than can iterate over all chunks in a
   * CombinedFileSplit.
   */
  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    if (!(split instanceof CombineHiveInputSplit)) {
      return super.getRecordReader(split, job, reporter);
    }

    CombineHiveInputSplit hsplit = (CombineHiveInputSplit) split;

    String inputFormatClassName = null;
    Class inputFormatClass = null;
    try {
      inputFormatClassName = hsplit.inputFormatClassName();
      inputFormatClass = job.getClassByName(inputFormatClassName);
    } catch (Exception e) {
      throw new IOException("cannot find class " + inputFormatClassName);
    }

    initColumnsNeeded(job, inputFormatClass, hsplit.getPath(0).toString(),
        hsplit.getPath(0).toUri().getPath());

    return ShimLoader.getHadoopShims().getCombineFileInputFormat()
        .getRecordReader(job,
        ((CombineHiveInputSplit) split).getInputSplitShim(), reporter,
        CombineHiveRecordReader.class);
  }

  protected static PartitionDesc getPartitionDescFromPath(
      Map<String, PartitionDesc> pathToPartitionInfo, Path dir) throws IOException {

    // We first do exact match, and then do prefix matching. The latter is due to input dir
    // could be /dir/ds='2001-02-21'/part-03 where part-03 is not part of partition
    String dirPath = dir.toUri().getPath();
    PartitionDesc part = pathToPartitionInfo.get(dir.toString());
    if (part == null) {
      LOG.warn("exact match not found, try ripping input path's theme and authority");
      part = pathToPartitionInfo.get(dirPath);
    }
    if (part == null) {

      LOG.warn("still does not found just the path part: " + dirPath + " in pathToPartitionInfo."
          + " Will try prefix matching");
      for (Map.Entry<String, PartitionDesc> entry : pathToPartitionInfo.entrySet()) {
        String keyPath = entry.getKey();
        String dirStr = dir.toString();
        // keyPath could start with hdfs:// or not, so we need to match both cases.
        if (dirStr.startsWith(keyPath)) {
          part = entry.getValue();
          break;
        } else {
          Path p = new Path(entry.getKey());
          String newP = p.toUri().getPath().toString();
          if (dirStr.startsWith(newP)) {
            part = entry.getValue();
            break;
          }
        }
      }
    }
    if (part != null) {
      return part;
    } else {
      throw new IOException("cannot find dir = " + dir.toString()
                          + " in partToPartitionInfo: " + pathToPartitionInfo.keySet());
    }
  }

  static class CombineFilter implements PathFilter {
    private final String pString;

    // store a path prefix in this TestFilter
    public CombineFilter(Path p) {
      // we need to keep the path part only because the Hadoop CombineFileInputFormat will
      // pass the path part only to accept().
      pString = p.toUri().getPath().toString() + File.separator;
    }

    // returns true if the specified path matches the prefix stored
    // in this TestFilter.
    public boolean accept(Path path) {
      if (path.toString().indexOf(pString) == 0) {
        return true;
      }
      return false;
    }

    @Override
    public String toString() {
      return "PathFilter:" + pString;
    }
  }
}
