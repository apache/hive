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

import java.io.File;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.Map.Entry;
import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileInputFormat;

import org.apache.hadoop.hive.shims.HadoopShims.CombineFileInputFormatShim;
import org.apache.hadoop.hive.shims.HadoopShims.InputSplitShim;
import org.apache.hadoop.hive.shims.ShimLoader;

import org.apache.hadoop.util.ReflectionUtils;

/**
 * CombineHiveInputFormat is a parameterized InputFormat which looks at the path name and determine
 * the correct InputFormat for that path name from mapredPlan.pathToPartitionInfo().
 * It can be used to read files with different input format in the same map-reduce job.
 */
public class CombineHiveInputFormat<K extends WritableComparable,
                             V extends Writable> extends HiveInputFormat<K, V> {

  public static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.hive.ql.io.CombineHiveInputFormat");

  /**
   * CombineHiveInputSplit encapsulates an InputSplit with its corresponding inputFormatClassName.
   * A CombineHiveInputSplit comprises of multiple chunks from different files. Since, they belong
   * to a single directory, there is a single inputformat for all the chunks.
   */
  public static class CombineHiveInputSplit implements InputSplitShim {

    String           inputFormatClassName;
    InputSplitShim   inputSplitShim;

    public CombineHiveInputSplit() throws IOException {
      this(ShimLoader.getHadoopShims().getCombineFileInputFormat().getInputSplitShim());
    }

    public CombineHiveInputSplit(InputSplitShim inputSplitShim) throws IOException {
      this(inputSplitShim.getJob(), inputSplitShim);
    }

    public CombineHiveInputSplit(JobConf job, InputSplitShim inputSplitShim) throws IOException {
      this.inputSplitShim = inputSplitShim;
      if (job != null) {
        Map<String, partitionDesc> pathToPartitionInfo = 
          Utilities.getMapRedWork(job).getPathToPartitionInfo();

        // extract all the inputFormatClass names for each chunk in the CombinedSplit.
        Path[] ipaths = inputSplitShim.getPaths();
        for (int i = 0; i < ipaths.length; i++) {
          tableDesc table = null;
          try {
            table = getTableDescFromPath(pathToPartitionInfo, ipaths[i].getParent());
          } catch (IOException e) {
            // The file path may be present in case of sampling - so ignore that
            table = null;
          }

          if (table == null) {
            try {
              table = getTableDescFromPath(pathToPartitionInfo, ipaths[i]);
            } catch (IOException e) {
              LOG.warn("CombineHiveInputSplit unable to find table description for " +
                       ipaths[i].getParent());
              continue;
            }
          }
          
          // create a new InputFormat instance if this is the first time to see this class
          if (i == 0)
            inputFormatClassName = table.getInputFileFormatClass().getName();
          else
            assert inputFormatClassName.equals(table.getInputFileFormatClass().getName());
        }
      }
    }

    public InputSplitShim getInputSplitShim() {
      return inputSplitShim;
    }
    
    /**
     * Returns the inputFormat class name for the i-th chunk
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
    
    /** Returns an array containing the startoffsets of the files in the split*/ 
    public long[] getStartOffsets() {
      return inputSplitShim.getStartOffsets();
    }
    
    /** Returns an array containing the lengths of the files in the split*/ 
    public long[] getLengths() {
      return inputSplitShim.getLengths();
    }
    
    /** Returns the start offset of the i<sup>th</sup> Path */
    public long getOffset(int i) {
      return inputSplitShim.getOffset(i);
    }
    
    /** Returns the length of the i<sup>th</sup> Path */
    public long getLength(int i) {
      return inputSplitShim.getLength(i);
    }
    
    /** Returns the number of Paths in the split */
    public int getNumPaths() {
      return inputSplitShim.getNumPaths();
    }
    
    /** Returns the i<sup>th</sup> Path */
    public Path getPath(int i) {
      return inputSplitShim.getPath(i);
    }
    
    /** Returns all the Paths in the split */
    public Path[] getPaths() {
      return inputSplitShim.getPaths();
    }
    
    /** Returns all the Paths where this input-split resides */
    public String[] getLocations() throws IOException {
      return inputSplitShim.getLocations();
    }
    
    /**
     * Prints this obejct as a string.
     */
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append(inputSplitShim.toString());
      sb.append("InputFormatClass: " + inputFormatClassName);
      sb.append("\n");
      return sb.toString();
    }

    /**
     * Writable interface
     */
    public void readFields(DataInput in) throws IOException {
      inputSplitShim.readFields(in);
      inputFormatClassName = in.readUTF();
    }

    /**
     * Writable interface
     */
    public void write(DataOutput out) throws IOException {
      inputSplitShim.write(out);

      if (inputFormatClassName == null) {
        Map<String, partitionDesc> pathToPartitionInfo = 
          Utilities.getMapRedWork(getJob()).getPathToPartitionInfo();
        
        // extract all the inputFormatClass names for each chunk in the CombinedSplit.
        tableDesc table = null;
        try {
          table = getTableDescFromPath(pathToPartitionInfo, inputSplitShim.getPath(0).getParent());
        } catch (IOException e) {
          // The file path may be present in case of sampling - so ignore that
          table = null;
        }

        if (table == null)
          table = getTableDescFromPath(pathToPartitionInfo, inputSplitShim.getPath(0));

        // create a new InputFormat instance if this is the first time to see this class
        inputFormatClassName = table.getInputFileFormatClass().getName();
      }

      out.writeUTF(inputFormatClassName);
    }
  }

  /**
   * Create Hive splits based on CombineFileSplit
   */
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

    init(job);
    CombineFileInputFormatShim combine = ShimLoader.getHadoopShims().getCombineFileInputFormat();

    if (combine.getInputPathsShim(job).length == 0) {
      throw new IOException("No input paths specified in job");
    }
    ArrayList<InputSplit> result = new ArrayList<InputSplit>();

    // combine splits only from same tables. Do not combine splits from multiple tables.
    Path[] paths = combine.getInputPathsShim(job);
    for (int i = 0; i < paths.length; i++) {
      LOG.info("CombineHiveInputSplit creating pool for " + paths[i]);
      combine.createPool(job, new CombineFilter(paths[i]));
    }

    InputSplitShim[] iss = (InputSplitShim[])combine.getSplits(job, 1);
    for (InputSplitShim is: iss) {
      CombineHiveInputSplit csplit = new CombineHiveInputSplit(job, is);
      result.add(csplit);
    }
    
    LOG.info("number of splits " + result.size());

    return result.toArray(new CombineHiveInputSplit[result.size()]);
  }

  /**
   * Create a generic Hive RecordReader than can iterate over all chunks in 
   * a CombinedFileSplit
   */
  public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    CombineHiveInputSplit hsplit = (CombineHiveInputSplit)split;

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

    return 
      ShimLoader.getHadoopShims().getCombineFileInputFormat().getRecordReader(job, 
        ((CombineHiveInputSplit)split).getInputSplitShim(), 
        reporter, CombineHiveRecordReader.class);
  }

  static class CombineFilter implements PathFilter {
    private String pString;

    // store a path prefix in this TestFilter
    public CombineFilter(Path p) {
      pString = p.toString() + File.separator;
    }

    // returns true if the specified path matches the prefix stored
    // in this TestFilter.
    public boolean accept(Path path) {
      if (path.toString().indexOf(pString) == 0) {
        return true;
      }
      return false;
    }

    public String toString() {
      return "PathFilter:" + pString;
    }
  }
}
