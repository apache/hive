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
package org.apache.hadoop.hive.shims;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskCompletionEvent;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

/**
 * In order to be compatible with multiple versions of Hadoop, all parts
 * of the Hadoop interface that are not cross-version compatible are
 * encapsulated in an implementation of this class. Users should use
 * the ShimLoader class as a factory to obtain an implementation of
 * HadoopShims corresponding to the version of Hadoop currently on the
 * classpath.
 */
public interface HadoopShims {

  /**
   * Return true if the current version of Hadoop uses the JobShell for
   * command line interpretation.
   */
  public boolean usesJobShell();

  /**
   * Calls fs.deleteOnExit(path) if such a function exists.
   *
   * @return true if the call was successful
   */
  public boolean fileSystemDeleteOnExit(FileSystem fs, Path path) throws IOException;

  /**
   * Calls fmt.validateInput(conf) if such a function exists.
   */
  public void inputFormatValidateInput(InputFormat fmt, JobConf conf) throws IOException;

  /**
   * If JobClient.getCommandLineConfig exists, sets the given
   * property/value pair in that Configuration object.
   *
   * This applies for Hadoop 0.17 through 0.19
   */
  public void setTmpFiles(String prop, String files);
  
  /**
   * return the last access time of the given file.
   * @param file
   * @return last access time. -1 if not supported.
   */
  public long getAccessTime(FileStatus file);

  /**
   * Returns a shim to wrap MiniDFSCluster. This is necessary since this class
   * was moved from org.apache.hadoop.dfs to org.apache.hadoop.hdfs
   */
  public MiniDFSShim getMiniDfs(Configuration conf,
                                int numDataNodes,
                                boolean format,
                                String[] racks) throws IOException;

  /**
   * Shim around the functions in MiniDFSCluster that Hive uses.
   */
  public interface MiniDFSShim {
    public FileSystem getFileSystem() throws IOException;
    public void shutdown() throws IOException;
  }

  /**
   * We define this function here to make the code compatible between
   * hadoop 0.17 and hadoop 0.20.
   *
   * Hive binary that compiled Text.compareTo(Text) with hadoop 0.20 won't
   * work with hadoop 0.17 because in hadoop 0.20, Text.compareTo(Text) is
   * implemented in org.apache.hadoop.io.BinaryComparable, and Java compiler
   * references that class, which is not available in hadoop 0.17.
   */
  public int compareText(Text a, Text b);

  public CombineFileInputFormatShim getCombineFileInputFormat();

  public String getInputFormatClassName();
  
  /**
   * getTaskJobIDs returns an array of String with two elements. The first
   * element is a string representing the task id and the second is a string
   * representing the job id. This is necessary as TaskID and TaskAttemptID 
   * are not supported in Haddop 0.17
   */
  public String [] getTaskJobIDs(TaskCompletionEvent t);

  public interface InputSplitShim extends InputSplit {
    public JobConf getJob();
    public long getLength();

    /** Returns an array containing the startoffsets of the files in the split*/ 
    public long[] getStartOffsets();
    
    /** Returns an array containing the lengths of the files in the split*/ 
    public long[] getLengths();
    
    /** Returns the start offset of the i<sup>th</sup> Path */
    public long getOffset(int i);
    
    /** Returns the length of the i<sup>th</sup> Path */
    public long getLength(int i);
  
    /** Returns the number of Paths in the split */
    public int getNumPaths();

    /** Returns the i<sup>th</sup> Path */
    public Path getPath(int i);
    
    /** Returns all the Paths in the split */
    public Path[] getPaths();
    
    /** Returns all the Paths where this input-split resides */
    public String[] getLocations() throws IOException;

    public String toString();
    public void readFields(DataInput in) throws IOException;
    public void write(DataOutput out) throws IOException;
  }

  public interface CombineFileInputFormatShim<K, V> {
    public Path[] getInputPathsShim(JobConf conf);
    public void createPool(JobConf conf, PathFilter... filters);
    public InputSplitShim[] getSplits(JobConf job, int numSplits) throws IOException;
    public InputSplitShim getInputSplitShim() throws IOException;
    public RecordReader getRecordReader(JobConf job, InputSplitShim split, Reporter reporter, 
                                        Class<RecordReader<K, V>> rrClass) throws IOException;
  }
}
