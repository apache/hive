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

import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

/**
 * Implemention of shims against Hadoop 0.20.0.
 */
public class Hadoop20Shims implements HadoopShims {
  public boolean usesJobShell() {
    return false;
  }

  public boolean fileSystemDeleteOnExit(FileSystem fs, Path path)
      throws IOException {

    return fs.deleteOnExit(path);
  }

  public void inputFormatValidateInput(InputFormat fmt, JobConf conf)
      throws IOException {
    // gone in 0.18+
  }

  /**
   * Workaround for hadoop-17 - jobclient only looks at commandlineconfig.
   */
  public void setTmpFiles(String prop, String files) {
    // gone in 20+
  }

  public HadoopShims.MiniDFSShim getMiniDfs(Configuration conf,
      int numDataNodes,
      boolean format,
      String[] racks) throws IOException {
    return new MiniDFSShim(new MiniDFSCluster(conf, numDataNodes, format, racks));
  }

  /**
   * MiniDFSShim.
   *
   */
  public class MiniDFSShim implements HadoopShims.MiniDFSShim {
    private MiniDFSCluster cluster;

    public MiniDFSShim(MiniDFSCluster cluster) {
      this.cluster = cluster;
    }

    public FileSystem getFileSystem() throws IOException {
      return cluster.getFileSystem();
    }

    public void shutdown() {
      cluster.shutdown();
    }
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
  public int compareText(Text a, Text b) {
    return a.compareTo(b);
  }

  @Override
  public long getAccessTime(FileStatus file) {
    return file.getAccessTime();
  }

  public HadoopShims.CombineFileInputFormatShim getCombineFileInputFormat() {
    return new CombineFileInputFormatShim() {
      @Override
      public RecordReader getRecordReader(InputSplit split,
          JobConf job, Reporter reporter) throws IOException {
        throw new IOException("CombineFileInputFormat.getRecordReader not needed.");
      }
    };
  }

  public static class InputSplitShim extends CombineFileSplit implements HadoopShims.InputSplitShim {
    public InputSplitShim() {
      super();
    }

    public InputSplitShim(CombineFileSplit old) throws IOException {
      super(old);
    }
  }

  /* This class should be replaced with org.apache.hadoop.mapred.lib.CombineFileRecordReader class, once
   * https://issues.apache.org/jira/browse/MAPREDUCE-955 is fixed. This code should be removed - it is a copy
   * of org.apache.hadoop.mapred.lib.CombineFileRecordReader
   */
  public static class CombineFileRecordReader<K, V> implements RecordReader<K, V> {

    static final Class[] constructorSignature = new Class[] {
        InputSplit.class,
        Configuration.class,
        Reporter.class,
        Integer.class
        };

    protected CombineFileSplit split;
    protected JobConf jc;
    protected Reporter reporter;
    protected Class<RecordReader<K, V>> rrClass;
    protected Constructor<RecordReader<K, V>> rrConstructor;
    protected FileSystem fs;

    protected int idx;
    protected long progress;
    protected RecordReader<K, V> curReader;

    public boolean next(K key, V value) throws IOException {

      while ((curReader == null) || !curReader.next(key, value)) {
        if (!initNextRecordReader()) {
          return false;
        }
      }
      return true;
    }

    public K createKey() {
      return curReader.createKey();
    }

    public V createValue() {
      return curReader.createValue();
    }

    /**
     * Return the amount of data processed.
     */
    public long getPos() throws IOException {
      return progress;
    }

    public void close() throws IOException {
      if (curReader != null) {
        curReader.close();
        curReader = null;
      }
    }

    /**
     * Return progress based on the amount of data processed so far.
     */
    public float getProgress() throws IOException {
      return Math.min(1.0f, progress / (float) (split.getLength()));
    }

    /**
     * A generic RecordReader that can hand out different recordReaders
     * for each chunk in the CombineFileSplit.
     */
    public CombineFileRecordReader(JobConf job, CombineFileSplit split,
        Reporter reporter,
        Class<RecordReader<K, V>> rrClass)
        throws IOException {
      this.split = split;
      this.jc = job;
      this.rrClass = rrClass;
      this.reporter = reporter;
      this.idx = 0;
      this.curReader = null;
      this.progress = 0;

      try {
        rrConstructor = rrClass.getDeclaredConstructor(constructorSignature);
        rrConstructor.setAccessible(true);
      } catch (Exception e) {
        throw new RuntimeException(rrClass.getName() +
            " does not have valid constructor", e);
      }
      initNextRecordReader();
    }

    /**
     * Get the record reader for the next chunk in this CombineFileSplit.
     */
    protected boolean initNextRecordReader() throws IOException {

      if (curReader != null) {
        curReader.close();
        curReader = null;
        if (idx > 0) {
          progress += split.getLength(idx - 1); // done processing so far
        }
      }

      // if all chunks have been processed, nothing more to do.
      if (idx == split.getNumPaths()) {
        return false;
      }

      // get a record reader for the idx-th chunk
      try {
        curReader = rrConstructor.newInstance(new Object[]
            {split, jc, reporter, Integer.valueOf(idx)});

        // setup some helper config variables.
        jc.set("map.input.file", split.getPath(idx).toString());
        jc.setLong("map.input.start", split.getOffset(idx));
        jc.setLong("map.input.length", split.getLength(idx));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      idx++;
      return true;
    }
  }

  public abstract static class CombineFileInputFormatShim<K, V> extends
      CombineFileInputFormat<K, V>
      implements HadoopShims.CombineFileInputFormatShim<K, V> {

    public Path[] getInputPathsShim(JobConf conf) {
      Path[] paths;
      try {
        paths = FileInputFormat.getInputPaths(conf);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      Path[] newPaths = new Path[paths.length];
      // remove file:
      for (int pos = 0; pos < paths.length; pos++) {
        newPaths[pos] = new Path(paths[pos].toUri().getPath());
      }
      return newPaths;
    }

    @Override
    public void createPool(JobConf conf, PathFilter... filters) {
      super.createPool(conf, filters);
    }

    @Override
    public InputSplitShim[] getSplits(JobConf job, int numSplits) throws IOException {
      long minSize = job.getLong("mapred.min.split.size", 0);

      // For backward compatibility, let the above parameter be used
      if (job.getLong("mapred.min.split.size.per.node", 0) == 0) {
        super.setMinSplitSizeNode(minSize);
      }

      if (job.getLong("mapred.min.split.size.per.rack", 0) == 0) {
        super.setMinSplitSizeRack(minSize);
      }

      if (job.getLong("mapred.max.split.size", 0) == 0) {
        super.setMaxSplitSize(minSize);
      }

      CombineFileSplit[] splits = (CombineFileSplit[]) super.getSplits(job, numSplits);

      InputSplitShim[] isplits = new InputSplitShim[splits.length];
      for (int pos = 0; pos < splits.length; pos++) {
        isplits[pos] = new InputSplitShim(splits[pos]);
      }

      return isplits;
    }

    public InputSplitShim getInputSplitShim() throws IOException {
      return new InputSplitShim();
    }

    public RecordReader getRecordReader(JobConf job, HadoopShims.InputSplitShim split,
        Reporter reporter,
        Class<RecordReader<K, V>> rrClass)
        throws IOException {
      CombineFileSplit cfSplit = (CombineFileSplit) split;
      return new CombineFileRecordReader(job, cfSplit, reporter, rrClass);
    }

  }

  public String getInputFormatClassName() {
    return "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat";
  }

  String[] ret = new String[2];

  @Override
  public String[] getTaskJobIDs(TaskCompletionEvent t) {
    TaskID tid = t.getTaskAttemptId().getTaskID();
    ret[0] = tid.toString();
    ret[1] = tid.getJobID().toString();
    return ret;
  }

  public void setFloatConf(Configuration conf, String varName, float val) {
    conf.setFloat(varName, val);
  }
}
