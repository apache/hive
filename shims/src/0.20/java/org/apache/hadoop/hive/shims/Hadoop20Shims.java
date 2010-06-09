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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.hadoop.util.ToolRunner;

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

  public boolean isJobPreparing(RunningJob job) throws IOException {
    return job.getJobState() == JobStatus.PREP;
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
    private final MiniDFSCluster cluster;

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

  public static class CombineHiveKey implements WritableComparable {
    Object key;

    public CombineHiveKey(Object key) {
      this.key = key;
    }

    public Object getKey() {
      return key;
    }

    public void setKey(Object key) {
      this.key = key;
    }

    public void write(DataOutput out) throws IOException {
      throw new IOException("Method not supported");
    }

    public void readFields(DataInput in) throws IOException {
      throw new IOException("Method not supported");
    }

    public int compareTo(Object w) {
      assert false;
      return 0;
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

      while ((curReader == null) || !curReader.next((K)((CombineHiveKey)key).getKey(), value)) {
        if (!initNextRecordReader(key)) {
          return false;
        }
      }
      return true;
    }

    public K createKey() {
      K newKey = curReader.createKey();
      return (K)(new CombineHiveKey(newKey));
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
      initNextRecordReader(null);
    }

    /**
     * Get the record reader for the next chunk in this CombineFileSplit.
     */
    protected boolean initNextRecordReader(K key) throws IOException {

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

        // change the key if need be
        if (key != null) {
          K newKey = curReader.createKey();
          ((CombineHiveKey)key).setKey(newKey);
        }

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
      try {
        return FileInputFormat.getInputPaths(conf);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
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

  @Override
  public int createHadoopArchive(Configuration conf, Path sourceDir, Path destDir,
      String archiveName) throws Exception {

    HadoopArchives har = new HadoopArchives(conf);
    List<String> args = new ArrayList<String>();

    if (conf.get("hive.archive.har.parentdir.settable") == null) {
      throw new RuntimeException("hive.archive.har.parentdir.settable is not set");
    }
    boolean parentSettable =
      conf.getBoolean("hive.archive.har.parentdir.settable", false);

    if (parentSettable) {
      args.add("-archiveName");
      args.add(archiveName);
      args.add("-p");
      args.add(sourceDir.toString());
      args.add(destDir.toString());
    } else {
      args.add("-archiveName");
      args.add(archiveName);
      args.add(sourceDir.toString());
      args.add(destDir.toString());
    }

    return ToolRunner.run(har, args.toArray(new String[0]));
  }

  public static class NullOutputCommitter extends OutputCommitter {
    @Override
    public void setupJob(JobContext jobContext) { }
    @Override
    public void cleanupJob(JobContext jobContext) { }

    @Override
    public void setupTask(TaskAttemptContext taskContext) { }
    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) {
      return false;
    }
    @Override
    public void commitTask(TaskAttemptContext taskContext) { }
    @Override
    public void abortTask(TaskAttemptContext taskContext) { }
  }

  public void setNullOutputFormat(JobConf conf) {
    conf.setOutputFormat(NullOutputFormat.class);
    conf.setOutputCommitter(Hadoop20Shims.NullOutputCommitter.class);

    // option to bypass job setup and cleanup was introduced in hadoop-21 (MAPREDUCE-463)
    // but can be backported. So we disable setup/cleanup in all versions >= 0.19
    conf.setBoolean("mapred.committer.job.setup.cleanup.needed", false);
  }
}
