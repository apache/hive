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
package org.apache.hadoop.hive.shims;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DefaultFileAccess;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hive.io.HiveIOExceptionHandlerUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.Progressable;

/**
 * Base implemention for shims against secure Hadoop 0.20.3/0.23.
 */
public abstract class HadoopShimsSecure implements HadoopShims {

  static final Logger LOG = LoggerFactory.getLogger(HadoopShimsSecure.class);

  public static class InputSplitShim extends CombineFileSplit {
    long shrinkedLength;
    boolean _isShrinked;
    public InputSplitShim() {
      super();
      _isShrinked = false;
    }

    public InputSplitShim(JobConf conf, Path[] paths, long[] startOffsets,
      long[] lengths, String[] locations) throws IOException {
      super(conf, paths, startOffsets, lengths, dedup(locations));
      _isShrinked = false;
    }

    public void shrinkSplit(long length) {
      _isShrinked = true;
      shrinkedLength = length;
    }

    public boolean isShrinked() {
      return _isShrinked;
    }

    public long getShrinkedLength() {
      return shrinkedLength;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      _isShrinked = in.readBoolean();
      if (_isShrinked) {
        shrinkedLength = in.readLong();
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      out.writeBoolean(_isShrinked);
      if (_isShrinked) {
        out.writeLong(shrinkedLength);
      }
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
        Integer.class,
        RecordReader.class
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
    protected boolean isShrinked;
    protected long shrinkedLength;

    @Override
    public boolean next(K key, V value) throws IOException {

      while ((curReader == null)
          || !doNextWithExceptionHandler((K) ((CombineHiveKey) key).getKey(),
              value)) {
        if (!initNextRecordReader(key)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public K createKey() {
      K newKey = curReader.createKey();
      return (K)(new CombineHiveKey(newKey));
    }

    @Override
    public V createValue() {
      return curReader.createValue();
    }

    /**
     * Return the amount of data processed.
     */
    @Override
    public long getPos() throws IOException {
      return progress;
    }

    @Override
    public void close() throws IOException {
      if (curReader != null) {
        curReader.close();
        curReader = null;
      }
    }

    /**
     * Return progress based on the amount of data processed so far.
     */
    @Override
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

      isShrinked = false;

      assert (split instanceof InputSplitShim);
      if (((InputSplitShim) split).isShrinked()) {
        isShrinked = true;
        shrinkedLength = ((InputSplitShim) split).getShrinkedLength();
      }

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
     * do next and handle exception inside it.
     * @param key
     * @param value
     * @return
     * @throws IOException
     */
    private boolean doNextWithExceptionHandler(K key, V value) throws IOException {
      try {
        return curReader.next(key, value);
      } catch (Exception e) {
        return HiveIOExceptionHandlerUtil
            .handleRecordReaderNextException(e, jc);
      }
    }

    /**
     * Get the record reader for the next chunk in this CombineFileSplit.
     */
    protected boolean initNextRecordReader(K key) throws IOException {

      RecordReader preReader = curReader; //it is OK, curReader is closed, for we only need footer buffer info from preReader.
      if (curReader != null) {
        curReader.close();
        curReader = null;
        if (idx > 0) {
          progress += split.getLength(idx - 1); // done processing so far
        }
      }

      // if all chunks have been processed, nothing more to do.
      if (idx == split.getNumPaths() || (isShrinked && progress > shrinkedLength)) {
        return false;
      }

      // get a record reader for the idx-th chunk
      try {
        curReader = rrConstructor.newInstance(new Object[]
            {split, jc, reporter, Integer.valueOf(idx), preReader});

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
        curReader = HiveIOExceptionHandlerUtil.handleRecordReaderCreationException(
            e, jc);
      }
      idx++;
      return true;
    }
  }

  public abstract static class CombineFileInputFormatShim<K, V> extends
      CombineFileInputFormat<K, V>
      implements HadoopShims.CombineFileInputFormatShim<K, V> {

    @Override
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
    public CombineFileSplit[] getSplits(JobConf job, int numSplits) throws IOException {

      long minSize =
          job.getLong(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MINSIZE, 0);

      // For backward compatibility, let the above parameter be used
      if (job.getLong(
          org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat.SPLIT_MINSIZE_PERNODE,
          0) == 0) {
        super.setMinSplitSizeNode(minSize);
      }

      if (job.getLong(
          org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat.SPLIT_MINSIZE_PERRACK,
          0) == 0) {
        super.setMinSplitSizeRack(minSize);
      }

      if (job.getLong(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE,
          0) == 0) {
        super.setMaxSplitSize(minSize);
      }

      InputSplit[] splits = super.getSplits(job, numSplits);

      ArrayList<InputSplitShim> inputSplitShims = new ArrayList<InputSplitShim>();
      for (int pos = 0; pos < splits.length; pos++) {
        CombineFileSplit split = (CombineFileSplit) splits[pos];
        if (split.getPaths().length > 0) {
          inputSplitShims.add(new InputSplitShim(job, split.getPaths(),
            split.getStartOffsets(), split.getLengths(), split.getLocations()));
        }
      }
      return inputSplitShims.toArray(new InputSplitShim[inputSplitShims.size()]);
    }

    @Override
    public InputSplitShim getInputSplitShim() throws IOException {
      return new InputSplitShim();
    }

    @Override
    public RecordReader getRecordReader(JobConf job, CombineFileSplit split,
        Reporter reporter,
        Class<RecordReader<K, V>> rrClass)
        throws IOException {
      CombineFileSplit cfSplit = split;
      return new CombineFileRecordReader(job, cfSplit, reporter, rrClass);
    }
  }

  @Override
  abstract public JobTrackerState getJobTrackerState(ClusterStatus clusterStatus) throws Exception;

  @Override
  abstract public org.apache.hadoop.mapreduce.TaskAttemptContext newTaskAttemptContext(
      Configuration conf, final Progressable progressable);

  @Override
  abstract public org.apache.hadoop.mapreduce.JobContext newJobContext(Job job);

  @Override
  abstract public boolean isLocalMode(Configuration conf);

  @Override
  abstract public void setJobLauncherRpcAddress(Configuration conf, String val);

  @Override
  abstract public String getJobLauncherHttpAddress(Configuration conf);

  @Override
  abstract public String getJobLauncherRpcAddress(Configuration conf);

  @Override
  abstract public short getDefaultReplication(FileSystem fs, Path path);

  @Override
  abstract public long getDefaultBlockSize(FileSystem fs, Path path);

  @Override
  abstract public FileSystem createProxyFileSystem(FileSystem fs, URI uri);

  @Override
  abstract public FileSystem getNonCachedFileSystem(URI uri, Configuration conf) throws IOException;

  private static String[] dedup(String[] locations) throws IOException {
    Set<String> dedup = new HashSet<String>();
    Collections.addAll(dedup, locations);
    return dedup.toArray(new String[dedup.size()]);
  }

  @Override
  public void checkFileAccess(FileSystem fs, FileStatus stat, FsAction action)
      throws IOException, AccessControlException, Exception {
    DefaultFileAccess.checkFileAccess(fs, stat, action);
  }

  @Override
  abstract public void addDelegationTokens(FileSystem fs, Credentials cred, String uname) throws IOException;
}
