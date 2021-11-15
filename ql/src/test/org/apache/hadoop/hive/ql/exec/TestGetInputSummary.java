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

package org.apache.hadoop.hive.ql.exec;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.io.ContentSummaryInputFormat;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InputEstimator;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestGetInputSummary {

  private static final String TEST_TABLE_NAME = "testTable";
  private static final Path TEST_TABLE_PATH = new Path(TEST_TABLE_NAME);

  private JobConf jobConf;
  private Properties properties;

  @Before
  public void setup() throws Exception {
    // creates scratch directories needed by the Context object
    SessionState.start(new HiveConf());

    this.jobConf = new JobConf();
    this.properties = new Properties();

    final FileSystem fs = FileSystem.getLocal(jobConf);
    fs.delete(TEST_TABLE_PATH, true);
    fs.mkdirs(TEST_TABLE_PATH);
  }

  @After
  public void teardown() throws Exception {
    final FileSystem fs = FileSystem.getLocal(jobConf);
    fs.delete(TEST_TABLE_PATH, true);
  }

  @Test
  public void testGetInputSummaryPoolWithCache() throws Exception {
    final int BYTES_PER_FILE = 5;

    final Collection<Path> testPaths = Arrays.asList(new Path("p1/test.txt"),
        new Path("p2/test.txt"), new Path("p3/test.txt"),
        new Path("p4/test.txt"), new Path("p5/test.txt"));

    ContentSummary cs = new ContentSummary.Builder().directoryCount(10L)
        .fileCount(10L).length(10L).build();

    Map<Path, ContentSummary> cache = new LinkedHashMap<>();
    cache.put(new Path("p2"), cs);

    jobConf.setInt(
        HiveConf.ConfVars.HIVE_EXEC_INPUT_LISTING_MAX_THREADS.varname, 0);

    ContentSummary summary = runTestGetInputSummary(jobConf, properties,
        testPaths, BYTES_PER_FILE, HiveInputFormat.class, cache);

    // The partition paths all contain a single file with 5 bytes of length,
    // however, one entry was added to the cache which specifies that the
    // partition has 10 directories and 10 files and these values should
    // override the real values since the cache is consulted before looking at
    // the actual file system.
    final long expectedLength = ((testPaths.size() - 1) * BYTES_PER_FILE) + 10L;
    final long expectedFileCount = (testPaths.size() - 1) + 10L;
    final long expectedDirCount = (testPaths.size() - 1) + 10L;

    assertEquals(expectedLength, summary.getLength());
    assertEquals(expectedFileCount, summary.getFileCount());
    assertEquals(expectedDirCount, summary.getDirectoryCount());
  }

  /**
   * Read several files so that their information is cached, then delete those
   * files and read them again to see if the results were cached from the first
   * read. If the cache is not working, the sizes will be off since the files no
   * longer exist in the file system.
   *
   * @throws Exception e
   */
  @Test
  public void testGetInputSummaryPoolWithCacheReuse() throws Exception {
    final int BYTES_PER_FILE = 5;

    final Collection<Path> testPaths1 = Arrays.asList(new Path("p1/test.txt"),
        new Path("p2/test.txt"), new Path("p3/test.txt"));
    final Collection<Path> testPaths2 =
        Arrays.asList(new Path("p4/test.txt"), new Path("p5/test.txt"));

    jobConf.setInt(
        HiveConf.ConfVars.HIVE_EXEC_INPUT_LISTING_MAX_THREADS.varname, 0);

    ContentSummary summary =
        runTestGetInputSummary(jobConf, properties, testPaths1, BYTES_PER_FILE,
            HiveInputFormat.class, Collections.emptyMap());

    // Ensure the first group of files were read correctly
    assertEquals(testPaths1.size() * BYTES_PER_FILE, summary.getLength());
    assertEquals(testPaths1.size(), summary.getFileCount());
    assertEquals(testPaths1.size(), summary.getDirectoryCount());

    // Delete the files from the first group
    final FileSystem fs = FileSystem.getLocal(jobConf);
    for (final Path path : testPaths1) {
      fs.delete(path, true);
    }

    // Read all the files and the first group's stats should be pulled from
    // cache and the second group from the file system
    summary = runTestGetInputSummary(jobConf, properties,
        CollectionUtils.union(testPaths1, testPaths2), BYTES_PER_FILE,
        new HashSet<>(testPaths1), HiveInputFormat.class,
        Collections.emptyMap());

    assertEquals((testPaths1.size() + testPaths2.size()) * BYTES_PER_FILE,
        summary.getLength());
    assertEquals((testPaths1.size() + testPaths2.size()),
        summary.getFileCount());
    assertEquals((testPaths1.size() + testPaths2.size()),
        summary.getDirectoryCount());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testGetInputSummaryWithMultipleThreads() throws IOException {
    final int BYTES_PER_FILE = 5;

    final Collection<Path> testPaths = Arrays.asList(new Path("p1/test.txt"),
        new Path("p2/test.txt"), new Path("p3/test.txt"),
        new Path("p4/test.txt"), new Path("p5/test.txt"));

    jobConf.setInt(
        HiveConf.ConfVars.HIVE_EXEC_INPUT_LISTING_MAX_THREADS.varname, 2);
    ContentSummary summary =
        runTestGetInputSummary(jobConf, properties, testPaths, BYTES_PER_FILE,
            HiveInputFormat.class, Collections.emptyMap());
    assertEquals(testPaths.size() * BYTES_PER_FILE, summary.getLength());
    assertEquals(testPaths.size(), summary.getFileCount());
    assertEquals(testPaths.size(), summary.getDirectoryCount());

    // Test deprecated mapred.dfsclient.parallelism.max
    jobConf.setInt(
        HiveConf.ConfVars.HIVE_EXEC_INPUT_LISTING_MAX_THREADS.varname, 0);
    jobConf.setInt(Utilities.DEPRECATED_MAPRED_DFSCLIENT_PARALLELISM_MAX, 2);
    summary = runTestGetInputSummary(jobConf, properties, testPaths,
        BYTES_PER_FILE, HiveInputFormat.class, Collections.emptyMap());
    assertEquals(testPaths.size() * BYTES_PER_FILE, summary.getLength());
    assertEquals(testPaths.size(), summary.getFileCount());
    assertEquals(testPaths.size(), summary.getDirectoryCount());
  }

  @Test
  public void testGetInputSummaryWithInputEstimator()
      throws IOException, HiveException {
    final int BYTES_PER_FILE = 10;
    final int NUM_OF_ROWS = 5;

    final Collection<Path> testPaths = Arrays.asList(new Path("p1/test.txt"),
        new Path("p2/test.txt"), new Path("p3/test.txt"),
        new Path("p4/test.txt"), new Path("p5/test.txt"));

    jobConf.setInt(
        HiveConf.ConfVars.HIVE_EXEC_INPUT_LISTING_MAX_THREADS.varname, 2);

    properties.setProperty(hive_metastoreConstants.META_TABLE_STORAGE,
        InputEstimatorTestClass.class.getName());
    InputEstimatorTestClass.setEstimation(
        new InputEstimator.Estimation(NUM_OF_ROWS, BYTES_PER_FILE));

    /*
     * Let's write more bytes to the files to test that Estimator is actually
     * working returning the file size not from the filesystem
     */
    ContentSummary summary =
        runTestGetInputSummary(jobConf, properties, testPaths,
            BYTES_PER_FILE * 2, HiveInputFormat.class, Collections.emptyMap());
    assertEquals(testPaths.size() * BYTES_PER_FILE, summary.getLength());

    // Current getInputSummary() returns -1 for each file found
    assertEquals(testPaths.size() * -1, summary.getFileCount());

    // Current getInputSummary() returns -1 for each file found
    assertEquals(testPaths.size() * -1, summary.getDirectoryCount());

    // Test deprecated mapred.dfsclient.parallelism.max
    jobConf.setInt(
        HiveConf.ConfVars.HIVE_EXEC_INPUT_LISTING_MAX_THREADS.varname, 0);
    jobConf.setInt(
        HiveConf.ConfVars.HIVE_EXEC_INPUT_LISTING_MAX_THREADS.varname, 2);

    properties.setProperty(hive_metastoreConstants.META_TABLE_STORAGE,
        InputEstimatorTestClass.class.getName());
    InputEstimatorTestClass.setEstimation(
        new InputEstimator.Estimation(NUM_OF_ROWS, BYTES_PER_FILE));

    /*
     * Let's write more bytes to the files to test that Estimator is actually
     * working returning the file size not from the filesystem
     */
    summary = runTestGetInputSummary(jobConf, properties, testPaths,
        BYTES_PER_FILE * 2, HiveInputFormat.class, Collections.emptyMap());
    assertEquals(testPaths.size() * BYTES_PER_FILE, summary.getLength());

    // Current getInputSummary() returns -1 for each file found
    assertEquals(testPaths.size() * -1, summary.getFileCount());

    // Current getInputSummary() returns -1 for each file found
    assertEquals(testPaths.size() * -1, summary.getDirectoryCount());
  }

  @Test
  public void testGetInputSummaryWithASingleThread() throws IOException {
    final int BYTES_PER_FILE = 5;

    // Set to zero threads to disable thread pool
    jobConf.setInt(
        HiveConf.ConfVars.HIVE_EXEC_INPUT_LISTING_MAX_THREADS.varname, 0);

    final Collection<Path> testPaths = Arrays.asList(new Path("p1/test.txt"),
        new Path("p2/test.txt"), new Path("p3/test.txt"),
        new Path("p4/test.txt"), new Path("p5/test.txt"));

    ContentSummary summary =
        runTestGetInputSummary(jobConf, properties, testPaths, BYTES_PER_FILE,
            HiveInputFormat.class, Collections.emptyMap());
    assertEquals(testPaths.size() * BYTES_PER_FILE, summary.getLength());
    assertEquals(testPaths.size(), summary.getFileCount());
    assertEquals(testPaths.size(), summary.getDirectoryCount());
  }

  @Test
  public void testGetInputSummaryWithContentSummaryInputFormat()
      throws IOException {
    final int BYTES_PER_FILE = 10;

    final Collection<Path> testPaths = Arrays.asList(new Path("p1/test.txt"),
        new Path("p2/test.txt"), new Path("p3/test.txt"),
        new Path("p4/test.txt"), new Path("p5/test.txt"));

    jobConf.setInt(ConfVars.HIVE_EXEC_INPUT_LISTING_MAX_THREADS.varname, 2);

    ContentSummaryInputFormatTestClass
        .setContentSummary(new ContentSummary.Builder().length(BYTES_PER_FILE)
            .fileCount(2).directoryCount(1).build());

    /*
     * Write more bytes to the files to test that ContentSummaryInputFormat is
     * actually working returning the file size not from the filesystem
     */
    ContentSummary summary = runTestGetInputSummary(jobConf, properties,
        testPaths, BYTES_PER_FILE * 2, ContentSummaryInputFormatTestClass.class,
        Collections.emptyMap());
    assertEquals(testPaths.size() * BYTES_PER_FILE, summary.getLength());
    assertEquals(testPaths.size() * 2, summary.getFileCount());
    assertEquals(testPaths.size(), summary.getDirectoryCount());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetInputSummaryPool()
      throws ExecutionException, InterruptedException, IOException {
    ExecutorService pool = mock(ExecutorService.class);
    when(pool.submit(any(Runnable.class))).thenReturn(mock(Future.class));

    Set<Path> pathNeedProcess = new HashSet<>();
    pathNeedProcess.add(new Path("dummy-path1"));
    pathNeedProcess.add(new Path("dummy-path2"));
    pathNeedProcess.add(new Path("dummy-path3"));

    Context context = new Context(jobConf);

    Utilities.getInputSummaryWithPool(context, pathNeedProcess,
        mock(MapWork.class), new long[3], pool);
    verify(pool, times(3)).submit(any(Runnable.class));
    verify(pool).shutdown();
    verify(pool).shutdownNow();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetInputSummaryPoolAndFailure()
      throws ExecutionException, InterruptedException, IOException {
    ExecutorService pool = mock(ExecutorService.class);
    when(pool.submit(any(Runnable.class))).thenReturn(mock(Future.class));

    Set<Path> pathNeedProcess = new HashSet<>();
    pathNeedProcess.add(new Path("dummy-path1"));
    pathNeedProcess.add(new Path("dummy-path2"));
    pathNeedProcess.add(new Path("dummy-path3"));

    Context context = new Context(jobConf);

    Utilities.getInputSummaryWithPool(context, pathNeedProcess,
        mock(MapWork.class), new long[3], pool);
    verify(pool, times(3)).submit(any(Runnable.class));
    verify(pool).shutdown();
    verify(pool).shutdownNow();
  }

  @SuppressWarnings("rawtypes")
  private ContentSummary runTestGetInputSummary(JobConf jobConf,
      Properties properties, Collection<Path> testPaths, int bytesPerFile,
      Class<? extends InputFormat> inputFormatClass,
      Map<Path, ContentSummary> cache) throws IOException {
    return runTestGetInputSummary(jobConf, properties, testPaths, bytesPerFile,
        Collections.emptyList(), inputFormatClass, cache);
  }

  @SuppressWarnings("rawtypes")
  private ContentSummary runTestGetInputSummary(JobConf jobConf,
      Properties properties, Collection<Path> testPaths, int bytesPerFile,
      Collection<Path> providedPaths,
      Class<? extends InputFormat> inputFormatClass,
      Map<Path, ContentSummary> cache) throws IOException {

    final FileSystem fs = FileSystem.getLocal(jobConf);

    MapWork mapWork = new MapWork();
    Context context = new Context(jobConf);

    for (Map.Entry<Path, ContentSummary> entry : cache.entrySet()) {
      final Path partitionPath = new Path(TEST_TABLE_PATH, entry.getKey());
      context.addCS(partitionPath.toString(), entry.getValue());
    }

    Map<Path, PartitionDesc> pathToPartitionInfo = new LinkedHashMap<>();
    Map<Path, List<String>> pathToAliasTable = new LinkedHashMap<>();
    TableScanOperator scanOp = new TableScanOperator();

    PartitionDesc partitionDesc = new PartitionDesc(
        new TableDesc(inputFormatClass, null, properties), null);

    for (final Path path : testPaths) {
      final Path fullPath = new Path(TEST_TABLE_PATH, path);
      final Path partitionPath = fullPath.getParent();

      // If it is not provided by the test case, create a dummy file
      if (!providedPaths.contains(path)) {
        final byte[] data = new byte[bytesPerFile];

        fs.mkdirs(partitionPath);

        FSDataOutputStream out = fs.create(fullPath);
        out.write(data);
        out.close();
      }

      pathToPartitionInfo.put(partitionPath, partitionDesc);
      pathToAliasTable.put(partitionPath,
          Lists.newArrayList(partitionPath.getName()));
      mapWork.getAliasToWork().put(partitionPath.getName(), scanOp);
    }

    mapWork.setPathToAliases(pathToAliasTable);
    mapWork.setPathToPartitionInfo(pathToPartitionInfo);

    return Utilities.getInputSummary(context, mapWork, null);
  }

  @SuppressWarnings("rawtypes")
  static class ContentSummaryInputFormatTestClass extends FileInputFormat
      implements ContentSummaryInputFormat {
    private static ContentSummary summary =
        new ContentSummary.Builder().build();

    public static void setContentSummary(ContentSummary contentSummary) {
      summary = contentSummary;
    }

    @Override
    public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf,
        Reporter reporter) throws IOException {
      return null;
    }

    @Override
    public ContentSummary getContentSummary(Path p, JobConf job)
        throws IOException {
      return summary;
    }
  }
}
