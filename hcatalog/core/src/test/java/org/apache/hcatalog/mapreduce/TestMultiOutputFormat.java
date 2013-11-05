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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hcatalog.mapreduce;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hcatalog.mapreduce.MultiOutputFormat.JobConfigurer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.mapreduce.TestMultiOutputFormat} instead
 */
public class TestMultiOutputFormat {

  private static final Logger LOG = LoggerFactory.getLogger(TestMultiOutputFormat.class);
  private static File workDir;
  private static Configuration mrConf = null;
  private static FileSystem fs = null;
  private static MiniMRCluster mrCluster = null;

  @BeforeClass
  public static void setup() throws IOException {
    createWorkDir();
    Configuration conf = new Configuration(true);
    conf.set("yarn.scheduler.capacity.root.queues", "default");
    conf.set("yarn.scheduler.capacity.root.default.capacity", "100");

    fs = FileSystem.get(conf);
    System.setProperty("hadoop.log.dir", new File(workDir, "/logs").getAbsolutePath());
    // LocalJobRunner does not work with mapreduce OutputCommitter. So need
    // to use MiniMRCluster. MAPREDUCE-2350
    mrCluster = new MiniMRCluster(1, fs.getUri().toString(), 1, null, null,
      new JobConf(conf));
    mrConf = mrCluster.createJobConf();
  }

  private static void createWorkDir() throws IOException {
    String testDir = System.getProperty("test.tmp.dir", "./");
    testDir = testDir + "/test_multiout_" + Math.abs(new Random().nextLong()) + "/";
    workDir = new File(new File(testDir).getCanonicalPath());
    FileUtil.fullyDelete(workDir);
    workDir.mkdirs();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (mrCluster != null) {
      mrCluster.shutdown();
    }
    FileUtil.fullyDelete(workDir);
  }

  /**
   * A test job that reads a input file and outputs each word and the index of
   * the word encountered to a text file and sequence file with different key
   * values.
   */
  @Test
  public void testMultiOutputFormatWithoutReduce() throws Throwable {
    Job job = new Job(mrConf, "MultiOutNoReduce");
    job.setMapperClass(MultiOutWordIndexMapper.class);
    job.setJarByClass(this.getClass());
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(MultiOutputFormat.class);
    job.setNumReduceTasks(0);

    JobConfigurer configurer = MultiOutputFormat.createConfigurer(job);
    configurer.addOutputFormat("out1", TextOutputFormat.class, IntWritable.class, Text.class);
    configurer.addOutputFormat("out2", SequenceFileOutputFormat.class, Text.class,
      IntWritable.class);
    Path outDir = new Path(workDir.getPath(), job.getJobName());
    FileOutputFormat.setOutputPath(configurer.getJob("out1"), new Path(outDir, "out1"));
    FileOutputFormat.setOutputPath(configurer.getJob("out2"), new Path(outDir, "out2"));

    String fileContent = "Hello World";
    String inputFile = createInputFile(fileContent);
    FileInputFormat.setInputPaths(job, new Path(inputFile));

    //Test for merging of configs
    DistributedCache.addFileToClassPath(new Path(inputFile), job.getConfiguration(), fs);
    String dummyFile = createInputFile("dummy file");
    DistributedCache.addFileToClassPath(new Path(dummyFile), configurer.getJob("out1")
      .getConfiguration(), fs);
    // duplicate of the value. Merging should remove duplicates
    DistributedCache.addFileToClassPath(new Path(inputFile), configurer.getJob("out2")
      .getConfiguration(), fs);

    configurer.configure();

    // Verify if the configs are merged
    Path[] fileClassPaths = DistributedCache.getFileClassPaths(job.getConfiguration());
    List<Path> fileClassPathsList = Arrays.asList(fileClassPaths);
    Assert.assertTrue(fileClassPathsList.contains(new Path(inputFile)));
    Assert.assertTrue(fileClassPathsList.contains(new Path(dummyFile)));

    URI[] cacheFiles = DistributedCache.getCacheFiles(job.getConfiguration());
    List<URI> cacheFilesList = Arrays.asList(cacheFiles);
    Assert.assertTrue(cacheFilesList.contains(new Path(inputFile).makeQualified(fs).toUri()));
    Assert.assertTrue(cacheFilesList.contains(new Path(dummyFile).makeQualified(fs).toUri()));

    Assert.assertTrue(job.waitForCompletion(true));

    Path textOutPath = new Path(outDir, "out1/part-m-00000");
    String[] textOutput = readFully(textOutPath).split("\n");
    Path seqOutPath = new Path(outDir, "out2/part-m-00000");
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, seqOutPath, mrConf);
    Text key = new Text();
    IntWritable value = new IntWritable();
    String[] words = fileContent.split(" ");
    Assert.assertEquals(words.length, textOutput.length);
    LOG.info("Verifying file contents");
    for (int i = 0; i < words.length; i++) {
      Assert.assertEquals((i + 1) + "\t" + words[i], textOutput[i]);
      reader.next(key, value);
      Assert.assertEquals(words[i], key.toString());
      Assert.assertEquals((i + 1), value.get());
    }
    Assert.assertFalse(reader.next(key, value));
  }

  /**
   * A word count test job that reads a input file and outputs the count of
   * words to a text file and sequence file with different key values.
   */
  @Test
  public void testMultiOutputFormatWithReduce() throws Throwable {
    Job job = new Job(mrConf, "MultiOutWithReduce");

    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(MultiOutWordCountReducer.class);
    job.setJarByClass(this.getClass());
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(MultiOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    JobConfigurer configurer = MultiOutputFormat.createConfigurer(job);

    configurer.addOutputFormat("out1", TextOutputFormat.class, IntWritable.class, Text.class);
    configurer.addOutputFormat("out2", SequenceFileOutputFormat.class, Text.class,
      IntWritable.class);
    configurer.addOutputFormat("out3", NullOutputFormat.class, Text.class,
      IntWritable.class);
    Path outDir = new Path(workDir.getPath(), job.getJobName());
    FileOutputFormat.setOutputPath(configurer.getJob("out1"), new Path(outDir, "out1"));
    FileOutputFormat.setOutputPath(configurer.getJob("out2"), new Path(outDir, "out2"));

    configurer.configure();

    String fileContent = "Hello World Hello World World";
    String inputFile = createInputFile(fileContent);
    FileInputFormat.setInputPaths(job, new Path(inputFile));

    Assert.assertTrue(job.waitForCompletion(true));

    Path textOutPath = new Path(outDir, "out1/part-r-00000");
    String[] textOutput = readFully(textOutPath).split("\n");
    Path seqOutPath = new Path(outDir, "out2/part-r-00000");
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, seqOutPath, mrConf);
    Text key = new Text();
    IntWritable value = new IntWritable();
    String[] words = "Hello World".split(" ");
    Assert.assertEquals(words.length, textOutput.length);
    for (int i = 0; i < words.length; i++) {
      Assert.assertEquals((i + 2) + "\t" + words[i], textOutput[i]);
      reader.next(key, value);
      Assert.assertEquals(words[i], key.toString());
      Assert.assertEquals((i + 2), value.get());
    }
    Assert.assertFalse(reader.next(key, value));

  }


  /**
   * Create a file for map input
   *
   * @return absolute path of the file.
   * @throws IOException if any error encountered
   */
  private String createInputFile(String content) throws IOException {
    File f = File.createTempFile("input", "txt");
    FileWriter writer = new FileWriter(f);
    writer.write(content);
    writer.close();
    return f.getAbsolutePath();
  }

  private String readFully(Path file) throws IOException {
    FSDataInputStream in = fs.open(file);
    byte[] b = new byte[in.available()];
    in.readFully(b);
    in.close();
    return new String(b);
  }

  private static class MultiOutWordIndexMapper extends
    Mapper<LongWritable, Text, Writable, Writable> {

    private IntWritable index = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        MultiOutputFormat.write("out1", index, word, context);
        MultiOutputFormat.write("out2", word, index, context);
        index.set(index.get() + 1);
      }
    }
  }

  private static class WordCountMapper extends
    Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  private static class MultiOutWordCountReducer extends
    Reducer<Text, IntWritable, Writable, Writable> {

    private IntWritable count = new IntWritable();

    @Override
    protected void reduce(Text word, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      count.set(sum);
      MultiOutputFormat.write("out1", count, word, context);
      MultiOutputFormat.write("out2", word, count, context);
      MultiOutputFormat.write("out3", word, count, context);
    }
  }

  private static class NullOutputFormat<K, V> extends
    org.apache.hadoop.mapreduce.lib.output.NullOutputFormat<K, V> {

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
      return new OutputCommitter() {
        public void abortTask(TaskAttemptContext taskContext) {
        }

        public void cleanupJob(JobContext jobContext) {
        }

        public void commitJob(JobContext jobContext) {
        }

        public void commitTask(TaskAttemptContext taskContext) {
          Assert.fail("needsTaskCommit is false but commitTask was called");
        }

        public boolean needsTaskCommit(TaskAttemptContext taskContext) {
          return false;
        }

        public void setupJob(JobContext jobContext) {
        }

        public void setupTask(TaskAttemptContext taskContext) {
        }
      };
    }
  }

}
