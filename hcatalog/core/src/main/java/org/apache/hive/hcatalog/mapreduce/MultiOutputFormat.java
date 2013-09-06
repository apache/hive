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

package org.apache.hive.hcatalog.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The MultiOutputFormat class simplifies writing output data to multiple
 * outputs.
 * <p>
 * Multiple output formats can be defined each with its own
 * <code>OutputFormat</code> class, own key class and own value class. Any
 * configuration on these output format classes can be done without interfering
 * with other output format's configuration.
 * <p>
 * Usage pattern for job submission:
 *
 * <pre>
 *
 * Job job = new Job();
 *
 * FileInputFormat.setInputPath(job, inDir);
 *
 * job.setMapperClass(WordCountMap.class);
 * job.setReducerClass(WordCountReduce.class);
 * job.setInputFormatClass(TextInputFormat.class);
 * job.setOutputFormatClass(MultiOutputFormat.class);
 * // Need not define OutputKeyClass and OutputValueClass. They default to
 * // Writable.class
 * job.setMapOutputKeyClass(Text.class);
 * job.setMapOutputValueClass(IntWritable.class);
 *
 *
 * // Create a JobConfigurer that will configure the job with the multiple
 * // output format information.
 * JobConfigurer configurer = MultiOutputFormat.createConfigurer(job);
 *
 * // Defines additional single text based output 'text' for the job.
 * // Any configuration for the defined OutputFormat should be done with
 * // the Job obtained with configurer.getJob() method.
 * configurer.addOutputFormat("text", TextOutputFormat.class,
 *                 IntWritable.class, Text.class);
 * FileOutputFormat.setOutputPath(configurer.getJob("text"), textOutDir);
 *
 * // Defines additional sequence-file based output 'sequence' for the job
 * configurer.addOutputFormat("sequence", SequenceFileOutputFormat.class,
 *                 Text.class, IntWritable.class);
 * FileOutputFormat.setOutputPath(configurer.getJob("sequence"), seqOutDir);
 * ...
 * // configure method to be called on the JobConfigurer once all the
 * // output formats have been defined and configured.
 * configurer.configure();
 *
 * job.waitForCompletion(true);
 * ...
 * </pre>
 * <p>
 * Usage in Reducer:
 *
 * <pre>
 * public class WordCountReduce extends
 *         Reducer&lt;Text, IntWritable, Writable, Writable&gt; {
 *
 *     private IntWritable count = new IntWritable();
 *
 *     public void reduce(Text word, Iterator&lt;IntWritable&gt; values,
 *             Context context)
 *             throws IOException {
 *         int sum = 0;
 *         for (IntWritable val : values) {
 *             sum += val.get();
 *         }
 *         count.set(sum);
 *         MultiOutputFormat.write(&quot;text&quot;, count, word, context);
 *         MultiOutputFormat.write(&quot;sequence&quot;, word, count, context);
 *     }
 *
 * }
 *
 * </pre>
 *
 * Map only jobs:
 * <p>
 * MultiOutputFormat.write("output", key, value, context); can be called similar
 * to a reducer in map only jobs.
 *
 */
public class MultiOutputFormat extends OutputFormat<Writable, Writable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultiOutputFormat.class.getName());
  private static final String MO_ALIASES = "mapreduce.multiout.aliases";
  private static final String MO_ALIAS = "mapreduce.multiout.alias";
  private static final String CONF_KEY_DELIM = "%%";
  private static final String CONF_VALUE_DELIM = ";;";
  private static final String COMMA_DELIM = ",";
  private static final List<String> configsToOverride = new ArrayList<String>();
  private static final Map<String, String> configsToMerge = new HashMap<String, String>();

  static {
    configsToOverride.add("mapred.output.dir");
    configsToOverride.add(ShimLoader.getHadoopShims().getHCatShim().getPropertyName(
        HadoopShims.HCatHadoopShims.PropertyName.CACHE_SYMLINK));
    configsToMerge.put(JobContext.JOB_NAMENODES, COMMA_DELIM);
    configsToMerge.put("tmpfiles", COMMA_DELIM);
    configsToMerge.put("tmpjars", COMMA_DELIM);
    configsToMerge.put("tmparchives", COMMA_DELIM);
    configsToMerge.put(ShimLoader.getHadoopShims().getHCatShim().getPropertyName(
        HadoopShims.HCatHadoopShims.PropertyName.CACHE_ARCHIVES), COMMA_DELIM);
    configsToMerge.put(ShimLoader.getHadoopShims().getHCatShim().getPropertyName(
        HadoopShims.HCatHadoopShims.PropertyName.CACHE_FILES), COMMA_DELIM);
    String fileSep;
    if (HCatUtil.isHadoop23()) {
      fileSep = ",";
    } else {
      fileSep = System.getProperty("path.separator");
    }
    configsToMerge.put("mapred.job.classpath.archives", fileSep);
    configsToMerge.put("mapred.job.classpath.files", fileSep);
  }

  /**
   * Get a JobConfigurer instance that will support configuration of the job
   * for multiple output formats.
   *
   * @param job the mapreduce job to be submitted
   * @return JobConfigurer
   */
  public static JobConfigurer createConfigurer(Job job) {
    return JobConfigurer.create(job);
  }

  /**
   * Get the JobContext with the related OutputFormat configuration populated given the alias
   * and the actual JobContext
   * @param alias the name given to the OutputFormat configuration
   * @param context the JobContext
   * @return a copy of the JobContext with the alias configuration populated
   */
  public static JobContext getJobContext(String alias, JobContext context) {
    String aliasConf = context.getConfiguration().get(getAliasConfName(alias));
    JobContext aliasContext = ShimLoader.getHadoopShims().getHCatShim().createJobContext(
        context.getConfiguration(), context.getJobID());
    addToConfig(aliasConf, aliasContext.getConfiguration());
    return aliasContext;
  }

  /**
   * Get the TaskAttemptContext with the related OutputFormat configuration populated given the alias
   * and the actual TaskAttemptContext
   * @param alias the name given to the OutputFormat configuration
   * @param context the Mapper or Reducer Context
   * @return a copy of the TaskAttemptContext with the alias configuration populated
   */
  public static TaskAttemptContext getTaskAttemptContext(String alias, TaskAttemptContext context) {
    String aliasConf = context.getConfiguration().get(getAliasConfName(alias));
    TaskAttemptContext aliasContext = ShimLoader.getHadoopShims().getHCatShim().createTaskAttemptContext(
        context.getConfiguration(), context.getTaskAttemptID());
    addToConfig(aliasConf, aliasContext.getConfiguration());
    return aliasContext;
  }

  /**
   * Write the output key and value using the OutputFormat defined by the
   * alias.
   *
   * @param alias the name given to the OutputFormat configuration
   * @param key the output key to be written
   * @param value the output value to be written
   * @param context the Mapper or Reducer Context
   * @throws IOException
   * @throws InterruptedException
   */
  public static <K, V> void write(String alias, K key, V value, TaskInputOutputContext context)
    throws IOException, InterruptedException {
    KeyValue<K, V> keyval = new KeyValue<K, V>(key, value);
    context.write(new Text(alias), keyval);
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    for (String alias : getOutputFormatAliases(context)) {
      LOGGER.debug("Calling checkOutputSpecs for alias: " + alias);
      JobContext aliasContext = getJobContext(alias, context);
      OutputFormat<?, ?> outputFormat = getOutputFormatInstance(aliasContext);
      outputFormat.checkOutputSpecs(aliasContext);
      // Copy credentials and any new config added back to JobContext
      context.getCredentials().addAll(aliasContext.getCredentials());
      setAliasConf(alias, context, aliasContext);
    }
  }

  @Override
  public RecordWriter<Writable, Writable> getRecordWriter(TaskAttemptContext context)
    throws IOException,
    InterruptedException {
    return new MultiRecordWriter(context);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException,
    InterruptedException {
    return new MultiOutputCommitter(context);
  }

  private static OutputFormat<?, ?> getOutputFormatInstance(JobContext context) {
    OutputFormat<?, ?> outputFormat;
    try {
      outputFormat = ReflectionUtils.newInstance(context.getOutputFormatClass(),
          context.getConfiguration());
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
    return outputFormat;
  }

  private static String[] getOutputFormatAliases(JobContext context) {
    return context.getConfiguration().getStrings(MO_ALIASES);
  }

  /**
   * Compare the aliasContext with userJob and add the differing configuration
   * as mapreduce.multiout.alias.<aliasname>.conf to the userJob.
   * <p>
   * Merge config like tmpjars, tmpfile, tmparchives,
   * mapreduce.job.hdfs-servers that are directly handled by JobClient and add
   * them to userJob.
   * <p>
   * Add mapred.output.dir config to userJob.
   *
   * @param alias alias name associated with a OutputFormat
   * @param userJob reference to Job that the user is going to submit
   * @param aliasContext JobContext populated with OutputFormat related
   *            configuration.
   */
  private static void setAliasConf(String alias, JobContext userJob, JobContext aliasContext) {
    Configuration userConf = userJob.getConfiguration();
    StringBuilder builder = new StringBuilder();
    for (Entry<String, String> conf : aliasContext.getConfiguration()) {
      String key = conf.getKey();
      String value = conf.getValue();
      String jobValue = userConf.getRaw(key);
      if (jobValue == null || !jobValue.equals(value)) {
        if (configsToMerge.containsKey(key)) {
          String mergedValue = getMergedConfValue(jobValue, value, configsToMerge.get(key));
          userConf.set(key, mergedValue);
        } else {
          if (configsToOverride.contains(key)) {
            userConf.set(key, value);
          }
          builder.append(key).append(CONF_KEY_DELIM).append(value)
              .append(CONF_VALUE_DELIM);
        }
      }
    }
    if (builder.length() > CONF_VALUE_DELIM.length()) {
      builder.delete(builder.length() - CONF_VALUE_DELIM.length(), builder.length());
      userConf.set(getAliasConfName(alias), builder.toString());
    }
  }

  private static String getMergedConfValue(String originalValues, String newValues, String separator) {
    if (originalValues == null) {
      return newValues;
    }
    Set<String> mergedValues = new LinkedHashSet<String>();
    mergedValues.addAll(Arrays.asList(StringUtils.split(originalValues, separator)));
    mergedValues.addAll(Arrays.asList(StringUtils.split(newValues, separator)));
    StringBuilder builder = new StringBuilder(originalValues.length() + newValues.length() + 2);
    for (String value : mergedValues) {
      builder.append(value).append(separator);
    }
    return builder.substring(0, builder.length() - separator.length());
  }

  private static String getAliasConfName(String alias) {
    return MO_ALIAS + "." + alias + ".conf";
  }

  private static void addToConfig(String aliasConf, Configuration conf) {
    String[] config = aliasConf.split(CONF_KEY_DELIM + "|" + CONF_VALUE_DELIM);
    for (int i = 0; i < config.length; i += 2) {
      conf.set(config[i], config[i + 1]);
    }
  }

  /**
   * Class that supports configuration of the job for multiple output formats.
   */
  public static class JobConfigurer {

    private final Job job;
    private Map<String, Job> outputConfigs = new LinkedHashMap<String, Job>();

    private JobConfigurer(Job job) {
      this.job = job;
    }

    private static JobConfigurer create(Job job) {
      JobConfigurer configurer = new JobConfigurer(job);
      return configurer;
    }

    /**
     * Add a OutputFormat configuration to the Job with a alias name.
     *
     * @param alias the name to be given to the OutputFormat configuration
     * @param outputFormatClass OutputFormat class
     * @param keyClass the key class for the output data
     * @param valueClass the value class for the output data
     * @throws IOException
     */
    public void addOutputFormat(String alias,
        Class<? extends OutputFormat> outputFormatClass,
        Class<?> keyClass, Class<?> valueClass) throws IOException {
      Job copy = new Job(this.job.getConfiguration());
      outputConfigs.put(alias, copy);
      copy.setOutputFormatClass(outputFormatClass);
      copy.setOutputKeyClass(keyClass);
      copy.setOutputValueClass(valueClass);
    }

    /**
     * Get the Job configuration for a OutputFormat defined by the alias
     * name. The job returned by this method should be passed to the
     * OutputFormat for any configuration instead of the Job that will be
     * submitted to the JobClient.
     *
     * @param alias the name used for the OutputFormat during
     *            addOutputFormat
     * @return Job
     */
    public Job getJob(String alias) {
      Job copy = outputConfigs.get(alias);
      if (copy == null) {
        throw new IllegalArgumentException("OutputFormat with alias " + alias
            + " has not beed added");
      }
      return copy;
    }

    /**
     * Configure the job with the multiple output formats added. This method
     * should be called after all the output formats have been added and
     * configured and before the job submission.
     */
    public void configure() {
      StringBuilder aliases = new StringBuilder();
      Configuration jobConf = job.getConfiguration();
      for (Entry<String, Job> entry : outputConfigs.entrySet()) {
        // Copy credentials
        job.getCredentials().addAll(entry.getValue().getCredentials());
        String alias = entry.getKey();
        aliases.append(alias).append(COMMA_DELIM);
        // Store the differing configuration for each alias in the job
        // as a setting.
        setAliasConf(alias, job, entry.getValue());
      }
      aliases.delete(aliases.length() - COMMA_DELIM.length(), aliases.length());
      jobConf.set(MO_ALIASES, aliases.toString());
    }

  }

  private static class KeyValue<K, V> implements Writable {
    private final K key;
    private final V value;

    public KeyValue(K key, V value) {
      this.key = key;
      this.value = value;
    }

    public K getKey() {
      return key;
    }

    public V getValue() {
      return value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      // Ignore. Not required as this will be never
      // serialized/deserialized.
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      // Ignore. Not required as this will be never
      // serialized/deserialized.
    }
  }

  private static class MultiRecordWriter extends RecordWriter<Writable, Writable> {

    private final Map<String, BaseRecordWriterContainer> baseRecordWriters;

    public MultiRecordWriter(TaskAttemptContext context) throws IOException,
        InterruptedException {
      baseRecordWriters = new LinkedHashMap<String, BaseRecordWriterContainer>();
      String[] aliases = getOutputFormatAliases(context);
      for (String alias : aliases) {
        LOGGER.info("Creating record writer for alias: " + alias);
        TaskAttemptContext aliasContext = getTaskAttemptContext(alias, context);
        Configuration aliasConf = aliasContext.getConfiguration();
        // Create output directory if not already created.
        String outDir = aliasConf.get("mapred.output.dir");
        if (outDir != null) {
          Path outputDir = new Path(outDir);
          FileSystem fs = outputDir.getFileSystem(aliasConf);
          if (!fs.exists(outputDir)) {
            fs.mkdirs(outputDir);
          }
        }
        OutputFormat<?, ?> outputFormat = getOutputFormatInstance(aliasContext);
        baseRecordWriters.put(alias,
            new BaseRecordWriterContainer(outputFormat.getRecordWriter(aliasContext),
                aliasContext));
      }
    }

    @Override
    public void write(Writable key, Writable value) throws IOException, InterruptedException {
      Text _key = (Text) key;
      KeyValue _value = (KeyValue) value;
      String alias = new String(_key.getBytes(), 0, _key.getLength());
      BaseRecordWriterContainer baseRWContainer = baseRecordWriters.get(alias);
      if (baseRWContainer == null) {
        throw new IllegalArgumentException("OutputFormat with alias " + alias
            + " has not been added");
      }
      baseRWContainer.getRecordWriter().write(_value.getKey(), _value.getValue());
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      for (Entry<String, BaseRecordWriterContainer> entry : baseRecordWriters.entrySet()) {
        BaseRecordWriterContainer baseRWContainer = entry.getValue();
        LOGGER.info("Closing record writer for alias: " + entry.getKey());
        baseRWContainer.getRecordWriter().close(baseRWContainer.getContext());
      }
    }

  }

  private static class BaseRecordWriterContainer {

    private final RecordWriter recordWriter;
    private final TaskAttemptContext context;

    public BaseRecordWriterContainer(RecordWriter recordWriter, TaskAttemptContext context) {
      this.recordWriter = recordWriter;
      this.context = context;
    }

    public RecordWriter getRecordWriter() {
      return recordWriter;
    }

    public TaskAttemptContext getContext() {
      return context;
    }
  }

  public class MultiOutputCommitter extends OutputCommitter {

    private final Map<String, BaseOutputCommitterContainer> outputCommitters;

    public MultiOutputCommitter(TaskAttemptContext context) throws IOException,
        InterruptedException {
      outputCommitters = new LinkedHashMap<String, MultiOutputFormat.BaseOutputCommitterContainer>();
      String[] aliases = getOutputFormatAliases(context);
      for (String alias : aliases) {
        LOGGER.info("Creating output committer for alias: " + alias);
        TaskAttemptContext aliasContext = getTaskAttemptContext(alias, context);
        OutputCommitter baseCommitter = getOutputFormatInstance(aliasContext)
            .getOutputCommitter(aliasContext);
        outputCommitters.put(alias,
            new BaseOutputCommitterContainer(baseCommitter, aliasContext));
      }
    }

    @Override
    public void setupJob(JobContext jobContext) throws IOException {
      for (String alias : outputCommitters.keySet()) {
        LOGGER.info("Calling setupJob for alias: " + alias);
        BaseOutputCommitterContainer outputContainer = outputCommitters.get(alias);
        outputContainer.getBaseCommitter().setupJob(outputContainer.getContext());
      }
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) throws IOException {
      for (String alias : outputCommitters.keySet()) {
        LOGGER.info("Calling setupTask for alias: " + alias);
        BaseOutputCommitterContainer outputContainer = outputCommitters.get(alias);
        outputContainer.getBaseCommitter().setupTask(outputContainer.getContext());
      }
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
      boolean needTaskCommit = false;
      for (String alias : outputCommitters.keySet()) {
        BaseOutputCommitterContainer outputContainer = outputCommitters.get(alias);
        needTaskCommit = needTaskCommit
            || outputContainer.getBaseCommitter().needsTaskCommit(
                outputContainer.getContext());
      }
      return needTaskCommit;
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext) throws IOException {
      for (String alias : outputCommitters.keySet()) {
        BaseOutputCommitterContainer outputContainer = outputCommitters.get(alias);
        OutputCommitter baseCommitter = outputContainer.getBaseCommitter();
        TaskAttemptContext committerContext = outputContainer.getContext();
        if (baseCommitter.needsTaskCommit(committerContext)) {
          LOGGER.info("Calling commitTask for alias: " + alias);
          baseCommitter.commitTask(committerContext);
        }
      }
    }

    @Override
    public void abortTask(TaskAttemptContext taskContext) throws IOException {
      for (String alias : outputCommitters.keySet()) {
        LOGGER.info("Calling abortTask for alias: " + alias);
        BaseOutputCommitterContainer outputContainer = outputCommitters.get(alias);
        outputContainer.getBaseCommitter().abortTask(outputContainer.getContext());
      }
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
      for (String alias : outputCommitters.keySet()) {
        LOGGER.info("Calling commitJob for alias: " + alias);
        BaseOutputCommitterContainer outputContainer = outputCommitters.get(alias);
        outputContainer.getBaseCommitter().commitJob(outputContainer.getContext());
      }
    }

    @Override
    public void abortJob(JobContext jobContext, State state) throws IOException {
      for (String alias : outputCommitters.keySet()) {
        LOGGER.info("Calling abortJob for alias: " + alias);
        BaseOutputCommitterContainer outputContainer = outputCommitters.get(alias);
        outputContainer.getBaseCommitter().abortJob(outputContainer.getContext(), state);
      }
    }
  }

  private static class BaseOutputCommitterContainer {

    private final OutputCommitter outputCommitter;
    private final TaskAttemptContext context;

    public BaseOutputCommitterContainer(OutputCommitter outputCommitter,
        TaskAttemptContext context) {
      this.outputCommitter = outputCommitter;
      this.context = context;
    }

    public OutputCommitter getBaseCommitter() {
      return outputCommitter;
    }

    public TaskAttemptContext getContext() {
      return context;
    }
  }

}
