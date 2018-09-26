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

package org.apache.hadoop.hive.kafka;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.DefaultHiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.StorageHandlerInfo;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Hive Kafka storage handler to allow user to read and write from/to Kafka message bus.
 */
@SuppressWarnings("ALL") public class KafkaStorageHandler extends DefaultHiveMetaHook implements HiveStorageHandler {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaStorageHandler.class);
  private static final String KAFKA_STORAGE_HANDLER = "org.apache.hadoop.hive.kafka.KafkaStorageHandler";

  private Configuration configuration;

  @Override public Class<? extends InputFormat> getInputFormatClass() {
    return KafkaInputFormat.class;
  }

  @Override public Class<? extends OutputFormat> getOutputFormatClass() {
    return KafkaOutputFormat.class;
  }

  @Override public Class<? extends AbstractSerDe> getSerDeClass() {
    return KafkaSerDe.class;
  }

  @Override public HiveMetaHook getMetaHook() {
    return this;
  }

  @Override public HiveAuthorizationProvider getAuthorizationProvider() {
    return new DefaultHiveAuthorizationProvider();
  }

  @Override public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    configureCommonProperties(tableDesc, jobProperties);
  }

  private void configureCommonProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    String topic = tableDesc.getProperties().getProperty(KafkaTableProperties.HIVE_KAFKA_TOPIC.getName(), "");
    if (topic.isEmpty()) {
      throw new IllegalArgumentException("Kafka topic missing set table property->"
          + KafkaTableProperties.HIVE_KAFKA_TOPIC.getName());
    }
    jobProperties.put(KafkaTableProperties.HIVE_KAFKA_TOPIC.getName(), topic);
    String
        brokerString =
        tableDesc.getProperties().getProperty(KafkaTableProperties.HIVE_KAFKA_BOOTSTRAP_SERVERS.getName(), "");
    if (brokerString.isEmpty()) {
      throw new IllegalArgumentException("Broker address missing set table property->"
          + KafkaTableProperties.HIVE_KAFKA_BOOTSTRAP_SERVERS.getName());
    }
    jobProperties.put(KafkaTableProperties.HIVE_KAFKA_BOOTSTRAP_SERVERS.getName(), brokerString);
    Arrays.stream(KafkaTableProperties.values())
        .filter(tableProperty -> !tableProperty.isMandatory())
        .forEach(tableProperty -> jobProperties.put(tableProperty.getName(),
            tableDesc.getProperties().getProperty(tableProperty.getName())));
    // If the user ask for EOS then set the read to only committed.
    if (jobProperties.get(KafkaTableProperties.WRITE_SEMANTIC_PROPERTY.getName())
        .equals(KafkaOutputFormat.WriteSemantic.EXACTLY_ONCE.name())) {
      jobProperties.put("kafka.consumer.isolation.level", "read_committed");
    }
  }

  @Override public void configureInputJobCredentials(TableDesc tableDesc, Map<String, String> secrets) {

  }

  @Override public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    configureCommonProperties(tableDesc, jobProperties);
  }

  @Override public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    configureInputJobProperties(tableDesc, jobProperties);
    configureOutputJobProperties(tableDesc, jobProperties);
  }

  @Override public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    Map<String, String> properties = new HashMap<>();
    configureInputJobProperties(tableDesc, properties);
    configureOutputJobProperties(tableDesc, properties);
    properties.forEach(jobConf::set);
    try {
      KafkaUtils.copyDependencyJars(jobConf, KafkaStorageHandler.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public void setConf(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override public Configuration getConf() {
    return configuration;
  }

  @Override public String toString() {
    return KAFKA_STORAGE_HANDLER;
  }

  @Override public StorageHandlerInfo getStorageHandlerInfo(Table table) throws MetaException {
    String topic = table.getParameters().get(KafkaTableProperties.HIVE_KAFKA_TOPIC.getName());
    if (topic == null || topic.isEmpty()) {
      throw new MetaException("topic is null or empty");
    }
    String brokers = table.getParameters().get(KafkaTableProperties.HIVE_KAFKA_BOOTSTRAP_SERVERS.getName());
    if (brokers == null || brokers.isEmpty()) {
      throw new MetaException("kafka brokers string is null or empty");
    }
    final Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers);
    properties.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, Utilities.getTaskId(getConf()));
    table.getParameters()
        .entrySet()
        .stream()
        .filter(objectObjectEntry -> objectObjectEntry.getKey()
            .toLowerCase()
            .startsWith(KafkaUtils.CONSUMER_CONFIGURATION_PREFIX))
        .forEach(entry -> {
          String key = entry.getKey().substring(KafkaUtils.CONSUMER_CONFIGURATION_PREFIX.length() + 1);
          if (KafkaUtils.FORBIDDEN_PROPERTIES.contains(key)) {
            throw new IllegalArgumentException("Not suppose to set Kafka Property " + key);
          }
          properties.put(key, entry.getValue());
        });
    return new KafkaStorageHandlerInfo(topic, properties);
  }

  private Properties buildProducerProperties(Table table) {
    String brokers = table.getParameters().get(KafkaTableProperties.HIVE_KAFKA_BOOTSTRAP_SERVERS.getName());
    if (brokers == null || brokers.isEmpty()) {
      throw new RuntimeException("kafka brokers string is null or empty");
    }
    final Properties properties = new Properties();
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers);
    table.getParameters()
        .entrySet()
        .stream()
        .filter(objectObjectEntry -> objectObjectEntry.getKey()
            .toLowerCase()
            .startsWith(KafkaUtils.PRODUCER_CONFIGURATION_PREFIX))
        .forEach(entry -> {
          String key = entry.getKey().substring(KafkaUtils.PRODUCER_CONFIGURATION_PREFIX.length() + 1);
          if (KafkaUtils.FORBIDDEN_PROPERTIES.contains(key)) {
            throw new IllegalArgumentException("Not suppose to set Kafka Property " + key);
          }
          properties.put(key, entry.getValue());
        });
    return properties;
  }

  @Override public LockType getLockType(WriteEntity writeEntity) {
    if (writeEntity.getWriteType().equals(WriteEntity.WriteType.INSERT)) {
      return LockType.SHARED_READ;
    }
    return LockType.SHARED_WRITE;
  }

  private String getQueryId() {
    return HiveConf.getVar(getConf(), HiveConf.ConfVars.HIVEQUERYID);
  }

  @Override public void commitInsertTable(Table table, boolean overwrite) throws MetaException {
    boolean
        isExactlyOnce =
        table.getParameters()
            .get(KafkaTableProperties.WRITE_SEMANTIC_PROPERTY.getName())
            .equals(KafkaOutputFormat.WriteSemantic.EXACTLY_ONCE.name());
    String optimiticCommitVal = table.getParameters().get(KafkaTableProperties.HIVE_KAFKA_OPTIMISTIC_COMMIT.getName());
    boolean isTwoPhaseCommit = !Boolean.parseBoolean(optimiticCommitVal);
    if (!isExactlyOnce || !isTwoPhaseCommit) {
      //Case it is not 2 phase commit no open transaction to handel.
      return;
    }

    final Path queryWorkingDir = getQueryWorkingDir(table);
    final Map<String, Pair<Long, Short>> transactionsMap;
    final int maxTries = Integer.parseInt(table.getParameters().get(KafkaTableProperties.MAX_RETRIES.getName()));
    // We have 4 Stages ahead of us:
    // 1 Fetch Transactions state from HDFS.
    // 2 Build/inti all the Kafka producers and perform a pre commit call to check if we can go ahead with commit.
    // 3 Commit Transactions one by one.
    // 4 Clean workingDirectory.

    //First stage fetch the Transactions states
    final RetryUtils.Task<Map<String, Pair<Long, Short>>>
        fetchTransactionStates =
        new RetryUtils.Task<Map<String, Pair<Long, Short>>>() {
          @Override public Map<String, Pair<Long, Short>> perform() throws Exception {
            return TransactionalKafkaWriter.getTransactionsState(FileSystem.get(getConf()), queryWorkingDir);
          }
        };

    try {
      transactionsMap = RetryUtils.retry(fetchTransactionStates, (error) -> (error instanceof IOException), maxTries);
    } catch (Exception e) {
      // Can not go further
      LOG.error("Can not fetch Transaction states due [{}]", e.getMessage());
      throw new MetaException(e.getMessage());
    }

    //Second Stage Resume Producers and Pre commit
    final Properties baseProducerPros = buildProducerProperties(table);
    final Map<String, HiveKafkaProducer> producersMap = new HashMap<>();
    final RetryUtils.Task<Void> buildProducersTask = new RetryUtils.Task<Void>() {
      @Override public Void perform() throws Exception {
        assert producersMap.size() == 0;
        transactionsMap.forEach((key, value) -> {
          // Base Producer propeties, missing the transaction Id.
          baseProducerPros.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, key);
          HiveKafkaProducer<byte[], byte[]> producer = new HiveKafkaProducer<>(baseProducerPros);
          producer.resumeTransaction(value.getLeft(), value.getRight());
          // This is a dummy RPC call to ensure that the producer still resumable and signal the Pre-commit as per :
          // https://cwiki.apache.org/confluence/display/KAFKA/Transactional+Messaging+in+Kafka#EndPhase
          producer.sendOffsetsToTransaction(ImmutableMap.of(), "__dry_run");
          producersMap.put(key, producer);
        });
        return null;
      }
    };

    RetryUtils.CleanupAfterFailure cleanUpTheMap = new RetryUtils.CleanupAfterFailure() {
      @Override public void cleanup() {
        producersMap.forEach((s, producer) -> producer.close(0, TimeUnit.MILLISECONDS));
        producersMap.clear();
      }
    };
    final Predicate<Throwable>
        isRetrayable = (error) -> !KafkaUtils.exceptionIsFatal(error) && !(error instanceof ProducerFencedException);
    try {
      RetryUtils.retry(buildProducersTask, isRetrayable, cleanUpTheMap, maxTries, "Error while Builing Producers");
    } catch (Exception e) {
      // Can not go further
      LOG.error("Can not fetch build produces due [{}]", e.getMessage());
      throw new MetaException(e.getMessage());
    }

    //Third Stage Commit Transactions, this part is the actual critical section.
    //The commit might be retried on error, but keep in mind in some cases, like open transaction can expire
    //after timeout duration of 15 mins it is not possible to go further.
    final Set<String> committedTx = new HashSet<>();
    final RetryUtils.Task<Void> commitTask = new RetryUtils.Task() {
      @Override public Object perform() throws Exception {
        producersMap.forEach((key, producer) -> {
          if (!committedTx.contains(key)) {
            producer.commitTransaction();
            committedTx.add(key);
            producer.close();
            LOG.info("Committed Transaction [{}]", key);
          }
        });
        return null;
      }
    };

    try {
      RetryUtils.retry(commitTask, isRetrayable, maxTries);
    } catch (Exception e) {
      // at this point we are in a funky state if one commit happend!! close and log it
      producersMap.forEach((key, producer) -> producer.close(0, TimeUnit.MILLISECONDS));
      LOG.error("Commit transaction failed", e);
      if (committedTx.size() > 0) {
        LOG.error("Partial Data Got Commited Some actions need to be Done");
        committedTx.stream().forEach(key -> LOG.error("Transaction [{}] is an orphen commit", key));
      }
      throw new MetaException(e.getMessage());
    }

    //Stage four, clean the Query Directory
    final RetryUtils.Task<Void> cleanQueryDirTask = new RetryUtils.Task<Void>() {
      @Override public Void perform() throws Exception {
        cleanWorkingDirectory(queryWorkingDir);
        return null;
      }
    };
    try {
      RetryUtils.retry(cleanQueryDirTask, (error) -> error instanceof IOException, maxTries);
    } catch (Exception e) {
      //just log it
      LOG.error("Faild to clean Query Working Directory [{}] due to [{}]", queryWorkingDir, e.getMessage());
    }
  }

  @Override public void preInsertTable(Table table, boolean overwrite) throws MetaException {
    if (overwrite) {
      throw new MetaException("Kafa Table does not support the overwite SQL Smentic");
    }
  }

  @Override public void rollbackInsertTable(Table table, boolean overwrite) throws MetaException {

  }

  @Override public void preCreateTable(Table table) throws MetaException {
    if (!table.getTableType().equals(TableType.EXTERNAL_TABLE.toString())) {
      throw new MetaException(KAFKA_STORAGE_HANDLER + " supports only " + TableType.EXTERNAL_TABLE);
    }
    Arrays.stream(KafkaTableProperties.values())
        .filter(KafkaTableProperties::isMandatory)
        .forEach(key -> Preconditions.checkNotNull(table.getParameters().get(key.getName()),
            "Set Table property " + key.getName()));
    // Put all the default at the pre create.
    Arrays.stream(KafkaTableProperties.values()).forEach((key) -> {
      if (table.getParameters().get(key.getName()) == null) {
        table.putToParameters(key.getName(), key.getDefaultValue());
      }
    });
  }

  @Override public void rollbackCreateTable(Table table) throws MetaException {

  }

  @Override public void commitCreateTable(Table table) throws MetaException {
    commitInsertTable(table, false);
  }

  @Override public void preDropTable(Table table) throws MetaException {

  }

  @Override public void rollbackDropTable(Table table) throws MetaException {

  }

  @Override public void commitDropTable(Table table, boolean deleteData) throws MetaException {

  }

  private Path getQueryWorkingDir(Table table) {
    return new Path(table.getSd().getLocation(), getQueryId());
  }

  private void cleanWorkingDirectory(Path queryWorkingDir) throws IOException {
    FileSystem fs = FileSystem.get(getConf());
    fs.delete(queryWorkingDir, true);
  }
}
