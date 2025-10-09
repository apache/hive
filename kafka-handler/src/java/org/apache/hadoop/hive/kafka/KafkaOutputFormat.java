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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Kafka Hive Output Format class used to write Hive Rows to a Kafka Queue.
 */
public class KafkaOutputFormat implements HiveOutputFormat<NullWritable, KafkaWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaOutputFormat.class);

  @Override public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc,
      Path finalOutPath,
      Class<? extends Writable> valueClass,
      boolean isCompressed,
      Properties tableProperties,
      Progressable progress) {
    final String topic = jc.get(KafkaTableProperties.HIVE_KAFKA_TOPIC.getName());
    final Boolean optimisticCommit = jc.getBoolean(KafkaTableProperties.HIVE_KAFKA_OPTIMISTIC_COMMIT.getName(), false);
    final WriteSemantic
        writeSemantic =
        WriteSemantic.valueOf(jc.get(KafkaTableProperties.WRITE_SEMANTIC_PROPERTY.getName()));
    final Properties producerProperties = KafkaUtils.producerProperties(jc);
    final FileSinkOperator.RecordWriter recordWriter;
    switch (writeSemantic) {
    case AT_LEAST_ONCE:
      recordWriter = new SimpleKafkaWriter(topic, Utilities.getTaskId(jc), producerProperties);
      break;
    case EXACTLY_ONCE:
      FileSystem fs;
      try {
        fs = finalOutPath.getFileSystem(jc);
      } catch (IOException e) {
        LOG.error("Can not construct file system instance", e);
        throw new RuntimeException(e);
      }
      final String queryId = Preconditions.checkNotNull(jc.get(HiveConf.ConfVars.HIVE_QUERY_ID.varname, null));
      recordWriter =
          new TransactionalKafkaWriter(topic, producerProperties,
              new Path(Preconditions.checkNotNull(finalOutPath), queryId),
              fs,
              optimisticCommit);
      break;
    default:
      throw new IllegalArgumentException(String.format("Unknown delivery semantic [%s]", writeSemantic.toString()));
    }
    return recordWriter;
  }

  @Override public RecordWriter<NullWritable, KafkaWritable> getRecordWriter(FileSystem fileSystem,
      JobConf jobConf,
      String s,
      Progressable progressable) {
    throw new RuntimeException("this is not suppose to be here");
  }

  @Override public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) {

  }

  /**
   * Possible write semantic supported by the Record Writer.
   */
  enum WriteSemantic {
    /**
     * Deliver all the record at least once unless the job fails.
     * Therefore duplicates can be introduced due to lost ACKs or Tasks retries.
     * Currently this is the default.
     */
    AT_LEAST_ONCE,
    /**
     * Deliver every record exactly once.
     */
    EXACTLY_ONCE,
  }
}
