/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class TestInferenceConfig {

  @Test
  public void embeddingThreadsDefaultsToAtLeastOne() {
    Configuration conf = new Configuration(false);
    InferenceConfig config = new InferenceConfig(conf);
    assertTrue(config.getEmbeddingThreads() >= 1);
  }

  @Test
  public void embeddingThreadsHonorsConfiguration() {
    Configuration conf = new Configuration(false);
    conf.setInt(InferenceConfig.EMBEDDING_THREADS, 8);
    InferenceConfig config = new InferenceConfig(conf);
    assertEquals(8, config.getEmbeddingThreads());
  }

  @Test
  public void embeddingThreadsRejectsNonPositiveValues() {
    Configuration conf = new Configuration(false);
    conf.setInt(InferenceConfig.EMBEDDING_THREADS, 0);
    InferenceConfig config = new InferenceConfig(conf);
    assertEquals(1, config.getEmbeddingThreads());
  }

  @Test
  public void embeddingMaxSeqLengthDefaultsTo512() {
    Configuration conf = new Configuration(false);
    InferenceConfig config = new InferenceConfig(conf);
    assertEquals(512, config.getEmbeddingMaxSeqLength());
  }

  @Test
  public void embeddingMaxSeqLengthHonorsConfiguration() {
    Configuration conf = new Configuration(false);
    conf.setInt(InferenceConfig.EMBEDDING_MAX_SEQ_LENGTH, 256);
    InferenceConfig config = new InferenceConfig(conf);
    assertEquals(256, config.getEmbeddingMaxSeqLength());
  }
}
