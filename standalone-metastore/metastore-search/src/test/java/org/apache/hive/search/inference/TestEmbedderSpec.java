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

package org.apache.hive.search.inference;

import java.nio.file.Path;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(MetastoreUnitTest.class)
public class TestEmbedderSpec {

  private static final Path MODEL_DIR = Path.of("/tmp/test-model");

  @Test
  public void prefixForTaskTypeE5() {
    EmbedderSpec spec = EmbedderSpec.e5("m", MODEL_DIR);
    assertEquals("passage: ", spec.prefixFor(Embedder.TaskType.DOCUMENT));
    assertEquals("query: ", spec.prefixFor(Embedder.TaskType.QUERY));
  }

  @Test
  public void noneUsesEmptyPrefixes() {
    EmbedderSpec spec = EmbedderSpec.none("m", MODEL_DIR);
    assertEquals("", spec.prefixFor(Embedder.TaskType.DOCUMENT));
    assertEquals("", spec.prefixFor(Embedder.TaskType.QUERY));
  }

  @Test
  public void poolingFromConfig() {
    assertEquals(EmbedderSpec.Pooling.MEAN, EmbedderSpec.Pooling.fromConfig("mean"));
    assertEquals(EmbedderSpec.Pooling.CLS, EmbedderSpec.Pooling.fromConfig("cls"));
    assertEquals(EmbedderSpec.Pooling.LAST, EmbedderSpec.Pooling.fromConfig("last"));
    assertEquals(EmbedderSpec.Pooling.LAST, EmbedderSpec.Pooling.fromConfig("eos"));
  }
}
