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

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hive.search.testutil.StubEmbedModel;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class TestEmbedModelRegistry {

  @Test
  public void getReturnsConfiguredModel() throws Exception {
    StubEmbedModel model = new StubEmbedModel("stub-model");
    EmbedModelRegistry registry = new EmbedModelRegistry(Map.of("stub-model", model));
    assertTrue(registry.get("stub-model") == model);
    registry.close();
  }

  @Test
  public void getThrowsForUnknownModel() {
    EmbedModelRegistry registry = new EmbedModelRegistry(Map.of());
    IllegalStateException error = assertThrows(IllegalStateException.class, () -> registry.get("missing"));
    assertTrue(error.getMessage().contains("missing"));
  }

  @Test
  public void stubModelProducesDeterministicVectors() throws Exception {
    StubEmbedModel model = new StubEmbedModel("stub-model");
    float[] first = model.embed(EmbedModel.TaskType.QUERY, "sales");
    float[] second = model.embed(EmbedModel.TaskType.QUERY, "sales");
    assertArrayEquals(first, second, 0.0001f);

    float[] document = model.embed(EmbedModel.TaskType.DOCUMENT, "sales");
    assertNotEquals(first[0], document[0], 0.0001f);
  }
}
