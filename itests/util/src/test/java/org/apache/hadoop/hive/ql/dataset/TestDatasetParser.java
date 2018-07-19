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
package org.apache.hadoop.hive.ql.dataset;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

/**
 * TestDatasetParser: test class for DataSetParser
 */
public class TestDatasetParser {

  @Test
  public void testParseOne() {
    Set<String> expected = new HashSet<String>(Arrays.asList("mydataset"));
    Assert.assertEquals(expected, DatasetParser
        .parseDatasetsFromLine(String.format("%smydataset", DatasetParser.DATASET_PREFIX)));
  }

  @Test
  public void testParseMultiple() {
    Set<String> expected = new HashSet<String>(Arrays.asList("one", "two", "three"));
    Assert.assertEquals(expected, DatasetParser
        .parseDatasetsFromLine(String.format("%sone,two,three", DatasetParser.DATASET_PREFIX)));
  }

  @Test
  public void testParseUnique() {
    Set<String> expected = new HashSet<String>(Arrays.asList("one", "two"));
    Assert.assertEquals(expected, DatasetParser
        .parseDatasetsFromLine(String.format("%sone,one,two", DatasetParser.DATASET_PREFIX)));
  }

  @Test
  public void testParseNone() {
    Assert.assertTrue(DatasetParser
        .parseDatasetsFromLine(String.format("%s", DatasetParser.DATASET_PREFIX)).isEmpty());
  }
}
