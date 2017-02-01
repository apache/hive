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
package org.apache.hadoop.hive.ql.plan;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestMapWork {
  @Test
  public void testGetAndSetConsistency() {
    MapWork mw = new MapWork();
    LinkedHashMap<Path, ArrayList<String>> pathToAliases = new LinkedHashMap<>();
    pathToAliases.put(new Path("p0"), Lists.newArrayList("a1", "a2"));
    mw.setPathToAliases(pathToAliases);

    LinkedHashMap<Path, ArrayList<String>> pta = mw.getPathToAliases();
    assertEquals(pathToAliases, pta);

  }

  @Test
  public void testPath() {
    Path p1 = new Path("hdfs://asd/asd");
    Path p2 = new Path("hdfs://asd/asd/");

    assertEquals(p1, p2);
  }

}
