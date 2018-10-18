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

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * DatasetCollection: utility wrapper class for a set of datasets
 */
public class DatasetCollection {
  private Set<Dataset> coll = new HashSet<Dataset>();

  public void add(Dataset dataset) {
    coll.add(dataset);
  }

  public void add(String table) {
    add(new Dataset(table));
  }

  public Set<Dataset> getDatasets() {
    return coll;
  }

  public Set<String> getTables() {
    return coll.stream().map(d -> d.getTable()).collect(Collectors.toSet());
  }
}
