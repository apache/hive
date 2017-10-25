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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.messaging;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import org.codehaus.jackson.annotate.JsonProperty;

public class PartitionFiles {

  @JsonProperty
  private String partitionName;
  @JsonProperty
  private List<String> files;

  public PartitionFiles(String partitionName, Iterator<String> files) {
    this.partitionName = partitionName;
    this.files = Lists.newArrayList(files);
  }

  public PartitionFiles() {
  }

  public String getPartitionName() {
    return partitionName;
  }

  public void setPartitionName(String partitionName) {
    this.partitionName = partitionName;
  }

  public Iterable<String> getFiles() {
    return files;
  }
}
