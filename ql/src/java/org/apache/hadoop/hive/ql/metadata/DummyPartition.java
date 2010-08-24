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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.metadata;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A Hive Table Partition: is a fundamental storage unit within a Table. Currently, Hive does not support
 * hierarchical partitions - For eg: if partition ds=1, hr=1 exists, there is no way to access ds=1
 *
 * Hierarchical partitions are needed in some cases, for eg. locking. For now, create a dummy partition to
 * satisfy this
 */
public class DummyPartition extends Partition {

  @SuppressWarnings("nls")
  static final private Log LOG = LogFactory
      .getLog("hive.ql.metadata.DummyPartition");

  private String name;
  public DummyPartition() {
  }

  public DummyPartition(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getCompleteName() {
    return getName();
  }
}
