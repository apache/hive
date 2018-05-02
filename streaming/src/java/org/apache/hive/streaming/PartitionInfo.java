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

package org.apache.hive.streaming;

/**
 * Simple wrapper class for minimal partition related information used by streaming ingest.
 */
public class PartitionInfo {
  private String name;
  private String partitionLocation;
  private boolean exists;

  public PartitionInfo(final String name, final String partitionLocation, final boolean exists) {
    this.name = name;
    this.partitionLocation = partitionLocation;
    this.exists = exists;
  }

  public String getName() {
    return name;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public String getPartitionLocation() {
    return partitionLocation;
  }

  public void setPartitionLocation(final String partitionLocation) {
    this.partitionLocation = partitionLocation;
  }

  public boolean isExists() {
    return exists;
  }

  public void setExists(final boolean exists) {
    this.exists = exists;
  }
}
