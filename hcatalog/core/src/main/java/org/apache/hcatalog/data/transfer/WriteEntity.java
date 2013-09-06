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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hcatalog.data.transfer;

import java.util.Map;

/**
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.data.transfer.WriteEntity} instead
 */
public class WriteEntity extends EntityBase.Entity {

  /**
   * Don't instantiate {@link WriteEntity} directly. Use, {@link Builder} to
   * build {@link WriteEntity}.
   */

  private WriteEntity() {
    // Not allowed.
  }

  private WriteEntity(Builder builder) {
    this.region = builder.region;
    this.dbName = builder.dbName;
    this.tableName = builder.tableName;
    this.partitionKVs = builder.partitionKVs;
  }

  /**
   * This class should be used to build {@link WriteEntity}. It follows builder
   * pattern, letting you build your {@link WriteEntity} with whatever level of
   * detail you want.
   *
   */
  public static class Builder extends EntityBase {

    public Builder withRegion(final String region) {
      this.region = region;
      return this;
    }

    public Builder withDatabase(final String dbName) {
      this.dbName = dbName;
      return this;
    }

    public Builder withTable(final String tblName) {
      this.tableName = tblName;
      return this;
    }

    public Builder withPartition(final Map<String, String> partKVs) {
      this.partitionKVs = partKVs;
      return this;
    }

    public WriteEntity build() {
      return new WriteEntity(this);
    }

  }
}
