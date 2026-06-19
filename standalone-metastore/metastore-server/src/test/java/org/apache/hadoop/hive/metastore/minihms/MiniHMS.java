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

package org.apache.hadoop.hive.metastore.minihms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

/**
 * Mini HMS implementation, which can be used to run tests against different HMS configurations.
 * Currently it supports 3 types:
 *  - EMBEDDED - MetaStore running in embedded mode
 *  - REMOTE - MetaStore running in the same process but in a dedicated thread and accessed
 *  through the Thrift interface
 *  - CLUSTER - In this case the MiniHMS is only a wrapper around the HMS running on a cluster,
 *  so the same tests could be run against a real cluster
 */
public class MiniHMS {
  /**
   * The possible MetaStore types.
   */
  public enum MiniHMSType {
    EMBEDDED,
    REMOTE,
    CLUSTER
  }

  /**
   * Builder for creating a Mini MetaStore object.
   */
  public static class Builder {
    private Configuration metaStoreConf = MetastoreConf.newMetastoreConf();
    private MiniHMSType miniHMSType = MiniHMSType.EMBEDDED;

    public Builder() {
    }

    public Builder setConf(Configuration conf) {
      this.metaStoreConf = new Configuration(conf);
      return this;
    }

    public Builder setType(MiniHMSType type) {
      this.miniHMSType = type;
      return this;
    }

    public AbstractMetaStoreService build() throws Exception {
      switch (miniHMSType) {
      case REMOTE:
        return new RemoteMetaStoreForTests(metaStoreConf);
      case EMBEDDED:
        return new EmbeddedMetaStoreForTests(metaStoreConf);
      case CLUSTER:
        return new ClusterMetaStoreForTests(metaStoreConf);
      default:
        throw new IllegalArgumentException("Unexpected miniHMSType: " + miniHMSType);
      }
    }
  }
}
