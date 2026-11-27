/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hive;

import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Owner resolution for new tables and views")
class TestHiveHadoopUtil {
  private static final Map<String, String> REQUESTED_OWNER =
      Collections.singletonMap(HiveCatalog.HMS_TABLE_OWNER, "some_owner");

  @DisplayName("Client-supplied owner is honored by default, as embedded engines rely on it")
  @Test
  void testClientSuppliedOwnerHonoredByDefault() {
    assertThat(HiveHadoopUtil.resolveTableOwner(new Configuration(false), REQUESTED_OWNER)).isEqualTo("some_owner");
    assertThat(HiveHadoopUtil.resolveTableOwner(null, REQUESTED_OWNER)).isEqualTo("some_owner");
  }

  @DisplayName("Client-supplied owner is ignored when disabled")
  @Test
  void testClientSuppliedOwnerIgnoredWhenDisabled() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(HiveHadoopUtil.CLIENT_SUPPLIED_OWNER_ENABLED, false);
    assertThat(HiveHadoopUtil.resolveTableOwner(conf, REQUESTED_OWNER)).isEqualTo(HiveHadoopUtil.currentUser());
  }

  @DisplayName("Owner defaults to the current user")
  @Test
  void testOwnerDefaultsToCurrentUser() {
    assertThat(HiveHadoopUtil.resolveTableOwner(new Configuration(false), Collections.emptyMap()))
        .isEqualTo(HiveHadoopUtil.currentUser());
    assertThat(HiveHadoopUtil.resolveTableOwner(null, null)).isEqualTo(HiveHadoopUtil.currentUser());
  }
}
