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

package org.apache.hadoop.hive.ql.hooks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class TestATSHook {

  private ObjectMapper objectMapper = new ObjectMapper();
  private ATSHook uut;

  @Before
  public void setUp() {
    uut = new ATSHook();
  }

  @Test
  public void testCreatePreHookEventJsonShhouldMatch() throws Exception {
    TimelineEntity timelineEntity =  uut.createPreHookEvent(
            "test-query-id", "test-query", new org.json.JSONObject(), 0L,
            "test-user", "test-request-user", 0, 0, "test-opid",
            "client-ip-address", "hive-instance-address", "hive-instance-type", "session-id", "log-id",
            "thread-id", "execution-mode", Collections.<String>emptyList(), Collections.<String>emptyList(),
            new HiveConf(), null, "domain-id");
    String resultStr = (String) timelineEntity.getOtherInfo()
            .get(ATSHook.OtherInfoTypes.QUERY.name());

    JsonNode result = objectMapper.readTree(resultStr);
    JsonNode expected = objectMapper.readTree("{\"queryText\":\"test-query\"," +
            "\"queryPlan\":{}}");

    assertEquals(expected, result);
  }
}
