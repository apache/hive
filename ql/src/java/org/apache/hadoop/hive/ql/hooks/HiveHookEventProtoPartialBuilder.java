/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hadoop.hive.ql.hooks;

import java.util.Map;

import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.HiveProtoLoggingHook.OtherInfoType;
import org.apache.hadoop.hive.ql.hooks.proto.HiveHookEvents;
import org.apache.hadoop.hive.ql.plan.ExplainWork;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveHookEventProtoPartialBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(HiveHookEventProtoPartialBuilder.class.getName());
  private final HiveHookEvents.HiveHookEventProto event;
  @Nullable
  private final ExplainWork explainWork;
  private final Map<OtherInfoType, JSONObject> otherInfo;
  private final String queryStr;
  private final String stageIdRearrange;

  public HiveHookEventProtoPartialBuilder(HiveHookEvents.HiveHookEventProto.Builder builder, ExplainWork explainWork, Map<OtherInfoType, JSONObject> otherInfo, String queryStr, String stageIdRearrange) {
    this.event = builder.buildPartial();
    this.explainWork = explainWork;
    this.otherInfo = otherInfo;
    this.queryStr = queryStr;
    this.stageIdRearrange = stageIdRearrange;
  }

  public HiveHookEvents.HiveHookEventProto build() {
    if (explainWork != null) {
      addQueryObj(explainWork);
    }
    HiveHookEvents.HiveHookEventProto.Builder builder = HiveHookEvents.HiveHookEventProto.newBuilder();
    for (Map.Entry<OtherInfoType, JSONObject> each : otherInfo.entrySet()) {
        OtherInfoType type = each.getKey();
        JSONObject json = each.getValue();
        try {
          // json conversion can be expensive, doing it separately
          HiveProtoLoggingHook.EventLogger.addMapEntry(builder, type, json.toString());
        } catch (Exception e) {
          LOG.error("Unexpected exception while serializing json.", e);
        }
    }
    return builder.mergeFrom(event).build();
  }

  private void addQueryObj(ExplainWork explainWork) {
    try {
      JSONObject queryObj = new JSONObject();
      queryObj.put("queryText", queryStr);
      queryObj.put("queryPlan", getExplainJSON(explainWork));
      otherInfo.put(OtherInfoType.QUERY, queryObj);
    } catch (Exception e) {
      LOG.error("Unexpected exception while serializing json.", e);
    }
  }

  private JSONObject getExplainJSON(ExplainWork explainWork) throws Exception {
    ExplainTask explain = (ExplainTask) TaskFactory.get(explainWork, null);
    return explain.getJSONPlan(null, explainWork, stageIdRearrange);
  }
}
