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

package org.apache.iceberg.view;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.util.Lists;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class RefreshStateParser {
  private static final String VIEW_VERSION_ID = "view-version-id";
  private static final String REFRESH_START_TIMESTAMP_MS = "refresh-start-timestamp-ms";
  private static final String SOURCE_STATES = "source-states";

  private RefreshStateParser() {
  }

  public static String toJson(RefreshState refreshState) {
    Preconditions.checkArgument(refreshState != null, "Invalid refresh state: null");

    return JsonUtil.generate(gen -> toJson(refreshState, gen), true);
  }

  private static void toJson(RefreshState refreshState, JsonGenerator gen) throws IOException {

    gen.writeStartObject();

    gen.writeNumberField(VIEW_VERSION_ID, refreshState.viewVersionId());
    gen.writeNumberField(REFRESH_START_TIMESTAMP_MS, refreshState.refreshStartTimestampMs());

    gen.writeArrayFieldStart(SOURCE_STATES);
    if (refreshState.sourceStates() != null) {

      for  (SourceState sourceState : refreshState.sourceStates()) {
        SourceStateParser.toJson(sourceState, gen);
      }

    }
    gen.writeEndArray();

    gen.writeEndObject();
  }

  public static RefreshState fromJson(String json) {
    Preconditions.checkArgument(json != null, "Cannot parse source state from null object");

    return JsonUtil.parse(json, RefreshStateParser::fromJson);
  }

  private static RefreshState fromJson(JsonNode node) {
    Preconditions.checkArgument(node != null, "Cannot parse source state from null object");
    Preconditions.checkArgument(node.isObject(), "Cannot parse source state from non-object: %s", node);

    Integer viewVersionId = JsonUtil.getInt(VIEW_VERSION_ID, node);
    Long refreshStartTimestampMs = JsonUtil.getLong(REFRESH_START_TIMESTAMP_MS, node);

    JsonNode sourceStatesNode = JsonUtil.get(SOURCE_STATES, node);
    Preconditions.checkArgument(sourceStatesNode != null, "Cannot parse source state from null object");
    Preconditions.checkArgument(sourceStatesNode.isArray(), "Cannot parse source state from non-array: %s", node);
    List<SourceState> sourceStates = Lists.newArrayListWithExpectedSize(sourceStatesNode.size());
    for (JsonNode sourceStateNode : sourceStatesNode) {
      sourceStates.add(SourceStateParser.fromJson(sourceStateNode));
    }

    return ImmutableRefreshState.of(viewVersionId, sourceStates, refreshStartTimestampMs);
  }
}
