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
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class SourceStateParser {

  private static final String TYPE = "type";
  private static final String NAME = "name";
  private static final String NAMESPACE = "namespace";
  private static final String CATALOG = "catalog";
  private static final String UUID = "uuid";
  private static final String SNAPSHOT_ID = "snapshot-id";
  private static final String REF = "ref";
  private static final String VERSION_ID = "version-id";

  private SourceStateParser() {

  }

  public static void toJson(SourceState sourceState, JsonGenerator generator) throws IOException {
    Preconditions.checkArgument(sourceState != null, "Cannot serialize null source state");

    generator.writeStartObject();
    generator.writeStringField(TYPE, sourceState.type().toString());
    generator.writeStringField(NAME, sourceState.name());
    generator.writeStringField(NAMESPACE, sourceState.nameSpace());

    if (!StringUtils.isEmpty(sourceState.catalog())) {
      generator.writeStringField(CATALOG, sourceState.catalog());
    }

    generator.writeStringField(UUID, sourceState.uuid().toString());

    switch (sourceState.type()) {
      case TABLE -> {
        generator.writeNumberField(SNAPSHOT_ID, sourceState.snapshotId());

        if (!StringUtils.isEmpty(sourceState.ref())) {
          generator.writeStringField(REF, sourceState.ref());
        }
      }
      case VIEW -> generator.writeNumberField(VERSION_ID, sourceState.versionId());
    }

    generator.writeEndObject();
  }

  public static SourceState fromJson(JsonNode node) {
    Preconditions.checkArgument(
        node != null,
        "Cannot parse source state representation from null string!");
    Preconditions.checkArgument(
        node.isObject(), "Cannot parse source state from a non-object: %s", node);

    String typeRepresentation = JsonUtil.getString(TYPE, node);
    SourceState.SourceStateType type = SourceState.SourceStateType.valueOf(typeRepresentation.toUpperCase());
    String name = JsonUtil.getString(NAME, node);
    String namespace = JsonUtil.getString(NAMESPACE, node);
    String catalog = JsonUtil.getStringOrNull(CATALOG, node);
    java.util.UUID uuid = java.util.UUID.fromString(JsonUtil.getString(UUID, node));
    Long snapshotId = JsonUtil.getLong(SNAPSHOT_ID, node);
    String ref = JsonUtil.getStringOrNull(REF, node);
    Integer versionId = JsonUtil.getIntOrNull(VERSION_ID, node);

    return ImmutableSourceState.of(type, name, namespace, catalog, uuid, snapshotId, ref, versionId);
  }

}
