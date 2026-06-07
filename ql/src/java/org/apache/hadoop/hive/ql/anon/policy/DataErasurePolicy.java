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

package org.apache.hadoop.hive.ql.anon.policy;

import org.apache.hive.hep.ErasurePolicyValidator;
import org.apache.hive.hep.ParsedPolicy;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataErasurePolicy {

  public static final ObjectMapper mapper = new ObjectMapper();

  public String identityFieldName;
  public String identityFieldType;
  public String schemaFieldType;
  public List<DataErasureStatement> statements = new ArrayList<>();

  private Set<WritableComparable> schemaIds;
  private Map<WritableComparable, DataErasureStatement> mapStmt;

  public static DataErasurePolicy fromDsl(final String dsl) {
    final ParsedPolicy parsed = ErasurePolicyValidator.parse(dsl);
    final DataErasurePolicy policy = new DataErasurePolicy();
    policy.identityFieldName = parsed.getIdentityFieldName();
    policy.identityFieldType = parsed.getIdentityFieldType().name().toLowerCase();
    policy.schemaFieldType = parsed.getSchemaType().name().toLowerCase();
    for (final ParsedPolicy.Statement s : parsed.getStatements()) {
      final DataErasureStatement stmt = new DataErasureStatement();
      stmt.schemaId = s.schemaId;
      for (final ParsedPolicy.Rule r : s.rules) {
        final DataErasureRule rule = new DataErasureRule();
        rule.path = r.fieldPath;
        rule.action = r.action == null ? null : r.action.name();
        rule.value = r.replaceValue;
        stmt.rules.add(rule);
      }
      policy.statements.add(stmt);
    }
    return policy;
  }

  public static DataErasurePolicy fromString(String jsonPolicy) {
    try {
      return mapper.readValue(jsonPolicy, DataErasurePolicy.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean hasId(final WritableComparable schemaId) {
    if (schemaIds == null) {
      initSet();
    }
    return schemaIds.contains(schemaId);
  }

  private void initSet() {
    schemaIds = new HashSet<>();
    mapStmt = new HashMap<>();
    for (DataErasureStatement statement : statements) {
      final String schId = statement.schemaId;
      final WritableComparable id = createSchemaId(schId);
      schemaIds.add(id);
      mapStmt.put(id, statement);
    }
  }

  private WritableComparable createSchemaId(final String schId) {
    switch (schemaFieldType) {
      case "int":
        return new IntWritable(Integer.parseInt(schId));
      case "string":
        return new Text(schId);
      default:
        throw new RuntimeException("unsupported schema field type: " + schemaFieldType);
    }
  }

  public DataErasureStatement getStmt(final WritableComparable schemaId) {
    return mapStmt.get(schemaId);
  }
}

