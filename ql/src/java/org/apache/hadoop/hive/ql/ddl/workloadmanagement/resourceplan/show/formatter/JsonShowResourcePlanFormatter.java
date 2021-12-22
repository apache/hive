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

package org.apache.hadoop.hive.ql.ddl.workloadmanagement.resourceplan.show.formatter;

import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Formats SHOW RESOURCE PLAN(S) results to json format.
 */
public class JsonShowResourcePlanFormatter extends ShowResourcePlanFormatter {
  @Override
  public void showResourcePlans(DataOutputStream out, List<WMResourcePlan> resourcePlans) throws HiveException {
    try (JsonGenerator generator = new ObjectMapper().getFactory().createJsonGenerator(out)) {
      generator.writeStartArray();
      for (WMResourcePlan plan : resourcePlans) {
        generator.writeStartObject();
        generator.writeStringField("name", plan.getName());
        generator.writeStringField("status", plan.getStatus().name());
        if (plan.isSetQueryParallelism()) {
          generator.writeNumberField("queryParallelism", plan.getQueryParallelism());
        }
        if (plan.isSetDefaultPoolPath()) {
          generator.writeStringField("defaultPoolPath", plan.getDefaultPoolPath());
        }
        generator.writeEndObject();
      }
      generator.writeEndArray();
      generator.close();
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  @Override
  public void showFullResourcePlan(DataOutputStream out, WMFullResourcePlan resourcePlan) throws HiveException {
    try (JsonRPFormatter formatter = new JsonRPFormatter(out)) {
      formatFullRP(formatter, resourcePlan);
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  /**
   * Formats a resource plan into a json object, the structure is as follows:
   * {
   *    name: "<rp_name>",
   *    parallelism: "<parallelism>",
   *    defaultQueue: "<defaultQueue>",
   *    pools : [
   *      {
   *        name: "<pool_name>",
   *        parallelism: "<parallelism>",
   *        schedulingPolicy: "<policy>",
   *        triggers: [
   *          { name: "<triggerName>", trigger: "<trigExpression>", action: "<actionExpr">}
   *          ...
   *        ]
   *      }
   *      ...
   *    ]
   * }
   */
  private static class JsonRPFormatter implements RPFormatter, Closeable {
    private final JsonGenerator generator;

    JsonRPFormatter(DataOutputStream out) throws IOException {
      generator = new ObjectMapper().getJsonFactory().createJsonGenerator(out);
    }

    private void writeNameAndFields(String name, Object ... keyValuePairs) throws IOException {
      if (keyValuePairs.length % 2 != 0) {
        throw new IllegalArgumentException("Expected pairs");
      }
      generator.writeStringField("name", name);
      for (int i = 0; i < keyValuePairs.length; i += 2) {
        generator.writeObjectField(keyValuePairs[i].toString(), keyValuePairs[i + 1]);
      }
    }

    @Override
    public void startRP(String rpName, Object ... keyValuePairs) throws IOException {
      generator.writeStartObject();
      writeNameAndFields(rpName, keyValuePairs);
    }

    @Override
    public void endRP() throws IOException {
      generator.writeEndObject();
    }

    @Override
    public void startPools() throws IOException {
      generator.writeArrayFieldStart("pools");
    }

    @Override
    public void endPools() throws IOException {
      generator.writeEndArray();
    }

    @Override
    public void startPool(String poolName, Object ... keyValuePairs) throws IOException {
      generator.writeStartObject();
      writeNameAndFields(poolName, keyValuePairs);
    }

    @Override
    public void startTriggers() throws IOException {
      generator.writeArrayFieldStart("triggers");
    }

    @Override
    public void endTriggers() throws IOException {
      generator.writeEndArray();
    }

    @Override
    public void startMappings() throws IOException {
      generator.writeArrayFieldStart("mappings");
    }

    @Override
    public void endMappings() throws IOException {
      generator.writeEndArray();
    }

    @Override
    public void endPool() throws IOException {
      generator.writeEndObject();
    }

    @Override
    public void formatTrigger(String triggerName, String actionExpression, String triggerExpression)
        throws IOException {
      generator.writeStartObject();
      writeNameAndFields(triggerName, "action", actionExpression, "trigger", triggerExpression);
      generator.writeEndObject();
    }

    @Override
    public void formatMappingType(String type, List<String> names) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type);
      generator.writeArrayFieldStart("values");
      for (String name : names) {
        generator.writeString(name);
      }
      generator.writeEndArray();
      generator.writeEndObject();
    }

    @Override
    public void close() throws IOException {
      generator.close();
    }
  }
}
