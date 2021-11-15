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

package org.apache.hadoop.hive.ql.metadata.formatting;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Format table and index information for machine readability using
 * json.
 */
public class JsonMetaDataFormatter implements MetaDataFormatter {
  /**
   * Convert the map to a JSON string.
   */
  private void asJson(OutputStream out, Map<String, Object> data) throws HiveException {
    try {
      new ObjectMapper().writeValue(out, data);
    } catch (IOException e) {
      throw new HiveException("Unable to convert to json", e);
    }
  }

  /**
   * Write an error message.
   */
  @Override
  public void error(OutputStream out, String msg, int errorCode, String sqlState) throws HiveException {
    error(out, msg, errorCode, sqlState, null);
  }

  @Override
  public void error(OutputStream out, String errorMessage, int errorCode, String sqlState, String errorDetail)
      throws HiveException {
    MapBuilder mb = MapBuilder.create().put("error", errorMessage);
    if (errorDetail != null) {
      mb.put("errorDetail", errorDetail);
    }
    mb.put("errorCode", errorCode);
    if (sqlState != null) {
      mb.put("sqlState", sqlState);
    }
    asJson(out, mb.build());
  }

  @Override
  public void showErrors(DataOutputStream out, WMValidateResourcePlanResponse response) throws HiveException {
    try (JsonGenerator generator = new ObjectMapper().getJsonFactory().createJsonGenerator(out)) {
      generator.writeStartObject();

      generator.writeArrayFieldStart("errors");
      for (String error : response.getErrors()) {
        generator.writeString(error);
      }
      generator.writeEndArray();

      generator.writeArrayFieldStart("warnings");
      for (String error : response.getWarnings()) {
        generator.writeString(error);
      }
      generator.writeEndArray();

      generator.writeEndObject();
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }
}
