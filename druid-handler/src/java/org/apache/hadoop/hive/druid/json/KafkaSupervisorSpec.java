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

package org.apache.hadoop.hive.druid.json;

import org.apache.druid.segment.indexing.DataSchema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;

import java.util.Map;

/**
 * This class is copied from druid source code
 * in order to avoid adding additional dependencies on druid-indexing-service.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type") @JsonSubTypes({
    @JsonSubTypes.Type(name = "kafka", value = KafkaSupervisorSpec.class) }) public class KafkaSupervisorSpec {
  private final DataSchema dataSchema;
  private final KafkaSupervisorTuningConfig tuningConfig;
  private final KafkaSupervisorIOConfig ioConfig;
  private final Map<String, Object> context;

  @JsonCreator public KafkaSupervisorSpec(@JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") KafkaSupervisorTuningConfig tuningConfig,
      @JsonProperty("ioConfig") KafkaSupervisorIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context) {
    this.dataSchema = Preconditions.checkNotNull(dataSchema, "dataSchema");
    this.tuningConfig =
        tuningConfig != null ?
            tuningConfig :
            new KafkaSupervisorTuningConfig(null,
                null,
                null,
                null,
                null,
                null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
                null, null);
    this.ioConfig = Preconditions.checkNotNull(ioConfig, "ioConfig");
    this.context = context;
  }

  @JsonProperty public DataSchema getDataSchema() {
    return dataSchema;
  }

  @JsonProperty public KafkaSupervisorTuningConfig getTuningConfig() {
    return tuningConfig;
  }

  @JsonProperty public KafkaSupervisorIOConfig getIoConfig() {
    return ioConfig;
  }

  @JsonProperty public Map<String, Object> getContext() {
    return context;
  }

  @Override public String toString() {
    return "KafkaSupervisorSpec{"
        + "dataSchema="
        + dataSchema
        + ", tuningConfig="
        + tuningConfig
        + ", ioConfig="
        + ioConfig
        + '}';
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    KafkaSupervisorSpec that = (KafkaSupervisorSpec) o;

    if (dataSchema != null ? !dataSchema.equals(that.dataSchema) : that.dataSchema != null) {
      return false;
    }
    if (tuningConfig != null ? !tuningConfig.equals(that.tuningConfig) : that.tuningConfig != null) {
      return false;
    }
    if (ioConfig != null ? !ioConfig.equals(that.ioConfig) : that.ioConfig != null) {
      return false;
    }
    return context != null ? context.equals(that.context) : that.context == null;
  }

  @Override public int hashCode() {
    int result = dataSchema != null ? dataSchema.hashCode() : 0;
    result = 31 * result + (tuningConfig != null ? tuningConfig.hashCode() : 0);
    result = 31 * result + (ioConfig != null ? ioConfig.hashCode() : 0);
    result = 31 * result + (context != null ? context.hashCode() : 0);
    return result;
  }
}
