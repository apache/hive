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
package org.apache.hadoop.hive.druid.serde;

import io.druid.java.util.common.granularity.PeriodGranularity;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;

import org.joda.time.DateTimeZone;

import java.io.IOException;

public class PeriodGranularitySerializer extends JsonSerializer<PeriodGranularity> {

  @Override
  public void serialize(PeriodGranularity granularity, JsonGenerator jsonGenerator,
          SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
    // Set timezone based on user timezone if origin is not already set
    // as it is default Hive time semantics to consider user timezone.
    PeriodGranularity granularityWithUserTimezone = new PeriodGranularity(
            granularity.getPeriod(),
            granularity.getOrigin(),
            DateTimeZone.getDefault()
    );
    granularityWithUserTimezone.serialize(jsonGenerator, serializerProvider);
  }

  @Override
  public void serializeWithType(PeriodGranularity value, JsonGenerator gen,
          SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
    serialize(value, gen, serializers);
  }
}


