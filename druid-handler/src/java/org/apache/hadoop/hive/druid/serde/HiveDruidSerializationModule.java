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
import io.druid.query.spec.LegacySegmentSpec;

import com.fasterxml.jackson.core.util.VersionUtil;
import com.fasterxml.jackson.databind.module.SimpleModule;

import org.joda.time.Interval;

/**
 * This class is used to define/override any serde behavior for classes from druid.
 * Currently it is used to override the default behavior when serializing PeriodGranularity to include user timezone.
 */
public class HiveDruidSerializationModule extends SimpleModule {
  private static final String NAME = "HiveDruidSerializationModule";
  private static final VersionUtil VERSION_UTIL = new VersionUtil() {};

  public HiveDruidSerializationModule() {
    super(NAME, VERSION_UTIL.version());
    addSerializer(PeriodGranularity.class, new PeriodGranularitySerializer());
  }
}