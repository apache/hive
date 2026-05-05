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

package org.apache.hive.kubernetes.operator.model.spec;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;

/** Hadoop configuration overrides, primarily for core-site.xml (S3A, filesystem settings). */
public class HadoopSpec {

  @JsonPropertyDescription("Key-value pairs written into core-site.xml")
  private Map<String, String> coreSiteOverrides;

  public Map<String, String> getCoreSiteOverrides() {
    return coreSiteOverrides;
  }

  public void setCoreSiteOverrides(Map<String, String> coreSiteOverrides) {
    this.coreSiteOverrides = coreSiteOverrides;
  }
}
