/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.avro;

import org.apache.avro.Schema;

class SchemaResolutionProblem {
  static final String sentinelString = "{\n" +
        "    \"namespace\": \"org.apache.hadoop.hive\",\n" +
        "    \"name\": \"CannotDetermineSchemaSentinel\",\n" +
        "    \"type\": \"record\",\n" +
        "    \"fields\": [\n" +
        "        {\n" +
        "            \"name\":\"ERROR_ERROR_ERROR_ERROR_ERROR_ERROR_ERROR\",\n" +
        "            \"type\":\"string\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"name\":\"Cannot_determine_schema\",\n" +
        "            \"type\":\"string\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"name\":\"check\",\n" +
        "            \"type\":\"string\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"name\":\"schema\",\n" +
        "            \"type\":\"string\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"name\":\"url\",\n" +
        "            \"type\":\"string\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"name\":\"and\",\n" +
        "            \"type\":\"string\"\n" +
        "        },\n" +
        "        {\n" +
        "            \"name\":\"literal\",\n" +
        "            \"type\":\"string\"\n" +
        "        }\n" +
        "    ]\n" +
        "}";
  public final static Schema SIGNAL_BAD_SCHEMA = Schema.parse(sentinelString);
}
