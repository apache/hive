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
package org.apache.hadoop.hive.druid;

import java.util.List;

import org.apache.hadoop.hive.druid.serde.DruidSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.util.StringUtils;

import com.fasterxml.jackson.core.type.TypeReference;

import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;

/**
 * Druid SerDe to be used in tests.
 */
public class QTestDruidSerDe2 extends DruidSerDe {

  // Request :
  //        "{\"queryType\":\"segmentMetadata\",\"dataSource\":{\"type\":\"table\",\"name\":\"wikipedia\"},"
  //        + "\"intervals\":{\"type\":\"intervals\","
  //        + "\"intervals\":[\"-146136543-09-08T00:30:34.096-07:52:58/146140482-04-24T08:36:27.903-07:00\"]},"
  //        + "\"toInclude\":{\"type\":\"all\"},\"merge\":true,\"context\":null,\"analysisTypes\":[],"
  //        + "\"usingDefaultInterval\":true,\"lenientAggregatorMerge\":false,\"descending\":false}";
  private static final String RESPONSE =
          "[ {\r\n "
                  + " \"id\" : \"merged\",\r\n "
                  + " \"intervals\" : [ \"2010-01-01T00:00:00.000Z/2015-12-31T00:00:00.000Z\" ],\r\n "
                  + " \"columns\" : {\r\n  "
                  + "  \"__time\" : { \"type\" : \"LONG\", \"hasMultipleValues\" : false, \"size\" : 407240380, \"cardinality\" : null, \"errorMessage\" : null },\r\n  "
                  + "  \"robot\" : { \"type\" : \"STRING\", \"hasMultipleValues\" : false, \"size\" : 100000, \"cardinality\" : 1944, \"errorMessage\" : null },\r\n  "
                  + "  \"namespace\" : { \"type\" : \"STRING\", \"hasMultipleValues\" : true, \"size\" : 100000, \"cardinality\" : 1504, \"errorMessage\" : null },\r\n  "
                  + "  \"anonymous\" : { \"type\" : \"STRING\", \"hasMultipleValues\" : false, \"size\" : 100000, \"cardinality\" : 1944, \"errorMessage\" : null },\r\n  "
                  // Next column has a similar name as previous, but different casing.
                  // This is allowed in Druid, but it should fail in Hive.
                  + "  \"Anonymous\" : { \"type\" : \"STRING\", \"hasMultipleValues\" : false, \"size\" : 100000, \"cardinality\" : 1944, \"errorMessage\" : null },\r\n  "
                  + "  \"page\" : { \"type\" : \"STRING\", \"hasMultipleValues\" : false, \"size\" : 100000, \"cardinality\" : 1944, \"errorMessage\" : null },\r\n  "
                  + "  \"language\" : { \"type\" : \"STRING\", \"hasMultipleValues\" : false, \"size\" : 100000, \"cardinality\" : 1944, \"errorMessage\" : null },\r\n  "
                  + "  \"newpage\" : { \"type\" : \"STRING\", \"hasMultipleValues\" : false, \"size\" : 100000, \"cardinality\" : 1944, \"errorMessage\" : null },\r\n  "
                  + "  \"user\" : { \"type\" : \"STRING\", \"hasMultipleValues\" : false, \"size\" : 100000, \"cardinality\" : 1944, \"errorMessage\" : null },\r\n  "
                  + "  \"count\" : { \"type\" : \"FLOAT\", \"hasMultipleValues\" : false, \"size\" : 100000, \"cardinality\" : null, \"errorMessage\" : null },\r\n  "
                  + "  \"added\" : { \"type\" : \"FLOAT\", \"hasMultipleValues\" : false, \"size\" : 100000, \"cardinality\" : null, \"errorMessage\" : null },\r\n  "
                  + "  \"delta\" : { \"type\" : \"FLOAT\", \"hasMultipleValues\" : false, \"size\" : 100000, \"cardinality\" : null, \"errorMessage\" : null },\r\n  "
                  + "  \"variation\" : { \"type\" : \"FLOAT\", \"hasMultipleValues\" : false, \"size\" : 100000, \"cardinality\" : null, \"errorMessage\" : null },\r\n  "
                  + "  \"deleted\" : { \"type\" : \"FLOAT\", \"hasMultipleValues\" : false, \"size\" : 100000, \"cardinality\" : null, \"errorMessage\" : null }\r\n "
                  + " },\r\n "
                  + " \"aggregators\" : {\r\n  "
                  + "  \"count\" : { \"type\" : \"longSum\", \"name\" : \"count\", \"fieldName\" : \"count\" },\r\n  "
                  + "  \"added\" : { \"type\" : \"doubleSum\", \"name\" : \"added\", \"fieldName\" : \"added\" },\r\n  "
                  + "  \"delta\" : { \"type\" : \"doubleSum\", \"name\" : \"delta\", \"fieldName\" : \"delta\" },\r\n  "
                  + "  \"variation\" : { \"type\" : \"doubleSum\", \"name\" : \"variation\", \"fieldName\" : \"variation\" },\r\n  "
                  + "  \"deleted\" : { \"type\" : \"doubleSum\", \"name\" : \"deleted\", \"fieldName\" : \"deleted\" }\r\n "
                  + " },\r\n "
                  + " \"queryGranularity\" : {\r\n    \"type\": \"none\"\r\n  },\r\n "
                  + " \"size\" : 300000,\r\n "
                  + " \"numRows\" : 5000000\r\n} ]";

  /* Submits the request and returns */
  @Override
  protected SegmentAnalysis submitMetadataRequest(String address, SegmentMetadataQuery query)
          throws SerDeException {
    // Retrieve results
    List<SegmentAnalysis> resultsList;
    try {
      resultsList = DruidStorageHandlerUtils.JSON_MAPPER.readValue(RESPONSE,
              new TypeReference<List<SegmentAnalysis>>() {
              }
      );
    } catch (Exception e) {
      throw new SerDeException(StringUtils.stringifyException(e));
    }
    return resultsList.get(0);
  }

}
