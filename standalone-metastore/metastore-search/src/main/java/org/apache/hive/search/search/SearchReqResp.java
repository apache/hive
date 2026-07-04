/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.search;

import java.util.List;
import java.util.Map;

import org.apache.hive.search.exception.SearchException;

public final class SearchReqResp {
  /** Public search mode exposed on the Metastore Thrift API. */
  public enum Mode {
    KEYWORD,
    SEMANTIC,
    HYBRID
  }

  public record Request(
      Map<String, Object> query,
      List<String> fields,
      int size,
      String catalogName,
      String databaseName) {
    public Request {
      query = query == null ? Map.of() : Map.copyOf(query);
      fields = fields == null ? List.of() : List.copyOf(fields);
    }

    public static Request validated(Map<String, Object> query, List<String> fields, int size,
        String catalogName, String databaseName) throws SearchException {
      if (size < 0) {
        throw new SearchException("size must be non-negative");
      }
      return new Request(query, fields, size, catalogName, databaseName);
    }
  }

  public record Response(List<Map<String, Object>> hits, long total) {}

}
