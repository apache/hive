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

import org.apache.commons.lang3.StringUtils;
import org.apache.hive.search.exception.SearchException;
import org.apache.hadoop.hive.common.TableName;

/** Narrow public search contract (Thrift v1 semantics). */
public record SearchQuery(
    String queryText,
    SearchReqResp.Mode mode,
    String catalogName,
    String databaseName,
    int limit,
    List<String> returnFields) {

  public SearchQuery {
    mode = mode == null ? SearchReqResp.Mode.HYBRID : mode;
    returnFields = returnFields == null ? List.of() : List.copyOf(returnFields);
  }

  private static void validate(String queryText, int limit) throws SearchException {
    if (StringUtils.isEmpty(queryText)) {
      throw new SearchException("queryText is required");
    }
    if (limit < 0) {
      throw new SearchException("limit must be non-negative");
    }
  }

  public static SearchQuery of(String queryText) throws SearchException {
    validate(queryText, 0);
    return new SearchQuery(queryText, SearchReqResp.Mode.HYBRID, null, null, 0, List.of());
  }

  public static SearchQuery of(String queryText, SearchReqResp.Mode mode, int limit)
      throws SearchException {
    validate(queryText, limit);
    return new SearchQuery(queryText, mode, null, null, limit, List.of());
  }

  public static SearchQuery of(TableName tableName) throws SearchException {
    validate(tableName.getTable(), 0);
    return new SearchQuery(tableName.getTable(), SearchReqResp.Mode.KEYWORD,
        tableName.getCat(), tableName.getDb(), 0, List.of());
  }
}
