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

package org.apache.hadoop.hive.ql.ddl.misc.sortoder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hive.ql.ddl.misc.sortoder.SortFieldDesc.SortDirection;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.unescapeIdentifier;

/**
 * Utility class for parsing and serializing sort order specifications.
 * Used by both CREATE TABLE and ALTER TABLE commands to avoid code duplication.
 */
public final class SortOrderUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SortOrderUtils.class);
  private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();

  private SortOrderUtils() {
  }

  /**
   * Parses an AST node containing sort column specifications and converts to JSON.
   * The AST node should contain children of type TOK_TABSORTCOLNAMEASC or TOK_TABSORTCOLNAMEDESC.
   * 
   * @param ast AST node with sort columns
   * @return JSON string representation of SortFields, or null if parsing fails or AST is empty
   */
  public static String parseSortOrderToJson(ASTNode ast) {
    if (ast == null || ast.getChildCount() == 0) {
      return null;
    }

    List<SortFieldDesc> sortFieldDescList = new ArrayList<>();
    
    for (int i = 0; i < ast.getChildCount(); i++) {
      ASTNode child = (ASTNode) ast.getChild(i);
      
      // Determine sort direction from token type
      SortDirection sortDirection = 
          child.getToken().getType() == HiveParser.TOK_TABSORTCOLNAMEDESC 
              ? SortDirection.DESC 
              : SortDirection.ASC;
      
      // Get column spec node
      ASTNode colSpecNode = (ASTNode) child.getChild(0);
      String columnName = unescapeIdentifier(colSpecNode.getChild(0).getText()).toLowerCase();
      NullOrdering nullOrder = NullOrdering.fromToken(colSpecNode.getToken().getType());
      
      sortFieldDescList.add(
          new SortFieldDesc(columnName, sortDirection, nullOrder));
    }

    SortFields sortFields = new SortFields(sortFieldDescList);
    try {
      return JSON_OBJECT_MAPPER.writeValueAsString(sortFields);
    } catch (JsonProcessingException e) {
      LOG.warn("Failed to serialize sort order specification", e);
      return null;
    }
  }
}

