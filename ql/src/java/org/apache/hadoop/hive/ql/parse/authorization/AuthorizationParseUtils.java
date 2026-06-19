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
package org.apache.hadoop.hive.ql.parse.authorization;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.ddl.privilege.PrincipalDesc;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;

/**
 * Utility functions for creating objects relevant for authorization operations
 * from AST
 */
public class AuthorizationParseUtils {

  public static PrincipalDesc getPrincipalDesc(ASTNode principal) {
    PrincipalType type = getPrincipalType(principal);
    if (type != null) {
      String text = principal.getChild(0).getText();
      String principalName = BaseSemanticAnalyzer.unescapeIdentifier(text);
      return new PrincipalDesc(principalName, type);
    }
    return null;
  }

  private static PrincipalType getPrincipalType(ASTNode principal) {
    switch (principal.getType()) {
    case HiveParser.TOK_USER:
      return PrincipalType.USER;
    case HiveParser.TOK_GROUP:
      return PrincipalType.GROUP;
    case HiveParser.TOK_ROLE:
      return PrincipalType.ROLE;
    default:
      return null;
    }
  }

  public static List<PrincipalDesc> analyzePrincipalListDef(ASTNode node) {
    List<PrincipalDesc> principalList = new ArrayList<PrincipalDesc>();
    for (int i = 0; i < node.getChildCount(); i++) {
      principalList.add(getPrincipalDesc((ASTNode) node.getChild(i)));
    }
    return principalList;
  }

}
