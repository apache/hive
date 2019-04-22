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

package org.apache.hadoop.hive.ql.security.authorization;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * Privilege type
 */
public enum PrivilegeType {

  ALL(HiveParser.TOK_PRIV_ALL, "All"),
  ALTER_DATA(HiveParser.TOK_PRIV_ALTER_DATA, "Update"),
  ALTER_METADATA(HiveParser.TOK_PRIV_ALTER_METADATA, "Alter"),
  CREATE(HiveParser.TOK_PRIV_CREATE, "Create"),
  DROP(HiveParser.TOK_PRIV_DROP, "Drop"),
  LOCK(HiveParser.TOK_PRIV_LOCK, "Lock"),
  SELECT(HiveParser.TOK_PRIV_SELECT, "Select"),
  SHOW_DATABASE(HiveParser.TOK_PRIV_SHOW_DATABASE, "Show_Database"),
  INSERT(HiveParser.TOK_PRIV_INSERT, "Insert"),
  DELETE(HiveParser.TOK_PRIV_DELETE, "Delete"),
  UNKNOWN(null, null);

  private final String name;
  private final Integer token;

  PrivilegeType(Integer token, String name){
    this.name = name;
    this.token = token;
  }

  @Override
  @Explain(displayName = "type", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String toString(){
    return name == null ? "unkown" : name;
  }

  public Integer getToken() {
    return token;
  }

  private static Map<Integer, PrivilegeType> token2Type;
  private static Map<String, PrivilegeType> name2Type;

  /**
   * Do case lookup of PrivilegeType associated with this antlr token
   * @param token
   * @return corresponding PrivilegeType
   */
  public static PrivilegeType getPrivTypeByToken(int token) {
    populateToken2Type();
    PrivilegeType privType = token2Type.get(token);
    if(privType != null){
      return privType;
    }
    return PrivilegeType.UNKNOWN;
  }

  private static synchronized void populateToken2Type() {
    if(token2Type != null){
      return;
    }
    token2Type = new HashMap<Integer, PrivilegeType>();
    for(PrivilegeType privType : PrivilegeType.values()){
      token2Type.put(privType.getToken(), privType);
    }
  }

  /**
   * Do case insensitive lookup of PrivilegeType with this name
   * @param privilegeName
   * @return corresponding PrivilegeType
   */
  public static PrivilegeType getPrivTypeByName(String privilegeName) {
    populateName2Type();
    String canonicalizedName = privilegeName.toLowerCase();
    PrivilegeType privType = name2Type.get(canonicalizedName);
    if(privType != null){
      return privType;
    }
    return PrivilegeType.UNKNOWN;
  }

  private static synchronized void populateName2Type() {
    if(name2Type != null){
      return;
    }
    name2Type = new HashMap<String, PrivilegeType>();
    for(PrivilegeType privType : PrivilegeType.values()){
      name2Type.put(privType.toString().toLowerCase(), privType);
    }
  }
}
