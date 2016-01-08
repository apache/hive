/**
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

package org.apache.hadoop.hive.ql.hooks;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * PreParseHook allows Hive QL Statements to be extended with custom
 * logic. A list of such hooks can be configured to be
 * called after variable substitution and before query parse,
 * allowing you to change/customize QL statement before hive parse
 */
public interface PreParseHook extends Hook {
  /**
   * Invoked before Hive performs parse analysis on
   * a statement.  The implementation may inspect the SQL command and
   * change/replace/substitute text and creating a new SQL command
   * to be used as hive QL 
   * 
   * @param context context information for statement analysis
   *
   * @param command original QL command
   *
   * @return replacement command (a new command QL statement customized
   * based on original QL command and hook logic)
   */
  public String getCustomCommand(Context context, String command)
    throws Exception;

}

