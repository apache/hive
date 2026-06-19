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
package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.QB;

public class ExplainSQRewriteWork implements Serializable {
  private static final long serialVersionUID = 1L;
  
  private String resFile;
  private QB qb;
  private ASTNode ast;
  private Context ctx;
  
  
  public ExplainSQRewriteWork() {
  }

  public ExplainSQRewriteWork(String resFile, QB qb, ASTNode ast, Context ctx) {
    this.resFile = resFile;
    this.qb = qb;
    this.ast = ast;
    this.ctx = ctx;
  }

  public String getResFile() {
    return resFile;
  }
  
  public QB getQb() {
    return qb;
  }

  public ASTNode getAst() {
    return ast;
  }

  public Context getCtx() {
    return ctx;
  }
  
}
