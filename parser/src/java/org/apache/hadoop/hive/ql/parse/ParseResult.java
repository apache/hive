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
package org.apache.hadoop.hive.ql.parse;

import java.util.List;
import org.antlr.runtime.TokenRewriteStream;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Contains result of {@link ParseDriver#parse(String)}.
 */
public class ParseResult {
  private final ASTNode tree;
  private final TokenRewriteStream tokenRewriteStream;
  private final List<Pair<String, String>> tables;

  public ParseResult(ASTNode tree, TokenRewriteStream tokenRewriteStream,
      List<Pair<String, String>> tables) {
    this.tree = tree;
    this.tokenRewriteStream = tokenRewriteStream;
    this.tables = tables;
  }

  public ASTNode getTree() {
    return tree;
  }

  public TokenRewriteStream getTokenRewriteStream() {
    return tokenRewriteStream;
  }

  public List<Pair<String, String>> getTables() {
    return tables;
  }
}
