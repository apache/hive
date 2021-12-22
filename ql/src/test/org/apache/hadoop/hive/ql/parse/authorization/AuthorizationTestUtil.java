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

import java.util.List;

import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;

/**
 * Util function for authorization tests
 */
public class AuthorizationTestUtil {

  public static DDLWork analyze(ASTNode ast, QueryState queryState, Hive db) throws Exception {
    BaseSemanticAnalyzer analyzer = DDLSemanticAnalyzerFactory.getAnalyzer(ast, queryState, db);
    SessionState.start(queryState.getConf());
    analyzer.analyze(ast, new Context(queryState.getConf()));
    List<Task<?>> rootTasks = analyzer.getRootTasks();
    return (DDLWork) inList(rootTasks).ofSize(1).get(0).getWork();
  }

  public static DDLWork analyze(String command, QueryState queryState, Hive db, Context ctx) throws Exception {
    return analyze(parse(command, ctx), queryState, db);
  }

  private static ASTNode parse(String command, Context ctx) throws Exception {
    return ParseUtils.parse(command, ctx);
  }

  /**
   * Helper class that lets you check the size and return the list in one line.
   *
   * @param <E>
   */
  public static class ListSizeMatcher<E> {
    private final List<E> list;
    private ListSizeMatcher(List<E> list) {
      this.list = list;
    }
    private List<E> ofSize(int size) {
      Assert.assertEquals(list.toString(),  size, list.size());
      return list;
    }
  }

  public static <E> ListSizeMatcher<E> inList(List<E> list) {
    return new ListSizeMatcher<E>(list);
  }

}

