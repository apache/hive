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

package org.apache.hadoop.hive.ql.ddl;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.reflections.Reflections;

/**
 * Manages the DDL command analyzers.
 */
public final class DDLSemanticAnalyzerFactory {
  private DDLSemanticAnalyzerFactory() {
    throw new UnsupportedOperationException("DDLSemanticAnalyzerFactory should not be instantiated");
  }

  /**
   * Annotation for the handled type by the analyzer.
   */
  @Retention(RetentionPolicy.RUNTIME)
  public @interface DDLType {
    int type();
  }

  private static final Map<Integer, Class<? extends BaseSemanticAnalyzer>> TYPE_TO_ANALYZER = new HashMap<>();

  static {
    Set<Class<? extends BaseSemanticAnalyzer>> analyzerClasses =
        new Reflections("org.apache.hadoop.hive.ql.ddl").getSubTypesOf(BaseSemanticAnalyzer.class);
    for (Class<? extends BaseSemanticAnalyzer> analyzerClass : analyzerClasses) {
      if (Modifier.isAbstract(analyzerClass.getModifiers())) {
        continue;
      }

      DDLType ddlType = analyzerClass.getAnnotation(DDLType.class);
      TYPE_TO_ANALYZER.put(ddlType.type(), analyzerClass);
    }
  }

  public static boolean handles(int type) {
    return TYPE_TO_ANALYZER.containsKey(type);
  }

  public static BaseSemanticAnalyzer getAnalyzer(ASTNode root, QueryState queryState) {
    Class<? extends BaseSemanticAnalyzer> analyzerClass = TYPE_TO_ANALYZER.get(root.getType());
    try {
      BaseSemanticAnalyzer analyzer = analyzerClass.getConstructor(QueryState.class).newInstance(queryState);
      return analyzer;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
