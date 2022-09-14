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
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.reflections.Reflections;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

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
    int[] types() default {};
  }

  /**
   * Reveals the actual type of an ASTTree that has a category as main element.
   */
  public interface DDLSemanticAnalyzerCategory {
    int getType(ASTNode root);
  }

  private static final String DDL_ROOT = "org.apache.hadoop.hive.ql.ddl";
  private static final Map<Integer, Class<? extends BaseSemanticAnalyzer>> TYPE_TO_ANALYZER = new HashMap<>();
  private static final Map<Integer, Class<? extends DDLSemanticAnalyzerCategory>> TYPE_TO_ANALYZERCATEGORY =
      new HashMap<>();

  static {
    Set<Class<? extends BaseSemanticAnalyzer>> analyzerClasses =
        new Reflections(DDL_ROOT).getSubTypesOf(BaseSemanticAnalyzer.class);
    for (Class<? extends BaseSemanticAnalyzer> analyzerClass : analyzerClasses) {
      if (Modifier.isAbstract(analyzerClass.getModifiers())) {
        continue;
      }

      DDLType ddlType = analyzerClass.getAnnotation(DDLType.class);
      if (ddlType == null) {
        continue;
      }
      for (int type : ddlType.types()) {
        if (TYPE_TO_ANALYZER.containsKey(type)) {
          throw new IllegalStateException(
              "Type " + type + " is declared more than once in different DDLType annotations.");
        }
        TYPE_TO_ANALYZER.put(type, analyzerClass);
      }
    }

    Set<Class<? extends DDLSemanticAnalyzerCategory>> analyzerCategoryClasses =
        new Reflections(DDL_ROOT).getSubTypesOf(DDLSemanticAnalyzerCategory.class);
    for (Class<? extends DDLSemanticAnalyzerCategory> analyzerCategoryClass : analyzerCategoryClasses) {
      if (Modifier.isAbstract(analyzerCategoryClass.getModifiers())) {
        continue;
      }

      DDLType ddlType = analyzerCategoryClass.getAnnotation(DDLType.class);
      if (ddlType == null) {
        continue;
      }
      for (int type : ddlType.types()) {
        if (TYPE_TO_ANALYZERCATEGORY.containsKey(type)) {
          throw new IllegalStateException(
              "Type " + type + " is declared more than once in different DDLType annotations for categories.");
        }
        TYPE_TO_ANALYZERCATEGORY.put(type, analyzerCategoryClass);
      }
    }
  }

  public static boolean handles(ASTNode root) {
    return getAnalyzerClass(root, null) != null;
  }

  public static BaseSemanticAnalyzer getAnalyzer(ASTNode root, QueryState queryState) {
    Class<? extends BaseSemanticAnalyzer> analyzerClass = getAnalyzerClass(root, queryState);
    try {
      BaseSemanticAnalyzer analyzer = analyzerClass.getConstructor(QueryState.class).newInstance(queryState);
      return analyzer;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  public static BaseSemanticAnalyzer getAnalyzer(ASTNode root, QueryState queryState, Hive db) {
    Class<? extends BaseSemanticAnalyzer> analyzerClass = getAnalyzerClass(root, queryState);
    try {
      BaseSemanticAnalyzer analyzer =
          analyzerClass.getConstructor(QueryState.class, Hive.class).newInstance(queryState, db);
      return analyzer;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Class<? extends BaseSemanticAnalyzer> getAnalyzerClass(ASTNode root, QueryState queryState) {
    if (TYPE_TO_ANALYZER.containsKey(root.getType())) {
      return TYPE_TO_ANALYZER.get(root.getType());
    }

    if (TYPE_TO_ANALYZERCATEGORY.containsKey(root.getType())) {
      Class<? extends DDLSemanticAnalyzerCategory> analyzerCategoryClass = TYPE_TO_ANALYZERCATEGORY.get(root.getType());
      try {
        DDLSemanticAnalyzerCategory analyzerCategory = analyzerCategoryClass.newInstance();
        int actualType = analyzerCategory.getType(root);
        if (TYPE_TO_ANALYZER.containsKey(actualType)) {
          if (queryState != null) {
            queryState.setCommandType(HiveOperation.operationForToken(actualType));
          }
          return TYPE_TO_ANALYZER.get(actualType);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    return null;
  }
}
