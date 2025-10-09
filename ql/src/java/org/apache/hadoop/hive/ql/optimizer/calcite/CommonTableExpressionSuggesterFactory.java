/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.conf.HiveConf;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;

@InterfaceAudience.Private
public final class CommonTableExpressionSuggesterFactory {

  private CommonTableExpressionSuggesterFactory() {
    throw new IllegalStateException("Must not instantiate");
  }

  public static CommonTableExpressionSuggester create(HiveConf configuration) {
    String suggestName = configuration.getVar(HiveConf.ConfVars.HIVE_CTE_SUGGESTER_CLASS);
    if (suggestName == null || suggestName.isEmpty()) {
      return (query, conf) -> Collections.emptyList();
    }
    try {
      Class<?> suggesterClass = Class.forName(suggestName);
      if (CommonTableExpressionSuggester.class.isAssignableFrom(suggesterClass)) {
        return (CommonTableExpressionSuggester) suggesterClass.getDeclaredConstructor().newInstance();
      }
      throw new IllegalArgumentException(
          suggesterClass.getSimpleName() + " must implement " + CommonTableExpressionSuggester.class.getSimpleName());
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      throw new CalciteCteException("Failed to instantiate suggester from class: " + suggestName, e);
    }
  }
}
