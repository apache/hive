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

import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSubqueryRuntimeException;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSubquerySemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteViewSemanticException;

/**
 * A strategy defining when CBO fallbacks to the legacy optimizer.
 */
public enum CBOFallbackStrategy {
  /**
   * Never use the legacy optimizer, all CBO errors are fatal.
   */
  NEVER {
    @Override
    boolean isFatal(Exception e) {
      return true;
    }

    @Override
    public boolean allowsRetry() {
      return false;
    }
  },
  /**
   * Use the legacy optimizer only when the CBO exception is not related to subqueries and views.
   */
  CONSERVATIVE {
    @Override
    boolean isFatal(Exception e) {
      // Non-CBO path for the following exceptions fail with completely different error and mask the original failure
      return e instanceof CalciteSubquerySemanticException || e instanceof CalciteViewSemanticException
          || e instanceof CalciteSubqueryRuntimeException;
    }

    @Override
    public boolean allowsRetry() {
      return true;
    }
  },
  /**
   * Always use the legacy optimizer, CBO errors are not fatal.
   */
  ALWAYS {
    @Override
    boolean isFatal(Exception e) {
      return false;
    }

    @Override
    public boolean allowsRetry() {
      return true;
    }
  },
  /**
   * Specific strategy only for tests.
   */
  TEST {
    @Override
    boolean isFatal(Exception e) {
      if (e instanceof CalciteSubquerySemanticException || e instanceof CalciteViewSemanticException
          || e instanceof CalciteSubqueryRuntimeException) {
        return true;
      }
      return !(e instanceof CalciteSemanticException);
    }

    @Override
    public boolean allowsRetry() {
      return true;
    }
  };

  /**
   * Returns true if the specified exception is fatal (must not fallback to legacy optimizer), and false otherwise.
   */
  abstract boolean isFatal(Exception e);

  public abstract boolean allowsRetry();
}
