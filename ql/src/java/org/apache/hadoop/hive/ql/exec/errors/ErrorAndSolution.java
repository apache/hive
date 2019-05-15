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

package org.apache.hadoop.hive.ql.exec.errors;

import java.util.Objects;

/**
 * Immutable class for storing a possible error and a resolution suggestion.
 */
public class ErrorAndSolution {

  private final String error;
  private final String solution;

  ErrorAndSolution(String error, String solution) {
    this.error = error;
    this.solution = solution;
  }

  public String getError() {
    return error;
  }

  public String getSolution() {
    return solution;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ErrorAndSolution)) {
      return false;
    }
    ErrorAndSolution e = (ErrorAndSolution)o;
    return Objects.equals(e.error, error) && Objects.equals(e.solution, solution);
  }

  @Override
  public int hashCode() {
    return error.hashCode() * 37 + solution.hashCode();
  }
}
