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
package org.apache.hadoop.hive.conf.valcoersion;

import org.apache.hadoop.conf.Configuration;

/**
 * VariableCoercions are used to enforce rules related to system variables.
 * These rules may transform the value of system properties returned by the
 * {@link org.apache.hadoop.hive.conf.SystemVariables SystemVariables} utility class
 */
public abstract class VariableCoercion {
  private final String name;

  public VariableCoercion(String name) {
    this.name = name;
  }

  public String getName() { return this.name; }

  /**
   * Coerce the original value of the variable
   * @param originalValue the unmodified value
   * @return transformed value
   */
  public abstract String getCoerced(String originalValue);
}
