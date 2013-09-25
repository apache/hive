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
package org.apache.hadoop.hive.serde2.typeinfo;

import java.io.Serializable;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Writable;

/**
 * Base type for type-specific params, such as char(10) or decimal(10, 2).
 */
public abstract class BaseTypeParams implements Writable, Serializable {

  private static final long serialVersionUID = 1L;

  public abstract void validateParams() throws SerDeException;

  public abstract void populateParams(String[] params) throws SerDeException;

  public abstract String toString();

  public void set(String[] params) throws SerDeException {
    populateParams(params);
    validateParams();
  }

  // Needed for conversion to/from TypeQualifiers. Override in subclasses.
  public boolean hasCharacterMaximumLength() {
    return false;
  }
  public int getCharacterMaximumLength() {
    return -1;
  }
}
