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

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Exception from SemanticAnalyzer.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SemanticException extends HiveException {

  private static final long serialVersionUID = 1L;

  public SemanticException() {
    super();
  }

  public SemanticException(String message) {
    super(message);
  }

  public SemanticException(Throwable cause) {
    super(cause);
  }

  public SemanticException(String message, Throwable cause) {
    super(message, cause);
  }

  public SemanticException(ErrorMsg errorMsg, String... msgArgs) {
    super(errorMsg, msgArgs);
  }
}
