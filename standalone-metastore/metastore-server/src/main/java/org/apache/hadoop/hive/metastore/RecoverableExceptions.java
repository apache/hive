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

package org.apache.hadoop.hive.metastore;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.stream.Stream;

public class RecoverableExceptions {
  private static final Class<SQLException>[] unrecoverableSqlExceptions = new Class[]{
      // TODO: collect more unrecoverable SQLExceptions
      SQLIntegrityConstraintViolationException.class
  };

  public static boolean isRecoverableException(Throwable t) {
    return Stream.of(unrecoverableSqlExceptions)
                 .allMatch(ex -> ExceptionUtils.indexOfType(t, ex) < 0);
  }
}
