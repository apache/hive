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
package org.apache.hadoop.hive.metastore;

import javax.jdo.JDOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.events.PreEventContext;

/**
 *
 * AlternateFailurePreListener.
 *
 * An implementation of MetaStorePreEventListener which fails every other time it's invoked,
 * starting with the first time.
 *
 * It also records and makes available the number of times it's been invoked.
 */
public class AlternateFailurePreListener extends MetaStorePreEventListener {

  private static int callCount = 0;
  private static boolean throwException = true;

  public AlternateFailurePreListener(Configuration config) {
    super(config);
  }

  @Override
  public void onEvent(PreEventContext context) throws MetaException, NoSuchObjectException,
      InvalidOperationException {

    callCount++;
    if (throwException) {
      throwException = false;
      throw new JDOException();
    }

    throwException = true;
  }

  public static int getCallCount() {
    return callCount;
  }
}
