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

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

import static org.junit.Assert.assertEquals;


/**
 * A wrapper around {@link ObjectStore} that allows us to inject custom behaviour
 * on to some of the methods for testing.
 */
public class InjectableBehaviourObjectStore extends ObjectStore {
  public InjectableBehaviourObjectStore() {
    super();
  }

  /**
   * A utility class that allows people injecting behaviour to determine if their injections occurred.
   */
  public static abstract class BehaviourInjection<T,F>
      implements com.google.common.base.Function<T,F>{
    protected boolean injectionPathCalled = false;
    protected boolean nonInjectedPathCalled = false;

    public void assertInjectionsPerformed(
        boolean expectedInjectionCalled, boolean expectedNonInjectedPathCalled){
      assertEquals(expectedInjectionCalled, injectionPathCalled);
      assertEquals(expectedNonInjectedPathCalled, nonInjectedPathCalled);
    }
  };

  private static com.google.common.base.Function<Table,Table> getTableModifier =
      com.google.common.base.Functions.identity();

  public static void setGetTableBehaviour(com.google.common.base.Function<Table,Table> modifier){
    getTableModifier = (modifier == null)? com.google.common.base.Functions.identity() : modifier;
  }

  public static void resetGetTableBehaviour(){
    setGetTableBehaviour(null);
  }

  public static com.google.common.base.Function<Table,Table> getGetTableBehaviour() {
    return getTableModifier;
  }

  @Override
  public Table getTable(String dbName, String tableName) throws MetaException {
    return getTableModifier.apply(super.getTable(dbName, tableName));
  }
}
