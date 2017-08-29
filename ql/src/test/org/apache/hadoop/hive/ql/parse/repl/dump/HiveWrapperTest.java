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
 
package org.apache.hadoop.hive.ql.parse.repl.dump;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HiveWrapperTest {
  @Mock
  private HiveWrapper.Tuple.Function<ReplicationSpec> specFunction;
  @Mock
  private HiveWrapper.Tuple.Function<Table> tableFunction;

  @Test
  public void replicationIdIsRequestedBeforeObjectDefinition() throws HiveException {
    new HiveWrapper.Tuple<>(specFunction, tableFunction);
    InOrder inOrder = Mockito.inOrder(specFunction, tableFunction);
    inOrder.verify(specFunction).fromMetaStore();
    inOrder.verify(tableFunction).fromMetaStore();
  }
}