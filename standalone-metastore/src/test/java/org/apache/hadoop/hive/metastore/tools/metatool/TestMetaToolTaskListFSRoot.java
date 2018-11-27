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

package org.apache.hadoop.hive.metastore.tools.metatool;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;

import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

/** Unit tests for MetaToolTaskListFSRoot. */
@Category(MetastoreUnitTest.class)
public class TestMetaToolTaskListFSRoot {
  @Test
  public void testListFSRoot() throws Exception {
    String fsRoot1 = "hdfs://abc.de";
    String fsRoot2 = "hdfs://fgh.ji";

    ObjectStore mockObjectStore = Mockito.mock(ObjectStore.class);
    when(mockObjectStore.listFSRoots()).thenReturn(Sets.newHashSet(fsRoot1, fsRoot2));

    OutputStream os = new ByteArrayOutputStream();
    System.setOut(new PrintStream(os));

    MetaToolTaskListFSRoot t = new MetaToolTaskListFSRoot();
    t.setCommandLine(new HiveMetaToolCommandLine(new String[] {"-listFSRoot"}));
    t.setObjectStore(mockObjectStore);
    t.execute();

    assertTrue(os.toString() + " doesn't contain " + fsRoot1, os.toString().contains(fsRoot1));
    assertTrue(os.toString() + " doesn't contain " + fsRoot2, os.toString().contains(fsRoot2));
  }
}
