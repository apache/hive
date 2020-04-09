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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

/** Unit tests for MetaToolTaskUpdateLocation. */
@Category(MetastoreUnitTest.class)
public class TestMetaToolTaskUpdateLocation {
  @Rule
  public final ExpectedException exception = ExpectedException.none();

  private OutputStream os;

  @Before
  public void setup() {
    os = new ByteArrayOutputStream();
    System.setOut(new PrintStream(os));
    System.setErr(new PrintStream(os));
  }

  @Test
  public void testNoHost() throws Exception {
    exception.expect(IllegalStateException.class);
    exception.expectMessage("HiveMetaTool:A valid host is required in both old-loc and new-loc");

    MetaToolTaskUpdateLocation t = new MetaToolTaskUpdateLocation();
    t.setCommandLine(new HiveMetaToolCommandLine(new String[] {"-updateLocation", "hdfs://", "hdfs://"}));
    t.execute();
  }

  @Test
  public void testNoScheme() throws Exception {
    exception.expect(IllegalStateException.class);
    exception.expectMessage("HiveMetaTool:A valid scheme is required in both old-loc and new-loc");

    MetaToolTaskUpdateLocation t = new MetaToolTaskUpdateLocation();
    t.setCommandLine(new HiveMetaToolCommandLine(new String[] {"-updateLocation", "//old.host", "//new.host"}));
    t.execute();
  }

  @Test
  public void testUpdateLocationNoUpdate() throws Exception {
    // testing only that the proper functions are called on ObjectStore - effect tested in TestHiveMetaTool in itests
    String oldUriString = "hdfs://old.host";
    String newUriString = "hdfs://new.host";
    String tablePropKey = "abc";
    String serdePropKey = "def";

    URI oldUri = new Path(oldUriString).toUri();
    URI newUri = new Path(newUriString).toUri();

    ObjectStore mockObjectStore = Mockito.mock(ObjectStore.class);
    when(mockObjectStore.updateMDatabaseURI(eq(oldUri), eq(newUri), eq(true))).thenReturn(null);
    when(mockObjectStore.updateMStorageDescriptorTblURI(eq(oldUri), eq(newUri), eq(true))).thenReturn(null);
    when(mockObjectStore.updateTblPropURI(eq(oldUri), eq(newUri), eq(tablePropKey), eq(true))).thenReturn(null);
    when(mockObjectStore.updateMStorageDescriptorTblPropURI(eq(oldUri), eq(newUri), eq(tablePropKey), eq(true)))
      .thenReturn(null);
    when(mockObjectStore.updateSerdeURI(eq(oldUri), eq(newUri), eq(serdePropKey), eq(true))).thenReturn(null);

    MetaToolTaskUpdateLocation t = new MetaToolTaskUpdateLocation();
    t.setCommandLine(new HiveMetaToolCommandLine(new String[] {"-updateLocation", newUriString, oldUriString, "-dryRun",
        "-tablePropKey", tablePropKey, "-serdePropKey", serdePropKey}));
    t.setObjectStore(mockObjectStore);
    t.execute();
  }
}
