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
package org.apache.hadoop.hive.ql.parse.repl.load;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class TestDumpMetaData {
  private Path dumpRoot = new Path("file:///tmp");;
  private HiveConf conf = new HiveConf();
  private DumpMetaData dmd = new DumpMetaData(dumpRoot, conf);

  @Before
  public void setUp() {
    dmd.setDump(DumpType.BOOTSTRAP, 12L, 14L, new Path("file:///tmp/cmroot"), 2L, false);
  }

  @Test
  public void testIncompatibleVersion() throws SemanticException {
    dmd.setDumpFormatVersion(Utilities.MIN_VERSION_FOR_NEW_DUMP_FORMAT - 1);
    dmd.write(true);

    DumpMetaData dmdLoad = new DumpMetaData(dumpRoot, conf);
    assertFalse(dmdLoad.isVersionCompatible());
  }

  @Test
  public void testCompatibleVersion() throws SemanticException {
    dmd.setDumpFormatVersion(Utilities.MIN_VERSION_FOR_NEW_DUMP_FORMAT);
    dmd.write(true);

    DumpMetaData dmdLoad = new DumpMetaData(dumpRoot, conf);
    assertTrue(dmdLoad.isVersionCompatible());
  }

  @Test
  public void testEmptyVersion() throws SemanticException {
    List<List<String>> listValues = new ArrayList<>();
    listValues.add(
        Arrays.asList(
            DumpType.BOOTSTRAP.toString(),
            "12",
            "14",
            "file:///tmp/cmroot",
            "2",
            "payload",
            "false")
    );
    Utils.writeOutput(listValues, new Path(dumpRoot, DumpMetaData.getDmdFileName()), conf, true);

    DumpMetaData dmdLoad = new DumpMetaData(dumpRoot, conf);
    assertFalse(dmdLoad.isVersionCompatible());
  }

}
