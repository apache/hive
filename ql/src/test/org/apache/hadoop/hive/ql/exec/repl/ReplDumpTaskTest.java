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
package org.apache.hadoop.hive.ql.exec.repl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ Utils.class })
@PowerMockIgnore({ "javax.management.*" })
public class ReplDumpTaskTest {

  @Mock
  private Hive hive;

  class StubReplDumpTask extends ReplDumpTask {

    @Override
    protected Hive getHive() {
      return hive;
    }

    @Override
    long currentNotificationId(Hive hiveDb) {
      return Long.MAX_VALUE;
    }

    @Override
    String getValidTxnListForReplDump(Hive hiveDb) {
      return "";
    }

    @Override
    void dumpFunctionMetadata(String dbName, Path dumpRoot) {
    }

    @Override
    Path dumpDbMetadata(String dbName, Path dumpRoot, long lastReplId) {
      return Mockito.mock(Path.class);
    }

    @Override
    void dumpConstraintMetadata(String dbName, String tblName, Path dbRoot) {
    }
  }

  private static class TestException extends Exception {
  }

  @Test(expected = TestException.class)
  public void removeDBPropertyToPreventRenameWhenBootstrapDumpOfTableFails() throws Exception {
    List<String> tableList = Arrays.asList("a1", "a2");
    String dbRandomKey = "akeytoberandom";

    mockStatic(Utils.class);
    when(Utils.matchesDb(same(hive), eq("default")))
        .thenReturn(Collections.singletonList("default"));
    when(Utils.getAllTables(same(hive), eq("default"))).thenReturn(tableList);
    when(Utils.setDbBootstrapDumpState(same(hive), eq("default"))).thenReturn(dbRandomKey);
    when(Utils.matchesTbl(same(hive), eq("default"), anyString())).thenReturn(tableList);


    when(hive.getAllFunctions()).thenReturn(Collections.emptyList());

    ReplDumpTask task = new StubReplDumpTask() {
      private int tableDumpCount = 0;

      @Override
      void dumpTable(String dbName, String tblName, String validTxnList, Path dbRoot)
          throws Exception {
        tableDumpCount++;
        if (tableDumpCount > 1) {
          throw new TestException();
        }
      }
    };

    task.setWork(
        new ReplDumpWork("default", "",
            Long.MAX_VALUE, Long.MAX_VALUE, "",
            Integer.MAX_VALUE, "")
    );

    try {
      task.bootStrapDump(mock(Path.class), null, mock(Path.class));
    } finally {
      verifyStatic();
      Utils.resetDbBootstrapDumpState(same(hive), eq("default"), eq(dbRandomKey));
    }
  }
}
