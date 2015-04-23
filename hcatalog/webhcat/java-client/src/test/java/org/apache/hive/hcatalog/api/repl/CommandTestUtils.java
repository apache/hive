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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.api.repl;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Class which provides several useful methods to test commands, but is itself not a test.
 */
public class CommandTestUtils {

  private static Log LOG = LogFactory.getLog(CommandTestUtils.class.getName());

  public static void compareCommands(Command expected, Command actual, boolean ignoreSortOrder) {
    // The reason we use compare-command, rather than simply getting the serialized output and comparing
    // for partition-based commands is that the partition specification order can be different in different
    // serializations, but still be effectively the same. (a="42",b="abc") should be the same as (b="abc",a="42")
    assertEquals(expected.getClass(),actual.getClass());
    assertEquals(expected.getEventId(),actual.getEventId());
    assertEquals(expected.isUndoable(),actual.isUndoable());
    assertEquals(expected.isRetriable(),actual.isRetriable());

    assertEquals(expected.get().size(),actual.get().size());
    Iterator<String> actualIter = actual.get().iterator();
    for (String s : expected.get()){
      if (ignoreSortOrder){
        // compare sorted strings, rather than comparing exact strings.
        assertSortedEquals(s, actualIter.next());
      } else {
        assertEquals(s,actualIter.next());
      }
    }

    if (expected.isUndoable()){
      Iterator<String> actualUndoIter = actual.getUndo().iterator();
      for (String s: expected.getUndo()){
        if (ignoreSortOrder){
          assertSortedEquals(s,actualUndoIter.next());
        } else {
          assertEquals(s,actualIter.next());
        }
      }
    }
  }

  private static void assertSortedEquals(String expected, String actual) {
    char[] expectedChars = expected.toCharArray();
    Arrays.sort(expectedChars);
    char[] actualChars = actual.toCharArray();
    Arrays.sort(actualChars);
    assertEquals(String.valueOf(expectedChars), String.valueOf(actualChars));
  }

  public static void testCommandSerialization(Command cmd) {
    String serializedCmd = null;
    try {
      serializedCmd = ReplicationUtils.serializeCommand(cmd);
    } catch (IOException e) {
      LOG.error("Serialization error",e);
      assertNull(e); // error out.
    }

    Command cmd2 = null;
    try {
      cmd2 = ReplicationUtils.deserializeCommand(serializedCmd);
    } catch (IOException e) {
      LOG.error("Serialization error",e);
      assertNull(e); // error out.
    }

    assertEquals(cmd.getClass(),cmd2.getClass());
    assertEquals(cmd.getEventId(), cmd2.getEventId());
    assertEquals(cmd.get(), cmd2.get());
    assertEquals(cmd.isUndoable(),cmd2.isUndoable());
    if (cmd.isUndoable()){
      assertEquals(cmd.getUndo(),cmd2.getUndo());
    }
    assertEquals(cmd.isRetriable(),cmd2.isRetriable());
  }

}
