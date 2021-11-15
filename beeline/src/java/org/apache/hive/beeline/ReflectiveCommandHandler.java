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

/*
 * This source file is based on code taken from SQLLine 1.0.2
 * See SQLLine notice in LICENSE
 */
package org.apache.hive.beeline;

import jline.console.completer.Completer;

import org.apache.hadoop.fs.shell.Command;

/**
 * A {@link Command} implementation that uses reflection to
 * determine the method to dispatch the command.
 *
 */
public class ReflectiveCommandHandler extends AbstractCommandHandler {
  private final BeeLine beeLine;

  /**
   * @param beeLine
   * @param cmds      'cmds' is an array of alternative names for the same command. And that the
   *                  first one is always chosen for display purposes and to lookup help
   *                  documentation from BeeLine.properties file.
   * @param completer
   */
  public ReflectiveCommandHandler(BeeLine beeLine, String[] cmds, Completer[] completer) {
    super(beeLine, cmds, beeLine.loc("help-" + cmds[0]), completer);
    this.beeLine = beeLine;
  }

  @Override
  public boolean execute(String line) {
    lastException = null;
    ClientHook hook = ClientCommandHookFactory.get().getHook(beeLine, line);

    try {
      Object ob = beeLine.getCommands().getClass().getMethod(getName(),
          new Class[] {String.class})
          .invoke(beeLine.getCommands(), new Object[] {line});

      boolean result = (ob != null && ob instanceof Boolean && ((Boolean) ob).booleanValue());

      if (hook != null && result) {
        hook.postHook(beeLine);
      }

      return result;
    } catch (Throwable e) {
      lastException = e;
      return beeLine.error(e);
    }
  }
}
