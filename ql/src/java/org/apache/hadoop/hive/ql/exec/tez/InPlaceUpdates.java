/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.tez;

import static org.fusesource.jansi.Ansi.ansi;
import static org.fusesource.jansi.internal.CLibrary.STDERR_FILENO;
import static org.fusesource.jansi.internal.CLibrary.STDOUT_FILENO;
import static org.fusesource.jansi.internal.CLibrary.isatty;

import java.io.PrintStream;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.fusesource.jansi.Ansi;

import jline.TerminalFactory;

public class InPlaceUpdates {

  public static final int MIN_TERMINAL_WIDTH = 94;

  static boolean isUnixTerminal() {

    String os = System.getProperty("os.name");
    if (os.startsWith("Windows")) {
      // we do not support Windows, we will revisit this if we really need it for windows.
      return false;
    }

    // We must be on some unix variant..
    // check if standard out is a terminal
    try {
      // isatty system call will return 1 if the file descriptor is terminal else 0
      if (isatty(STDOUT_FILENO) == 0) {
        return false;
      }
      if (isatty(STDERR_FILENO) == 0) {
        return false;
      }
    } catch (NoClassDefFoundError ignore) {
      // These errors happen if the JNI lib is not available for your platform.
      return false;
    } catch (UnsatisfiedLinkError ignore) {
      // These errors happen if the JNI lib is not available for your platform.
      return false;
    }
    return true;
  }

  public static boolean inPlaceEligible(HiveConf conf) {
    boolean inPlaceUpdates = HiveConf.getBoolVar(conf, HiveConf.ConfVars.TEZ_EXEC_INPLACE_PROGRESS);

    // we need at least 80 chars wide terminal to display in-place updates properly
    return inPlaceUpdates && !SessionState.getConsole().getIsSilent() && isUnixTerminal()
      && TerminalFactory.get().getWidth() >= MIN_TERMINAL_WIDTH;
  }

  public static void reprintLine(PrintStream out, String line) {
    out.print(ansi().eraseLine(Ansi.Erase.ALL).a(line).a('\n').toString());
    out.flush();
  }

  public static void rePositionCursor(PrintStream ps) {
    ps.print(ansi().cursorUp(0).toString());
    ps.flush();
  }
}
