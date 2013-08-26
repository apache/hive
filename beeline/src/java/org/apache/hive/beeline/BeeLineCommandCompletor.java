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
package org.apache.hive.beeline;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import jline.ArgumentCompletor;
import jline.Completor;
import jline.MultiCompletor;
import jline.NullCompletor;
import jline.SimpleCompletor;

class BeeLineCommandCompletor extends MultiCompletor {
  private final BeeLine beeLine;

  public BeeLineCommandCompletor(BeeLine beeLine) {
    this.beeLine = beeLine;
    List<ArgumentCompletor> completors = new LinkedList<ArgumentCompletor>();

    for (int i = 0; i < beeLine.commandHandlers.length; i++) {
      String[] cmds = beeLine.commandHandlers[i].getNames();
      for (int j = 0; cmds != null && j < cmds.length; j++) {
        Completor[] comps = beeLine.commandHandlers[i].getParameterCompletors();
        List<Completor> compl = new LinkedList<Completor>();
        compl.add(new SimpleCompletor(BeeLine.COMMAND_PREFIX + cmds[j]));
        compl.addAll(Arrays.asList(comps));
        compl.add(new NullCompletor()); // last param no complete
        completors.add(new ArgumentCompletor(
            compl.toArray(new Completor[0])));
      }
    }
    setCompletors(completors.toArray(new Completor[0]));
  }
}