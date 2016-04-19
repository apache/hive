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

import jline.console.completer.AggregateCompleter;
import jline.console.completer.Completer;
import jline.console.completer.NullCompleter;
import jline.console.completer.StringsCompleter;

class BeeLineCommandCompleter extends AggregateCompleter {
  public BeeLineCommandCompleter(Iterable<CommandHandler> handlers) {
    super(getCompleters(handlers));
  }

  public static List<Completer> getCompleters(Iterable<CommandHandler> handlers){
    List<Completer> completers = new LinkedList<Completer>();

    for (CommandHandler handler : handlers) {
      String[] commandNames = handler.getNames();
      if (commandNames != null) {
        for (String commandName : commandNames) {
          List<Completer> compl = new LinkedList<Completer>();
          compl.add(new StringsCompleter(BeeLine.COMMAND_PREFIX + commandName));
          compl.addAll(Arrays.asList(handler.getParameterCompleters()));
          compl.add(new NullCompleter()); // last param no complete
          completers.add(new AggregateCompleter(compl.toArray(new Completer[compl.size()])));
        }
      }
    }

    return completers;
  }
}