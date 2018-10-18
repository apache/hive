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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import jline.console.completer.Completer;
import jline.console.completer.NullCompleter;

/**
 * An abstract implementation of CommandHandler.
 *
 */
public abstract class AbstractCommandHandler implements CommandHandler {
  private final BeeLine beeLine;
  private final String name;
  private final String[] names;
  private final String helpText;
  private Completer[] parameterCompleters = new Completer[0];

  protected transient Throwable lastException;

  public AbstractCommandHandler(BeeLine beeLine, String[] names, String helpText,
                                Completer[] completors) {
    this.beeLine = beeLine;
    name = names[0];
    this.names = names;
    this.helpText = helpText;
    if (completors == null || completors.length == 0) {
      parameterCompleters = new Completer[] { new NullCompleter() };
    } else {
      List<Completer> c = new LinkedList<Completer>(Arrays.asList(completors));
      c.add(new NullCompleter());
      parameterCompleters = c.toArray(new Completer[0]);
    }
  }

  @Override
  public String getHelpText() {
    return helpText;
  }


  @Override
  public String getName() {
    return name;
  }


  @Override
  public String[] getNames() {
    return names;
  }


  @Override
  public String matches(String line) {
    if (line == null || line.length() == 0) {
      return null;
    }

    String[] parts = beeLine.split(line);
    if (parts == null || parts.length == 0) {
      return null;
    }

    for (String name2 : names) {
      if (name2.startsWith(parts[0])) {
        return name2;
      }
    }
    return null;
  }

  public void setParameterCompleters(Completer[] parameterCompleters) {
    this.parameterCompleters = parameterCompleters;
  }

  @Override
  public Completer[] getParameterCompleters() {
    return parameterCompleters;
  }

  @Override
  public Throwable getLastException() {
    return lastException;
  }
}
