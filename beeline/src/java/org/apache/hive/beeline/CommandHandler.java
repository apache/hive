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

/*
 * This source file is based on code taken from SQLLine 1.0.2
 * See SQLLine notice in LICENSE
 */
package org.apache.hive.beeline;

import jline.console.completer.Completer;

/**
 * A generic command to be executed. Execution of the command
 * should be dispatched to the {@link #execute(java.lang.String)} method after determining that
 * the command is appropriate with
 * the {@link #matches(java.lang.String)} method.
 *
 */
interface CommandHandler {
  /**
   * @return the name of the command
   */
  public String getName();


  /**
   * @return all the possible names of this command.
   */
  public String[] getNames();


  /**
   * @return the short help description for this command.
   */
  public String getHelpText();


  /**
   * Check to see if the specified string can be dispatched to this
   * command.
   *
   * @param line
   *          the command line to check.
   * @return the command string that matches, or null if it no match
   */
  public String matches(String line);


  /**
   * Execute the specified command.
   *
   * @param line
   *          the full command line to execute.
   */
  public boolean execute(String line);


  /**
   * Returns the completors that can handle parameters.
   */
  public Completer[] getParameterCompleters();

  /**
   * Returns exception thrown for last command
   * @return
   */
  public Throwable getLastException();
}