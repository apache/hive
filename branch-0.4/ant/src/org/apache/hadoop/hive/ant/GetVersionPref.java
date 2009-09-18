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

package org.apache.hadoop.hive.ant;

import org.apache.tools.ant.AntClassLoader;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.Project;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.*;

/**
 * Implementation of the ant task <getversionpref property="nameoftheproperty" input="versionstring"/>.
 *
 * This ant task takes an input version string (e.g. 0.17.2) and set an ant property (whose name
 * is specified in the property attribute) with the version prefix. For 0.17.2, the version prefix
 * is 0.17. Similarly, for 0.18.0, the version prefix is 0.18. The version prefix is the first two
 * components of the version string.
 */
public class GetVersionPref extends Task {

  /**
   * The name of the property that gets the version prefix.
   */
  protected String property;

  /**
   * The input string that contains the version string.
   */
  protected String input;
 
  public void setProperty(String property) {
    this.property = property;
  }

  public String getProperty() {
    return property;
  }

  public void setInput(String input) {
    this.input = input;
  }

  public String getInput() {
    return input;
  }

  /**
   * Executes the ant task <getversionperf>.
   *
   * It extracts the version prefix using regular expressions on the version string. It then sets
   * the property in the project with the extracted prefix. The property is set to an empty string
   * in case no match is found for the prefix regular expression (which will happen in case the
   * version string does not conform to the version format).
   */
  @Override
  public void execute() throws BuildException {

    if (property == null) {
      throw new BuildException("No property specified");
    }

    if (input == null) {
      throw new BuildException("No input stringspecified");
    }

    try {
      Pattern p = Pattern.compile("^(\\d+\\.\\d+).*");
      Matcher m = p.matcher(input);
      getProject().setProperty(property, m.matches() ? m.group(1) : "");
    }
    catch (Exception e) {
      throw new BuildException("Failed with: " + e.getMessage());
    }
  }
}
