/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.ptest.execution;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class Templates {
  public static void writeTemplateResult(String resource, File script,
      Map<String, String> keyValues) throws IOException {
    String template = readResource(resource);
    PrintWriter writer = new PrintWriter(script);
    try {
      writer.write(getTemplateResult(template, keyValues));
      if(writer.checkError()) {
        throw new IOException("Error writing to " + script);
      }
    } finally {
      writer.close();
    }
  }
  public static String readResource(String resource) throws IOException {
    return Resources.toString(Resources.getResource(resource), Charsets.UTF_8);
  }
  public static String getTemplateResult(String command, Map<String, String> keyValues)
      throws IOException {
    VelocityContext context = new VelocityContext();
    for(String key : keyValues.keySet()) {
      context.put(key, keyValues.get(key));
    }
    StringWriter writer = new StringWriter();
    if(!Velocity.evaluate(context, writer, command, command)) {
      throw new IOException("Unable to render " + command + " with " + keyValues);
    }
    writer.close();
    return writer.toString();
  }
  private Templates() {}
}
