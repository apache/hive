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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton.tool;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

public class TestTrivialExecService {
  @Test
  public void test() {
    ArrayList<String> list = new ArrayList<String>();
    list.add("echo");
    list.add("success");
    BufferedReader out = null;
    BufferedReader err = null;
    try {
      Process process = TrivialExecService.getInstance()
        .run(list,
          new ArrayList<String>(),
          new HashMap<String, String>());
      out = new BufferedReader(new InputStreamReader(
        process.getInputStream()));
      err = new BufferedReader(new InputStreamReader(
        process.getErrorStream()));
      Assert.assertEquals("success", out.readLine());
      out.close();
      String line;
      while ((line = err.readLine()) != null) {
        Assert.fail(line);
      }
      process.waitFor();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("Process caused exception.");
    } finally {
      try {
        out.close();
      } catch (Exception ex) {
        // Whatever.
      }
      try {
        err.close();
      } catch (Exception ex) {
        // Whatever
      }
    }
  }
}
