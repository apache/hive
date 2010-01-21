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

package org.apache.hadoop.hive.scripts;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class extracturl {

  protected static final Pattern pattern = Pattern.compile(
      "<a href=\"http://([\\w\\d]+\\.html)\">link</a>",
      Pattern.CASE_INSENSITIVE);
  static InputStreamReader converter = new InputStreamReader(System.in);
  static BufferedReader in = new BufferedReader(converter);

  public static void main(String[] args) {
    String input;
    try {
      while ((input = in.readLine()) != null) {
        Matcher m = pattern.matcher(input);

        while (m.find()) {
          String url = input.substring(m.start(1), m.end(1));
          System.out.println(url + "\t" + "1");
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
