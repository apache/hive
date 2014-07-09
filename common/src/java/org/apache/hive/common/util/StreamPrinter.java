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

package org.apache.hive.common.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

import org.apache.hadoop.io.IOUtils;

/**
 * StreamPrinter.
 *
 */
public class StreamPrinter extends Thread {
  InputStream is;
  String type;
  PrintStream os;

  public StreamPrinter(InputStream is, String type, PrintStream os) {
    this.is = is;
    this.type = type;
    this.os = os;
  }

  @Override
  public void run() {
    BufferedReader br = null;
    try {
      InputStreamReader isr = new InputStreamReader(is);
      br = new BufferedReader(isr);
      String line = null;
      if (type != null) {
        while ((line = br.readLine()) != null) {
          os.println(type + ">" + line);
        }
      } else {
        while ((line = br.readLine()) != null) {
          os.println(line);
        }
      }
      br.close();
      br=null;
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }finally{
      IOUtils.closeStream(br);
    }
  }
}

