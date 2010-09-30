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

package org.apache.hadoop.hive.ql.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;

public class DosToUnix {

  public static String convertWindowsScriptToUnix(File windowsScriptFile) throws Exception {
    String windowsScriptFilename = windowsScriptFile.getName();
    String unixScriptFilename = getUnixScriptNameFor(windowsScriptFilename);
    File unixScriptFile = null;
    if (windowsScriptFile.getParent() != null) {
      unixScriptFile = new File(windowsScriptFile.getParent() + "/" + unixScriptFilename);
    } else {
      unixScriptFile = new File(unixScriptFilename);
    }
    BufferedWriter writer = new BufferedWriter(new FileWriter(unixScriptFile));
    try {
      BufferedReader reader = new BufferedReader(new FileReader(windowsScriptFile));
      try {
        int prev = reader.read();
        int next = reader.read();
        while( prev != -1 ) {
          if ( prev != -1 && ( prev != '\r' || next != '\n' ) ) {
            writer.write(prev);
          }
          prev = next;
          next = reader.read();
        }
      }
      finally {
        reader.close();
      }
    }
    finally {
      writer.close();
    }
    unixScriptFile.setExecutable(true);
    return unixScriptFile.getAbsolutePath();
  }

  public static String getUnixScriptNameFor(String windowsScriptFilename) {
    int pos = windowsScriptFilename.indexOf(".");
    String unixScriptFilename;
    if ( pos >= 0 ) {
      unixScriptFilename = windowsScriptFilename.substring(0, pos) + "_unix" + windowsScriptFilename.substring(pos);
    }
    else {
      unixScriptFilename = windowsScriptFilename + "_unix";
    }
    return unixScriptFilename;
  }

  public static boolean isWindowsScript(File file) {
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
      char[] buffer = new char[4096];
      int readLength = reader.read(buffer);
      if (readLength >= 2 && buffer[0] == '#' && buffer[1] == '!') {
        for(int i=2; i<readLength; ++i) {
          switch(buffer[i]) {
          case '\r':
            return true;
          case '\n':
            return false;
          }
        }
      }
    } catch (Exception e) {
      // ignore error reading file, if we can't read it then it isn't a windows script
    }
    return false;
  }

}
