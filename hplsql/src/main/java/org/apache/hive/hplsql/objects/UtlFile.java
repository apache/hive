/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hive.hplsql.objects;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hive.hplsql.File;

public class UtlFile implements HplObject {
  private final UtlFileClass hplClass;

  public UtlFile(UtlFileClass hplClass) {
    this.hplClass = hplClass;
  }

  @Override
  public HplClass hplClass() {
    return hplClass;
  }

  public File fileOpen(String dir, String name, boolean write, boolean overwrite) {
    File file = new File();
    if (write) {
      file.create(dir, name, overwrite);
    } else {
      file.open(dir, name);
    }
    return file;
  }

  public void fileClose(File file) {
    file.close();
  }

  public String getLine(File file) {
    StringBuilder out = new StringBuilder();
    try {
      while(true) {
        char c = file.readChar();
          if(c == '\n') {
          break;
        }
        out.append(c);
      }
    } catch (IOException e) {
      if(!(e instanceof EOFException)) {
        out.setLength(0);
      }
    }
    return out.toString();
  }

  public void put(File file, String str, boolean newLine) {
    file.writeString(str);
    if (newLine) {
      file.writeString("\n");
    }
  }
}
