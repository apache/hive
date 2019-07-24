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

package org.apache.hadoop.hive.ql.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This class is to workaround existing issues on LocalFileSystem.
 */
public class ProxyLocalFileSystem extends LocalFileSystem {

  public static boolean armed = false;
  static int stackIdx = 0;
  private static Mode mode;
  static List<FileSystem> tracked = new LinkedList<FileSystem>();
  private RuntimeException eCreate;
  private boolean closed;

  enum Mode {
    OFF, TRACK, BOOM,
  }
  public ProxyLocalFileSystem() {
    synchronized (ProxyLocalFileSystem.class) {
      try (PrintStream fos = new PrintStream("/tmp/stack/" + stackIdx)) {
        RuntimeException ex = new RuntimeException("c");
        ex.printStackTrace(fos);
        stackIdx++;
        eCreate = new RuntimeException();
        eCreate.fillInStackTrace();
        if (mode == Mode.TRACK) {
          tracked.add(this);
        }
      } catch (FileNotFoundException e) {
        throw new RuntimeException(e);
      }
    }

  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    // Make sure for existing destination we return false as per FileSystem api contract
    return super.isFile(dst) ? false : super.rename(src, dst);
  }

  @Override
  public void close() throws IOException {
    if (!closed && mode == Mode.BOOM) {
      throw eCreate;
    }
    closed = true;
    super.close();
  }

  public static void track() {
    mode = Mode.TRACK;
  }

  public static void boom() throws IOException {
    mode = Mode.BOOM;
    for (FileSystem f : tracked) {
      f.close();
    }
    mode = Mode.OFF;

  }

}