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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class DatabaseConnections {
  private final List<DatabaseConnection> connections = new ArrayList<DatabaseConnection>();
  private int index = -1;

  public DatabaseConnection current() {
    if (index != -1) {
      return connections.get(index);
    }
    return null;
  }

  public int size() {
    return connections.size();
  }

  public Iterator<DatabaseConnection> iterator() {
    return connections.iterator();
  }

  public void remove() {
    if (index != -1) {
      connections.remove(index);
    }
    while (index >= connections.size()) {
      index--;
    }
  }

  public void setConnection(DatabaseConnection connection) {
    if (connections.indexOf(connection) == -1) {
      connections.add(connection);
    }
    index = connections.indexOf(connection);
  }

  public int getIndex() {
    return index;
  }


  public boolean setIndex(int index) {
    if (index < 0 || index >= connections.size()) {
      return false;
    }
    this.index = index;
    return true;
  }
}