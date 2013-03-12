/*
 *  Copyright (c) 2002,2003,2004,2005 Marc Prud'hommeaux
 *  All rights reserved.
 *
 *
 *  Redistribution and use in source and binary forms,
 *  with or without modification, are permitted provided
 *  that the following conditions are met:
 *
 *  Redistributions of source code must retain the above
 *  copyright notice, this list of conditions and the following
 *  disclaimer.
 *  Redistributions in binary form must reproduce the above
 *  copyright notice, this list of conditions and the following
 *  disclaimer in the documentation and/or other materials
 *  provided with the distribution.
 *  Neither the name of the <ORGANIZATION> nor the names
 *  of its contributors may be used to endorse or promote
 *  products derived from this software without specific
 *  prior written permission.
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS
 *  AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 *  WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 *  INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 *  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 *  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
 *  OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 *  OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 *  IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 *  ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *  This software is hosted by SourceForge.
 *  SourceForge is a trademark of VA Linux Systems, Inc.
 */

/*
 * This source file is based on code taken from SQLLine 1.0.2
 * The license above originally appeared in src/sqlline/SqlLine.java
 * http://sqlline.sourceforge.net/
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