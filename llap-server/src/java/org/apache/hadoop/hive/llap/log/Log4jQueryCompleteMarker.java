/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.log;

import org.apache.logging.log4j.Marker;

public class Log4jQueryCompleteMarker implements Marker {

  public static final String EOF_MARKER = "EOF_MARKER";

  @Override
  public Marker addParents(Marker... markers) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getName() {
    return EOF_MARKER;
  }

  @Override
  public Marker[] getParents() {
    return new Marker[0];
  }

  @Override
  public boolean hasParents() {
    return false;
  }

  @Override
  public boolean isInstanceOf(Marker m) {
    if (m!= null && this.getName().equals(m.getName())) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean isInstanceOf(String name) {
    return EOF_MARKER.equals(name);
  }

  @Override
  public boolean remove(Marker marker) {
    return false;
  }

  @Override
  public Marker setParents(Marker... markers) {
    return null;
  }
}
