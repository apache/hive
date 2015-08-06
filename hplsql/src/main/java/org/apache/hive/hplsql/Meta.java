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

package org.apache.hive.hplsql;

import java.util.ArrayList;

/**
 * Metadata
 */
public class Meta {
  /**
   * Normalize identifier name (convert "" [] to `` i.e.)
   */
  public String normalizeIdentifier(String name) {
    ArrayList<String> parts = splitIdentifier(name);
    if (parts != null) {
      StringBuilder norm = new StringBuilder();
      for (int i = 0; i < parts.size(); i++) {
        norm.append(normalizeIdentifierPart(parts.get(i)));
        if (i + 1 < parts.size()) {
          norm.append(".");
        }
      }
      return norm.toString();
    }
    return normalizeIdentifierPart(name);
  }
  
  /**
   * Normalize identifier (single part)
   */
  public String normalizeIdentifierPart(String name) {
    char start = name.charAt(0);
    char end = name.charAt(name.length() - 1);
    if ((start == '[' && end == ']') || (start == '"' && end == '"')) {
      return '`' + name.substring(1, name.length() - 1) + '`'; 
    }
    return name;
  }
  
  /**
   * Split identifier to parts (schema, table, colum name etc.)
   * @return null if identifier contains single part
   */
  public ArrayList<String> splitIdentifier(String name) {
    ArrayList<String> parts = null;
    int start = 0;
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      char del = '\0';
      if (c == '`' || c == '"') {
        del = c;        
      }
      else if (c == '[') {
        del = ']';
      }
      if (del != '\0') {
        for (int j = i + 1; i < name.length(); j++) {
          i++;
          if (name.charAt(j) == del) {
            break;
          }
        }
        continue;
      }
      if (c == '.') {
        if (parts == null) {
          parts = new ArrayList<String>();
        }
        parts.add(name.substring(start, i));
        start = i + 1;
      }
    }
    if (parts != null) {
      parts.add(name.substring(start));
    }
    return parts;
  }
}



