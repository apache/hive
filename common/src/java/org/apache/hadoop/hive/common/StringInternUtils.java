/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.common;

import org.apache.hadoop.fs.Path;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/**
 * Collection of utilities for string interning, common across Hive.
 * We use the standard String.intern() call, that performs very well
 * (no problems with PermGen overflowing, etc.) starting from JDK 7.
 */
public class StringInternUtils {

  // When a URI instance is initialized, it creates a bunch of private String
  // fields, never bothering about their possible duplication. It would be
  // best if we could tell URI constructor to intern these strings right away.
  // Without this option, we can only use reflection to "fix" strings in these
  // fields after a URI has been created.
  private static Class uriClass = URI.class;
  private static Field stringField, schemeField, authorityField, hostField, pathField,
      fragmentField, schemeSpecificPartField;

  static {
    try {
      stringField = uriClass.getDeclaredField("string");
      schemeField = uriClass.getDeclaredField("scheme");
      authorityField = uriClass.getDeclaredField("authority");
      hostField = uriClass.getDeclaredField("host");
      pathField = uriClass.getDeclaredField("path");
      fragmentField = uriClass.getDeclaredField("fragment");
      schemeSpecificPartField = uriClass.getDeclaredField("schemeSpecificPart");
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }

    // Note that the calls below will throw an exception if a Java SecurityManager
    // is installed and configured to forbid invoking setAccessible(). In practice
    // this is not a problem in Hive.
    stringField.setAccessible(true);
    schemeField.setAccessible(true);
    authorityField.setAccessible(true);
    hostField.setAccessible(true);
    pathField.setAccessible(true);
    fragmentField.setAccessible(true);
    schemeSpecificPartField.setAccessible(true);
  }

  public static URI internStringsInUri(URI uri) {
    if (uri == null) return null;
    try {
      String string = (String) stringField.get(uri);
      if (string != null) stringField.set(uri, string.intern());
      String scheme = (String) schemeField.get(uri);
      if (scheme != null) schemeField.set(uri, scheme.intern());
      String authority = (String) authorityField.get(uri);
      if (authority != null) authorityField.set(uri, authority.intern());
      String host = (String) hostField.get(uri);
      if (host != null) hostField.set(uri, host.intern());
      String path = (String) pathField.get(uri);
      if (path != null) pathField.set(uri, path.intern());
      String fragment = (String) fragmentField.get(uri);
      if (fragment != null) fragmentField.set(uri, fragment.intern());
      String schemeSpecificPart = (String) schemeSpecificPartField.get(uri);
      if (schemeSpecificPart != null) schemeSpecificPartField.set(uri, schemeSpecificPart.intern());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return uri;
  }

  public static Path internUriStringsInPath(Path path) {
    if (path != null) internStringsInUri(path.toUri());
    return path;
  }

  public static Path[] internUriStringsInPathArray(Path[] paths) {
    if (paths != null) {
      for (Path path : paths) {
        internUriStringsInPath(path);
      }
    }
    return paths;
  }

  /**
   * This method interns all the strings in the given list in place. That is,
   * it iterates over the list, replaces each element with the interned copy
   * and eventually returns the same list.
   *
   * Note that the provided List implementation should return an iterator
   * (via list.listIterator()) method, and that iterator should implement
   * the set(Object) method. That's what all List implementations in the JDK
   * provide. However, if some custom List implementation doesn't have this
   * functionality, this method will return without interning its elements.
   */
  public static List<String> internStringsInList(List<String> list) {
    if (list != null) {
      try {
        ListIterator<String> it = list.listIterator();
        while (it.hasNext()) {
          it.set(it.next().intern());
        }
      } catch (UnsupportedOperationException e) { } // set() not implemented - ignore
    }
    return list;
  }

  /** Interns all the strings in the given array in place, returning the same array */
  public static String[] internStringsInArray(String[] strings) {
    for (int i = 0; i < strings.length; i++) {
      if (strings[i] != null) {
        strings[i] = strings[i].intern();
      }
    }
    return strings;
  }

  public static <K> Map<K, String> internValuesInMap(Map<K, String> map) {
    if (map != null) {
      for (K key : map.keySet()) {
        String value = map.get(key);
        if (value != null) {
          map.put(key, value.intern());
        }
      }
    }
    return map;
  }

  public static String internIfNotNull(String s) {
    if (s != null) s = s.intern();
    return s;
  }
}
