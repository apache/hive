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

package org.apache.hadoop.hive.ql.udf;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Iterators;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser.Feature;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.codehaus.jackson.type.JavaType;

/**
 * UDFJson.
 *
 */
@Description(name = "get_json_object",
    value = "_FUNC_(json_txt, path) - Extract a json object from path ",
    extended = "Extract json object from a json string based on json path "
    + "specified, and return json string of the extracted json object. It "
    + "will return null if the input json string is invalid.\n"
    + "A limited version of JSONPath supported:\n"
    + "  $   : Root object\n"
    + "  .   : Child operator\n"
    + "  []  : Subscript operator for array\n"
    + "  *   : Wildcard for []\n"
    + "Syntax not supported that's worth noticing:\n"
    + "  ''  : Zero length string as key\n"
    + "  ..  : Recursive descent\n"
    + "  &amp;#064;   : Current object/element\n"
    + "  ()  : Script expression\n"
    + "  ?() : Filter (script) expression.\n"
    + "  [,] : Union operator\n"
    + "  [start:end:step] : array slice operator\n")
public class UDFJson extends UDF {
  private static final Pattern patternKey = Pattern.compile("^([a-zA-Z0-9_\\-\\:\\s]+).*");
  private static final Pattern patternIndex = Pattern.compile("\\[([0-9]+|\\*)\\]");
  private static final JavaType MAP_TYPE = TypeFactory.fromClass(Map.class);
  private static final JavaType LIST_TYPE = TypeFactory.fromClass(List.class);

  private final JsonFactory jsonFactory = new JsonFactory();
  private final ObjectMapper objectMapper = new ObjectMapper(jsonFactory);

  // An LRU cache using a linked hash map
  static class HashCache<K, V> extends LinkedHashMap<K, V> {

    private static final int CACHE_SIZE = 16;
    private static final int INIT_SIZE = 32;
    private static final float LOAD_FACTOR = 0.6f;

    HashCache() {
      super(INIT_SIZE, LOAD_FACTOR);
    }

    private static final long serialVersionUID = 1;

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
      return size() > CACHE_SIZE;
    }

  }

  Map<String, Object> extractObjectCache = new HashCache<String, Object>();
  Map<String, String[]> pathExprCache = new HashCache<String, String[]>();
  Map<String, ArrayList<String>> indexListCache =
      new HashCache<String, ArrayList<String>>();
  Map<String, String> mKeyGroup1Cache = new HashCache<String, String>();
  Map<String, Boolean> mKeyMatchesCache = new HashCache<String, Boolean>();

  public UDFJson() {
    // Allows for unescaped ASCII control characters in JSON values
    jsonFactory.enable(Feature.ALLOW_UNQUOTED_CONTROL_CHARS);
    // Enabled to accept quoting of all character backslash qooting mechanism
    jsonFactory.enable(Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER);
  }

  /**
   * Extract json object from a json string based on json path specified, and
   * return json string of the extracted json object. It will return null if the
   * input json string is invalid.
   *
   * A limited version of JSONPath supported: $ : Root object . : Child operator
   * [] : Subscript operator for array * : Wildcard for []
   *
   * Syntax not supported that's worth noticing: '' : Zero length string as key
   * .. : Recursive descent &amp;#064; : Current object/element () : Script
   * expression ?() : Filter (script) expression. [,] : Union operator
   * [start:end:step] : array slice operator
   *
   * @param jsonString
   *          the json string.
   * @param pathString
   *          the json path expression.
   * @return json string or null when an error happens.
   */
  public Text evaluate(String jsonString, String pathString) {
    if (jsonString == null || jsonString.isEmpty() || pathString == null
        || pathString.isEmpty() || pathString.charAt(0) != '$') {
      return null;
    }

    int pathExprStart = 1;
    boolean unknownType = pathString.equals("$");
    boolean isRootArray = false;

    if (pathString.length() > 1) {
      if (pathString.charAt(1) == '[') {
        pathExprStart = 0;
        isRootArray = true;
      } else if (pathString.charAt(1) == '.') {
        isRootArray = pathString.length() > 2 && pathString.charAt(2) == '[';
      } else {
        return null;
      }
    }

    // Cache pathExpr
    String[] pathExpr = pathExprCache.get(pathString);
    if (pathExpr == null) {
      pathExpr = pathString.split("\\.", -1);
      pathExprCache.put(pathString, pathExpr);
    }

    // Cache extractObject
    Object extractObject = extractObjectCache.get(jsonString);
    if (extractObject == null) {
      if (unknownType) {
        try {
          extractObject = objectMapper.readValue(jsonString, LIST_TYPE);
        } catch (Exception e) {
          // Ignore exception
        }
        if (extractObject == null) {
          try {
            extractObject = objectMapper.readValue(jsonString, MAP_TYPE);
          } catch (Exception e) {
            return null;
          }
        }
      } else {
        JavaType javaType = isRootArray ? LIST_TYPE : MAP_TYPE;
        try {
          extractObject = objectMapper.readValue(jsonString, javaType);
        } catch (Exception e) {
          return null;
        }
      }
      extractObjectCache.put(jsonString, extractObject);
    }

    for (int i = pathExprStart; i < pathExpr.length; i++) {
      if (extractObject == null) {
          return null;
      }
      extractObject = extract(extractObject, pathExpr[i], i == pathExprStart && isRootArray);
    }

    Text result = new Text();
    if (extractObject instanceof Map || extractObject instanceof List) {
      try {
        result.set(objectMapper.writeValueAsString(extractObject));
      } catch (Exception e) {
        return null;
      }
    } else if (extractObject != null) {
      result.set(extractObject.toString());
    } else {
      return null;
    }
    return result;
  }

  private Object extract(Object json, String path, boolean skipMapProc) {
    // skip MAP processing for the first path element if root is array
    if (!skipMapProc) {
      // Cache patternkey.matcher(path).matches()
      Matcher mKey = null;
      Boolean mKeyMatches = mKeyMatchesCache.get(path);
      if (mKeyMatches == null) {
        mKey = patternKey.matcher(path);
        mKeyMatches = mKey.matches() ? Boolean.TRUE : Boolean.FALSE;
        mKeyMatchesCache.put(path, mKeyMatches);
      }
      if (!mKeyMatches.booleanValue()) {
        return null;
      }

      // Cache mkey.group(1)
      String mKeyGroup1 = mKeyGroup1Cache.get(path);
      if (mKeyGroup1 == null) {
        if (mKey == null) {
          mKey = patternKey.matcher(path);
          mKeyMatches = mKey.matches() ? Boolean.TRUE : Boolean.FALSE;
          mKeyMatchesCache.put(path, mKeyMatches);
          if (!mKeyMatches.booleanValue()) {
            return null;
          }
        }
        mKeyGroup1 = mKey.group(1);
        mKeyGroup1Cache.put(path, mKeyGroup1);
      }
      json = extract_json_withkey(json, mKeyGroup1);
    }
    // Cache indexList
    ArrayList<String> indexList = indexListCache.get(path);
    if (indexList == null) {
      Matcher mIndex = patternIndex.matcher(path);
      indexList = new ArrayList<String>();
      while (mIndex.find()) {
        indexList.add(mIndex.group(1));
      }
      indexListCache.put(path, indexList);
    }

    if (indexList.size() > 0) {
      json = extract_json_withindex(json, indexList);
    }

    return json;
  }

  private transient AddingList jsonList = new AddingList();

  private static class AddingList extends ArrayList<Object> {
    private static final long serialVersionUID = 1L;

    @Override
    public Iterator<Object> iterator() {
      return Iterators.forArray(toArray());
    }
    @Override
    public void removeRange(int fromIndex, int toIndex) {
      super.removeRange(fromIndex, toIndex);
    }
  };

  @SuppressWarnings("unchecked")
  private Object extract_json_withindex(Object json, ArrayList<String> indexList) {
    jsonList.clear();
    jsonList.add(json);
    for (String index : indexList) {
      int targets = jsonList.size();
      if (index.equalsIgnoreCase("*")) {
        for (Object array : jsonList) {
          if (array instanceof List) {
            for (int j = 0; j < ((List<Object>)array).size(); j++) {
              jsonList.add(((List<Object>)array).get(j));
            }
          }
        }
      } else {
        for (Object array : jsonList) {
          int indexValue = Integer.parseInt(index);
          if (!(array instanceof List)) {
            continue;
          }
          List<Object> list = (List<Object>) array;
          if (indexValue >= list.size()) {
            continue;
          }
          jsonList.add(list.get(indexValue));
        }
      }
      if (jsonList.size() == targets) {
        return null;
      }
      jsonList.removeRange(0, targets);
    }
    if (jsonList.isEmpty()) {
      return null;
    }
    return (jsonList.size() > 1) ? new ArrayList<Object>(jsonList) : jsonList.get(0);
  }

  @SuppressWarnings("unchecked")
  private Object extract_json_withkey(Object json, String path) {
    if (json instanceof List) {
      List<Object> jsonArray = new ArrayList<Object>();
      for (int i = 0; i < ((List<Object>) json).size(); i++) {
        Object json_elem = ((List<Object>) json).get(i);
        Object json_obj = null;
        if (json_elem instanceof Map) {
          json_obj = ((Map<String, Object>) json_elem).get(path);
        } else {
          continue;
        }
        if (json_obj instanceof List) {
          for (int j = 0; j < ((List<Object>) json_obj).size(); j++) {
            jsonArray.add(((List<Object>) json_obj).get(j));
          }
        } else if (json_obj != null) {
          jsonArray.add(json_obj);
        }
      }
      return (jsonArray.size() == 0) ? null : jsonArray;
    } else if (json instanceof Map) {
      return ((Map<String, Object>) json).get(path);
    } else {
      return null;
    }
  }
}
