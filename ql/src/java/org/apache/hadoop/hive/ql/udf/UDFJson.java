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
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.Text;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

@Description(name = "get_json_object", value = "_FUNC_(json_txt, path) - Extract a json object from path ", extended = "Extract json object from a json string based on json path "
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
  private static Log LOG = LogFactory.getLog(UDFJson.class.getName());
  private final Pattern patternKey = Pattern.compile("^([a-zA-Z0-9_\\-]+).*");
  private final Pattern patternIndex = Pattern.compile("\\[([0-9]+|\\*)\\]");

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

  static Map<String, Object> extractObjectCache = new HashCache<String, Object>();
  static Map<String, String[]> pathExprCache = new HashCache<String, String[]>();
  static Map<String, ArrayList<String>> indexListCache = new HashCache<String, ArrayList<String>>();
  static Map<String, String> mKeyGroup1Cache = new HashCache<String, String>();
  static Map<String, Boolean> mKeyMatchesCache = new HashCache<String, Boolean>();

  Text result = new Text();

  public UDFJson() {
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

    if (jsonString == null || jsonString == "" || pathString == null
        || pathString == "") {
      return null;
    }

    try {
      // Cache pathExpr
      String[] pathExpr = pathExprCache.get(pathString);
      if (pathExpr == null) {
        pathExpr = pathString.split("\\.", -1);
        pathExprCache.put(pathString, pathExpr);
      }

      if (!pathExpr[0].equalsIgnoreCase("$")) {
        return null;
      }
      // Cache extractObject
      Object extractObject = extractObjectCache.get(jsonString);
      if (extractObject == null) {
        extractObject = new JSONObject(jsonString);
        extractObjectCache.put(jsonString, extractObject);
      }
      for (int i = 1; i < pathExpr.length; i++) {
        extractObject = extract(extractObject, pathExpr[i]);
      }
      result.set(extractObject.toString());
      return result;
    } catch (Exception e) {
      return null;
    }
  }

  private Object extract(Object json, String path) throws JSONException {

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
      }
      mKeyGroup1 = mKey.group(1);
      mKeyGroup1Cache.put(path, mKeyGroup1);
    }
    json = extract_json_withkey(json, mKeyGroup1);

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

  ArrayList<Object> jsonList = new ArrayList<Object>();

  private Object extract_json_withindex(Object json, ArrayList<String> indexList)
      throws JSONException {

    jsonList.clear();
    jsonList.add(json);
    Iterator<String> itr = indexList.iterator();
    while (itr.hasNext()) {
      String index = itr.next();
      ArrayList<Object> tmp_jsonList = new ArrayList<Object>();
      if (index.equalsIgnoreCase("*")) {
        for (int i = 0; i < (jsonList).size(); i++) {
          try {
            JSONArray array = (JSONArray) (jsonList).get(i);
            for (int j = 0; j < array.length(); j++) {
              tmp_jsonList.add(array.get(j));
            }
          } catch (Exception e) {
            continue;
          }
        }
        jsonList = tmp_jsonList;
      } else {
        for (int i = 0; i < (jsonList).size(); i++) {
          try {
            tmp_jsonList.add(((JSONArray) (jsonList).get(i)).get(Integer
                .parseInt(index)));
          } catch (ClassCastException e) {
            continue;
          } catch (JSONException e) {
            return null;
          }
          jsonList = tmp_jsonList;
        }
      }
    }
    return (jsonList.size() > 1) ? new JSONArray(jsonList) : jsonList.get(0);
  }

  private Object extract_json_withkey(Object json, String path)
      throws JSONException {
    if (json.getClass() == org.json.JSONArray.class) {
      JSONArray jsonArray = new JSONArray();
      for (int i = 0; i < ((JSONArray) json).length(); i++) {
        Object josn_elem = ((JSONArray) json).get(i);
        try {
          Object json_obj = ((JSONObject) josn_elem).get(path);
          if (json_obj.getClass() == org.json.JSONArray.class) {
            for (int j = 0; j < ((JSONArray) json_obj).length(); j++) {
              jsonArray.put(((JSONArray) json_obj).get(j));
            }
          } else {
            jsonArray.put(json_obj);
          }
        } catch (Exception e) {
          continue;
        }
      }
      return (jsonArray.length() == 0) ? null : jsonArray;
    } else {
      return ((JSONObject) json).get(path);
    }
  }
}
