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
import java.util.Collection;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.Text;

@description(
    name = "get_json_object",
    value = "_FUNC_(json_txt, path) - Extract a json object from path ",
    extended = "Extract json object from a json string based on json path " +
    		"specified, and return json string of the extracted json object. It " +
    		"will return null if the input json string is invalid.\n" +
        "A limited version of JSONPath supported:\n" +
        "  $   : Root object\n" +
        "  .   : Child operator\n" +
        "  []  : Subscript operator for array\n" +
        "  *   : Wildcard for []\n" +
        "Syntax not supported that's worth noticing:\n" +
        "  ''  : Zero length string as key\n" +
        "  ..  : Recursive descent\n" +
        "  &amp;#064;   : Current object/element\n" +
        "  ()  : Script expression\n" +
        "  ?() : Filter (script) expression.\n" +
        "  [,] : Union operator\n" +
        "  [start:end:step] : array slice operator\n"
    )
public class UDFJson extends UDF {
  private static Log LOG = LogFactory.getLog(UDFJson.class.getName());
  private Pattern pattern_key = Pattern.compile("^([a-zA-Z0-9_\\-]+).*");
  private Pattern pattern_index = Pattern.compile("\\[([0-9]+|\\*)\\]");

  Text result = new Text();
  public UDFJson() {
  }

  /**
   * Extract json object from a json string based on json path specified,
   * and return json string of the extracted json object. It will return null
   * if the input json string is invalid.
   *
   * A limited version of JSONPath supported:
   *    $   : Root object
   *    .   : Child operator
   *    []  : Subscript operator for array
   *    *   : Wildcard for []
   *
   * Syntax not supported that's worth noticing:
   *    ''  : Zero length string as key
   *    ..  : Recursive descent
   *    &amp;#064;   : Current object/element
   *    ()  : Script expression
   *    ?() : Filter (script) expression.
   *    [,] : Union operator
   *    [start:end:step] : array slice operator
   *
   * @param jsonText the json string.
   * @param pathText the json path expression.
   * @return json string or null when error happens.
   */
  public Text evaluate(Text jsonText, Text pathText) {
    if (jsonText == null || pathText == null) {
      return null;
    }

    String jsonString = jsonText.toString();
    String pathString = pathText.toString();
    
    try {
      String[] pathExpr = pathString.split("\\.", -1);
      if (!pathExpr[0].equalsIgnoreCase("$")) {
        return null;
      }
      Object extractObject = new JSONObject(jsonString);
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
    Matcher m_key = pattern_key.matcher(path);
    if (!m_key.matches()) {
      return null;
    }
    json = extract_json_withkey(json, m_key.group(1));

    Matcher m_index = pattern_index.matcher(path);
    ArrayList<String> index_list = new ArrayList<String>();
    while (m_index.find()) {
      index_list.add(m_index.group(1));
    }
    if (index_list.size() > 0) {
      json = extract_json_withindex(json, index_list);
    }

    return json;
  }

  private Object extract_json_withindex(Object json, ArrayList<String> indexList)
      throws JSONException {
    ArrayList<Object> jsonList = new ArrayList<Object>();
    jsonList.add(json);
    Iterator<String> itr = indexList.iterator();
    while (itr.hasNext()) {
      String index = itr.next();
      if (index.equalsIgnoreCase("*")) {
        ArrayList<Object> tmp_jsonList = new ArrayList<Object>();
        for (int i = 0; i < ((ArrayList<Object>) jsonList).size(); i++) {
          try {
            JSONArray array = (JSONArray) ((ArrayList<Object>) jsonList).get(i);
            for (int j = 0; j < array.length(); j++) {
              tmp_jsonList.add(array.get(j));
            }
          } catch (Exception e) {
            continue;
          }
        }
        jsonList = tmp_jsonList;
      } else {
        ArrayList<Object> tmp_jsonList = new ArrayList<Object>();
        for (int i = 0; i < ((ArrayList<Object>) jsonList).size(); i++) {
          try {
            tmp_jsonList
                .add(((JSONArray) ((ArrayList<Object>) jsonList).get(i))
                    .get(Integer.parseInt(index)));
          } catch (ClassCastException e) {
            continue;
          } catch (JSONException e) {
            return null;
          }
          jsonList = tmp_jsonList;
        }
      }
    }
    return (jsonList.size() > 1) ? new JSONArray((Collection) jsonList)
        : jsonList.get(0);
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
