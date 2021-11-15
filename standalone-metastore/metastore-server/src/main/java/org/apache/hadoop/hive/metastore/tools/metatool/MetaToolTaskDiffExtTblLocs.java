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
package org.apache.hadoop.hive.metastore.tools.metatool;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MetaToolTaskDiffExtTblLocs extends MetaToolTask {
  @Override
  void execute() {
    String[] args = getCl().getDiffExtTblLocsParams();
    try {
      File file1 = new File(args[0]);
      File file2 = new File(args[1]);
      String ouputDir = args[2];
      String outFileName = "diff_" + System.currentTimeMillis();
      System.out.println("Writing diff to " + outFileName);
      if (!file1.exists()) {
        System.out.println("Input " + args[0] + " does not exist.");
        return;
      }
      if (!file2.exists()) {
        System.out.println("Input " + args[1] + " does not exist.");
        return;
      }
      JSONObject jsonObject = getDiffJson(file1, file2);
      FileWriter fw = new FileWriter(ouputDir + "/" + outFileName);
      PrintWriter pw = new PrintWriter(fw);
      pw.println(jsonObject.toString(4).replace("\\", ""));
      pw.close();
    } catch (Exception e) {
      System.out.println("Generating diff failed: \n" + e.getMessage());
    }
  }

  private JSONObject getDiffJson(File file1, File file2) throws IOException, JSONException {
    JSONObject inJson1 = new JSONObject(new String(Files.readAllBytes(Paths.get(file1.getAbsolutePath()))));
    JSONObject inJson2 = new JSONObject(new String(Files.readAllBytes(Paths.get(file2.getAbsolutePath()))));
    Map<String, HashSet<String>> modifiedLocations = new HashMap<>();
    Set<String> keySet1 = getKeySet(inJson1);
    Set<String> keySet2 = getKeySet(inJson2);
    Set<String> uniqueLocationsFile1 = getSetDifference(keySet1, keySet2);
    Set<String> uniqueLocationsFile2 = getSetDifference(keySet2, keySet1);
    for (String loc : keySet1) {
      if (!uniqueLocationsFile1.contains(loc)) {
        //common key, we need to compare the values
        JSONArray valArr1 = inJson1.getJSONArray(loc);
        JSONArray valArr2 = inJson2.getJSONArray(loc);
        for (int i = 0; i < valArr1.length(); i++) {
          String val1 = valArr1.getString(i);
          boolean absentFromSecondKey = true;
          for (int j = 0; j < valArr2.length(); j++) {
            String val2 = valArr2.getString(j);
            if (val1.equalsIgnoreCase(val2)) {
              absentFromSecondKey = false;
              break;
            }
          }
          if (absentFromSecondKey) {
            if (modifiedLocations.containsKey(loc)) {
              modifiedLocations.get(loc).add(asDeleted(val1));
            } else {
              modifiedLocations.put(loc, new HashSet<>());
              modifiedLocations.get(loc).add(asDeleted(val1));
            }
          }
        }
        for (int i = 0; i < valArr2.length(); i++) {
          String val2 = valArr2.getString(i);
          boolean absentFromFirstKey = true;
          for (int j = 0; j < valArr1.length(); j++) {
            String val1 = valArr1.getString(j);
            if (val1.equalsIgnoreCase(val2)) {
              absentFromFirstKey = false;
              break;
            }
          }
          if (absentFromFirstKey) {
            if (modifiedLocations.containsKey(loc)) {
              modifiedLocations.get(loc).add(asAdded(val2));
            } else {
              modifiedLocations.put(loc, new HashSet<>());
              modifiedLocations.get(loc).add(asAdded(val2));
            }
          }
        }
      }
    }
    JSONObject jsonObject = new JSONObject();
    if(!uniqueLocationsFile1.isEmpty() || !uniqueLocationsFile2.isEmpty()) {
      jsonObject.put("Locations only in " + file1.getName(), uniqueLocationsFile1);
      jsonObject.put("Locations only in " + file2.getName(), uniqueLocationsFile2);
    }
    for(String commonLoc : modifiedLocations.keySet()) {
      List<String> modifiedEntries = new ArrayList<>();
      for (String entry : modifiedLocations.get(commonLoc)) {
        modifiedEntries.add(entry);
      }
      Collections.sort(modifiedEntries);
      jsonObject.put(commonLoc, modifiedEntries);
    }
    return jsonObject;
  }

  private Set<String> getKeySet(JSONObject jsonObject) {
    Iterator<String> keyIter = jsonObject.keys();
    Set<String> keySet = new HashSet();
    while (keyIter.hasNext()) {
      keySet.add(keyIter.next());
    }
    return keySet;
  }

  private Set<String> getSetDifference(Set<String> keySet1, Set<String> keySet2) {
    Set<String> diffSet = new HashSet();
    for(String elem : keySet1) {
      if(!keySet2.contains(elem)) {
        diffSet.add(elem);
      }
    }
    return diffSet;
  }

  private String asDeleted(String str) {
    return "- " + str;
  }

  private String asAdded(String str) {
    return "+ " + str;
  }
}