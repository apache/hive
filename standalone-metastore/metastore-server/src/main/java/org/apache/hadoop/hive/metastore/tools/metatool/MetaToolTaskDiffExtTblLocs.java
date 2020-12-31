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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
  private static final Logger LOG = LoggerFactory.getLogger(MetaToolTaskDiffExtTblLocs.class);
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
      JSONObject jsonObject = getDiffJson(args);
      FileWriter fw = new FileWriter(ouputDir + "/" + outFileName);
      PrintWriter pw = new PrintWriter(fw);
      pw.println(jsonObject.toString(4).replace("\\", ""));
      pw.close();
    } catch (Exception e) {
      LOG.error("Generating diff of external table locations failed: ", e);
    }
  }

  private JSONObject getDiffJson(String fileNames[]) throws IOException, JSONException {
    File file1 = new File(fileNames[0]);
    File file2 = new File(fileNames[1]);
    JSONObject inJson1 = new JSONObject(new String(Files.readAllBytes(Paths.get(file1.getAbsolutePath()))));
    JSONObject inJson2 = new JSONObject(new String(Files.readAllBytes(Paths.get(file2.getAbsolutePath()))));
    Iterator keyIter1 = inJson1.keys();
    Iterator keyIter2 = inJson2.keys();
    Set<String> keySet1 = new HashSet<>();
    Set<String> keySet2 = new HashSet<>();
    while (keyIter1.hasNext()) {
      keySet1.add(String.valueOf(keyIter1.next()));
    }
    while (keyIter2.hasNext()) {
      keySet2.add(String.valueOf(keyIter2.next()));
    }
    Set<String> uniqueLocationsFile1 = new HashSet<>();
    Set<String> uniqueLocationsFile2 = new HashSet<>();
    Map<String, HashSet<String>> modifiedLocations = new HashMap<>();
    for (String loc1 : keySet1) {
      if (!keySet2.contains(loc1)) {
        uniqueLocationsFile1.add(loc1);
      }
    }
    for (String loc2 : keySet2) {
      if (!keySet1.contains(loc2)) {
        uniqueLocationsFile2.add(loc2);
      }
    }
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
      jsonObject.put("Locations only in " + fileNames[0], uniqueLocationsFile1);
      jsonObject.put("Locations only in " + fileNames[1], uniqueLocationsFile2);
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

  private String asDeleted(String str) {
    return "- " + str;
  }

  private String asAdded(String str) {
    return "+ " + str;
  }
}