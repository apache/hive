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
package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.ql.exec.BucketMatcher;

/**
 * was inner class of MapreLocalWork. context for bucket mapjoin (or smb join)
 */
public class BucketMapJoinContext implements Serializable {

  private static final long serialVersionUID = 1L;

  // table alias (small) --> input file name (big) --> target file names (small)
  private Map<String, Map<String, List<String>>> aliasBucketFileNameMapping;
  private String mapJoinBigTableAlias;
  private Class<? extends BucketMatcher> bucketMatcherClass;

  // summary of aliasBucketFileNameMapping for test result
  // full paths are replaced with base filenames
  private transient Map<String, Map<String, List<String>>> aliasBucketBaseFileNameMapping;

  // input file name (big) to bucket number
  private Map<String, Integer> bucketFileNameMapping;

  // partition spec string to input file names (big)
  private Map<String, List<String>> bigTablePartSpecToFileMapping;

  // inverse of partSpecToFileMapping, populated at runtime
  private transient Map<String, String> inputToPartSpecMapping;

  public BucketMapJoinContext() {}

  public BucketMapJoinContext(MapJoinDesc clone) {
    this.mapJoinBigTableAlias = clone.getBigTableAlias();
    this.aliasBucketFileNameMapping = clone.getAliasBucketFileNameMapping();
    this.bucketFileNameMapping = clone.getBigTableBucketNumMapping();
    this.bigTablePartSpecToFileMapping = clone.getBigTablePartSpecToFileMapping();
  }

  public void setMapJoinBigTableAlias(String bigTableAlias) {
    this.mapJoinBigTableAlias = bigTableAlias;
  }

  public void deriveBucketMapJoinMapping() {
    if (aliasBucketFileNameMapping != null) {
      aliasBucketBaseFileNameMapping = new LinkedHashMap<String, Map<String, List<String>>>();

      for (Map.Entry<String, Map<String, List<String>>> aliasToMappins
          : aliasBucketFileNameMapping.entrySet()) {
        String tableAlias = aliasToMappins.getKey();
        Map<String, List<String>> fullPathMappings = aliasToMappins.getValue();

        Map<String, List<String>> baseFileNameMapping = new LinkedHashMap<String, List<String>>();
        for (Map.Entry<String, List<String>> inputToBuckets : fullPathMappings.entrySet()) {
          // For a given table and its bucket full file path list,
          // only keep the base file name (remove file path etc).
          // And put the new list into the new mapping.
          String inputPath = inputToBuckets.getKey();
          List<String> bucketPaths = inputToBuckets.getValue();

          List<String> bucketBaseFileNames = new ArrayList<String>(bucketPaths.size());
          //for each bucket file, only keep its base files and store into a new list.
          for (String bucketFName : bucketPaths) {
            bucketBaseFileNames.add(getBaseFileName(bucketFName));
          }
          //put the new mapping
          baseFileNameMapping.put(getBaseFileName(inputPath), bucketBaseFileNames);
        }
        aliasBucketBaseFileNameMapping.put(tableAlias, baseFileNameMapping);
      }
    }
  }

  private static final Pattern partPattern = Pattern.compile("^[^=]+=[^=]+$");

  // extract partition spec to file name part from path
  private String getBaseFileName(String string) {
    try {
      Path path = new Path(string);
      Path cursor = path.getParent();
      while (partPattern.matcher(cursor.getName()).matches()) {
        cursor = cursor.getParent();
      }
      return cursor.toUri().relativize(path.toUri()).getPath();
    } catch (Exception ex) {
      // This could be due to either URI syntax error or File constructor
      // illegal arg; we don't really care which one it is.
      return string;
    }
  }

  public String getMapJoinBigTableAlias() {
    return mapJoinBigTableAlias;
  }

  public Class<? extends BucketMatcher> getBucketMatcherClass() {
    return bucketMatcherClass;
  }

  public void setBucketMatcherClass(
      Class<? extends BucketMatcher> bucketMatcherClass) {
    this.bucketMatcherClass = bucketMatcherClass;
  }

  @Explain(displayName = "Alias Bucket File Name Mapping", normalExplain = false)
  public Map<String, Map<String, List<String>>> getAliasBucketFileNameMapping() {
    return aliasBucketFileNameMapping;
  }

  public void setAliasBucketFileNameMapping(
      Map<String, Map<String, List<String>>> aliasBucketFileNameMapping) {
    this.aliasBucketFileNameMapping = aliasBucketFileNameMapping;
  }

  @Override
  public String toString() {
    if (aliasBucketFileNameMapping != null) {
      return "Mapping:" + aliasBucketFileNameMapping.toString();
    } else {
      return "";
    }
  }

  @Explain(displayName = "Alias Bucket Base File Name Mapping", normalExplain = false)
  public Map<String, Map<String, List<String>>> getAliasBucketBaseFileNameMapping() {
    return aliasBucketBaseFileNameMapping;
  }

  public void setAliasBucketBaseFileNameMapping(
      Map<String, Map<String, List<String>>> aliasBucketBaseFileNameMapping) {
    this.aliasBucketBaseFileNameMapping = aliasBucketBaseFileNameMapping;
  }

  @Explain(displayName = "Alias Bucket Output File Name Mapping", normalExplain = false)
  public Map<String, Integer> getBucketFileNameMapping() {
    return bucketFileNameMapping;
  }

  public void setBucketFileNameMapping(Map<String, Integer> bucketFileNameMapping) {
    this.bucketFileNameMapping = bucketFileNameMapping;
  }

  public Map<String, List<String>> getBigTablePartSpecToFileMapping() {
    return bigTablePartSpecToFileMapping;
  }

  public void setBigTablePartSpecToFileMapping(
      Map<String, List<String>> bigTablePartSpecToFileMapping) {
    this.bigTablePartSpecToFileMapping = bigTablePartSpecToFileMapping;
  }

  // returns fileId for SMBJoin, which consists part of result file name
  // needed to avoid file name conflict when big table is partitioned
  public String createFileId(String inputPath) {
    String bucketNum = String.valueOf(bucketFileNameMapping.get(inputPath));
    if (bigTablePartSpecToFileMapping != null) {
      // partSpecToFileMapping is null if big table is partitioned
      return prependPartSpec(inputPath, bucketNum);
    }
    return bucketNum;
  }

  // returns name of hashfile made by HASHTABLESINK which is read by MAPJOIN
  public String createFileName(String inputPath, String fileName) {
    if (bigTablePartSpecToFileMapping != null) {
      // partSpecToFileMapping is null if big table is partitioned
      return prependPartSpec(inputPath, fileName);
    }
    return fileName;
  }

  // prepends partition spec of input path to candidate file name
  private String prependPartSpec(String inputPath, String fileName) {
    Map<String, String> mapping = inputToPartSpecMapping == null ?
        inputToPartSpecMapping = revert(bigTablePartSpecToFileMapping) : inputToPartSpecMapping;
    String partSpec = mapping.get(inputPath);
    return partSpec == null || partSpec.isEmpty() ? fileName :
      "(" + FileUtils.escapePathName(partSpec) + ")" + fileName;
  }

  // revert partSpecToFileMapping to inputToPartSpecMapping
  private Map<String, String> revert(Map<String, List<String>> mapping) {
    Map<String, String> converted = new HashMap<String, String>();
    for (Map.Entry<String, List<String>> entry : mapping.entrySet()) {
      String partSpec = entry.getKey();
      for (String file : entry.getValue()) {
        converted.put(file, partSpec);
      }
    }
    return converted;
  }
}
