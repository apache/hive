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
package org.apache.hadoop.hive.ql;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.ql.QTestMiniClusters.FsType;
import org.apache.hadoop.hive.ql.qoption.QTestReplaceHandler;

/**
 * QOutProcessor: produces the final q.out from original q.out by postprocessing (e.g. masks)
 *
 */
public class QOutProcessor {
  public static final String PATH_HDFS_REGEX = "(hdfs://)([a-zA-Z0-9:/_\\-\\.=])+";
  public static final String PATH_HDFS_WITH_DATE_USER_GROUP_REGEX =
      "([a-z]+) ([a-z]+)([ ]+)([0-9]+) ([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}) "
          + PATH_HDFS_REGEX;

  public static final String HDFS_MASK = "### HDFS PATH ###";
  public static final String HDFS_DATE_MASK = "### HDFS DATE ###";
  public static final String HDFS_USER_MASK = "### USER ###";
  public static final String HDFS_GROUP_MASK = "### GROUP ###";

  public static final String MASK_PATTERN = "#### A masked pattern was here ####";
  public static final String PARTIAL_MASK_PATTERN = "#### A PARTIAL masked pattern was here ####";
  private static final PatternReplacementPair MASK_STATS = new PatternReplacementPair(
      Pattern.compile(" Num rows: [1-9][0-9]* Data size: [1-9][0-9]*"),
      " Num rows: ###Masked### Data size: ###Masked###");
  private static final PatternReplacementPair MASK_DATA_SIZE = new PatternReplacementPair(
      Pattern.compile(" Data size: [1-9][0-9]*"),
      " Data size: ###Masked###");
    private static final PatternReplacementPair MASK_TIMESTAMP = new PatternReplacementPair(
      Pattern.compile(
          "[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1]) (2[0-3]|[01][0-9]):[0-5][0-9]:[0-5][0-9].[0-9]{1,3} [a-zA-Z/]*"),
        "  ###MaskedTimeStamp### ");
  private static final PatternReplacementPair MASK_LINEAGE = new PatternReplacementPair(
      Pattern.compile("POSTHOOK: Lineage: .*"),
      "POSTHOOK: Lineage: ###Masked###");
  
  private FsType fsType = FsType.LOCAL;

  public static class LineProcessingResult {
    private String line;
    private boolean partialMaskWasMatched = false;

    public LineProcessingResult(String line) {
      this.line = line;
    }

    public String get() {
      return line;
    }
  }

  private final Pattern[] planMask = toPattern(new String[] {
      ".*[.][.][.] [0-9]* more.*",
      "pk_-?[0-9]*_[0-9]*_[0-9]*",
      "fk_-?[0-9]*_[0-9]*_[0-9]*",
      "uk_-?[0-9]*_[0-9]*_[0-9]*",
      "nn_-?[0-9]*_[0-9]*_[0-9]*", // not null constraint name
      "dc_-?[0-9]*_[0-9]*_[0-9]*", // default constraint name
      "org\\.apache\\.hadoop\\.hive\\.metastore\\.model\\.MConstraint@([0-9]|[a-z])*",
  });

  // Using patterns for matching the whole line can take a long time, therefore we should try to avoid it
  // in case of really long lines trying to match a .*some string.* may take up to 4 seconds each!

  // Using String.startsWith instead of pattern, as it is much faster
  private final String[] maskIfStartsWith = new String[] {
      "Deleted",
      "Repair: Added partition to metastore",
      "latestOffsets",
      "minimumLag"
  };

  // Using String.contains instead of pattern, as it is much faster
  private final String[] maskIfContains = new String[] {
      "file:",
      "pfile:",
      "/tmp/",
      "invalidscheme:",
      "lastUpdateTime",
      "lastAccessTime",
      "lastModifiedTim",
      "Owner",
      "owner",
      "CreateTime",
      "LastAccessTime",
      "Location",
      "LOCATION '",
      "transient_lastDdlTime",
      "last_modified_",
      "at org",
      "at sun",
      "at java",
      "at junit",
      "LOCK_QUERYID:",
      "LOCK_TIME:",
      "grantTime",
      "job_",
      "USING 'java -cp",
      "DagName:",
      "DagId:",
      "total number of created files now is",
      "hive-staging",
      "at com.sun.proxy",
      "at com.jolbox",
      "at com.zaxxer",
      "at com.google"
  };

  // Using String.contains instead of pattern, as it is much faster
  private final String[][] maskIfContainsMultiple = new String[][] {
    {"Input:", "/data/files/"},
    {"Output:", "/data/files/"}
  };

  private enum Mask {
    STATS("-- MASK_STATS"), DATASIZE("-- MASK_DATA_SIZE"), LINEAGE("-- MASK_LINEAGE"), TIMESTAMP("-- MASK_TIMESTAMP");
    private Pattern pattern;

    Mask(String pattern) {
      this.pattern = Pattern.compile(pattern);
    }

    boolean existsIn(String query) {
      return pattern.matcher(query).find();
    }
  }

  private final QTestReplaceHandler replaceHandler;
  private final Set<Mask> queryMasks = new HashSet<>();

  public QOutProcessor(FsType fsType, QTestReplaceHandler replaceHandler) {
    this.fsType = fsType;
    this.replaceHandler = replaceHandler;
  }

  private Pattern[] toPattern(String[] patternStrs) {
    Pattern[] patterns = new Pattern[patternStrs.length];
    for (int i = 0; i < patternStrs.length; i++) {
      patterns[i] = Pattern.compile(patternStrs[i]);
    }
    return patterns;
  }

  public void maskPatterns(String fname) throws Exception {

    String line;
    BufferedReader in;
    BufferedWriter out;

    File file = new File(fname);
    File fileOrig = new File(fname + ".orig");
    FileUtils.copyFile(file, fileOrig);

    in = new BufferedReader(new InputStreamReader(new FileInputStream(fileOrig), "UTF-8"));
    out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"));

    boolean lastWasMasked = false;

    while (null != (line = in.readLine())) {
      LineProcessingResult result = processLine(line);

      if (result.line.equals(MASK_PATTERN)) {
        // We're folding multiple masked lines into one.
        if (!lastWasMasked) {
          out.write(result.line);
          out.write("\n");
          lastWasMasked = true;
          result.partialMaskWasMatched = false;
        }
      } else {
        out.write(result.line);
        out.write("\n");
        lastWasMasked = false;
        result.partialMaskWasMatched = false;
      }
    }

    in.close();
    out.close();
  }

  LineProcessingResult processLine(String line) {
    LineProcessingResult result = new LineProcessingResult(line);

    Matcher matcher = null;

    result.line = replaceHandler.processLine(result.line);

    if (fsType == FsType.ENCRYPTED_HDFS) {
      for (Pattern pattern : partialReservedPlanMask) {
        matcher = pattern.matcher(result.line);
        if (matcher.find()) {
          result.line = PARTIAL_MASK_PATTERN + " " + matcher.group(0);
          result.partialMaskWasMatched = true;
          break;
        }
      }
    }
    else {
      for (PatternReplacementPair prp : partialPlanMask) {
        matcher = prp.pattern.matcher(result.line);
        if (matcher.find()) {
          result.line = result.line.replaceAll(prp.pattern.pattern(), prp.replacement);
          result.partialMaskWasMatched = true;
        }
      }
    }

    if (!result.partialMaskWasMatched) {
      for (Pair<Pattern, String> pair : patternsWithMaskComments) {
        Pattern pattern = pair.getLeft();
        String maskComment = pair.getRight();

        matcher = pattern.matcher(result.line);
        if (matcher.find()) {
          result.line = matcher.replaceAll(maskComment);
          result.partialMaskWasMatched = true;
          break;
        }
      }

      maskPattern(result, Mask.STATS, MASK_STATS);
      maskPattern(result, Mask.DATASIZE, MASK_DATA_SIZE);
      maskPattern(result, Mask.LINEAGE,  MASK_LINEAGE);
      maskPattern(result, Mask.TIMESTAMP, MASK_TIMESTAMP);

      for (String prefix : maskIfStartsWith) {
        if (result.line.startsWith(prefix)) {
          result.line = MASK_PATTERN;
        }
      }

      for (String word : maskIfContains) {
        if (result.line.contains(word)) {
          result.line = MASK_PATTERN;
        }
      }

      for (String[] words : maskIfContainsMultiple) {
        int pos = 0;
        boolean containsAllInOrder = true;
        for (String word : words) {
          int wordPos = result.line.substring(pos).indexOf(word);
          if (wordPos == -1) {
            containsAllInOrder = false;
            break;
          } else {
            pos += wordPos + word.length();
          }
        }
        if (containsAllInOrder) {
          result.line = MASK_PATTERN;
        }
      }

      for (Pattern pattern : planMask) {
        result.line = pattern.matcher(result.line).replaceAll(MASK_PATTERN);
      }
    }

    return result;
  }

  private void maskPattern(LineProcessingResult result, Mask mask, PatternReplacementPair patternReplacementPair) {
    if (!result.partialMaskWasMatched && queryMasks.contains(mask)) {
      if (patternReplacementPair.pattern.matcher(result.line).find()) {
        result.line = result.line.replaceAll(patternReplacementPair.pattern.pattern(), patternReplacementPair.replacement);
        result.partialMaskWasMatched = true;
      }
    }
  }

  private final Pattern[] partialReservedPlanMask = toPattern(new String[] {
      "data/warehouse/(.*?/)+\\.hive-staging"  // the directory might be db/table/partition
      //TODO: add more expected test result here
  });
  /**
   * Pattern to match and (partial) replacement text.
   * For example, {"transaction":76,"bucketid":8249877}.  We just want to mask 76 but a regex that
   * matches just 76 will match a lot of other things.
   */
  private final static class PatternReplacementPair {
    private final Pattern pattern;
    private final String replacement;
    PatternReplacementPair(Pattern p, String r) {
      pattern = p;
      replacement = r;
    }
  }
  private final PatternReplacementPair[] partialPlanMask;
  {
    ArrayList<PatternReplacementPair> ppm = new ArrayList<>();
    ppm.add(new PatternReplacementPair(Pattern.compile("\\{\"writeid\":[1-9][0-9]*,\"bucketid\":"),
        "{\"writeid\":### Masked writeid ###,\"bucketid\":"));

    ppm.add(new PatternReplacementPair(Pattern.compile("attempt_[0-9_]+"), "attempt_#ID#"));
    ppm.add(new PatternReplacementPair(Pattern.compile("vertex_[0-9_]+"), "vertex_#ID#"));
    ppm.add(new PatternReplacementPair(Pattern.compile("task_[0-9_]+"), "task_#ID#"));

    // since TEZ-4506, the node is reported with task attempt failures, which needs to be masked
    ppm.add(new PatternReplacementPair(Pattern.compile("Error: Node: (.*) : Error while running task"),
        "Error: Node: #NODE# : Error while running task"));

    ppm.add(new PatternReplacementPair(Pattern.compile("rowcount = [0-9]+(\\.[0-9]+(E[0-9]+)?)?, cumulative cost = \\{.*\\}, id = [0-9]*"),
        "rowcount = ###Masked###, cumulative cost = ###Masked###, id = ###Masked###"));

    // When a vertex is killed due to failure of upstream vertex, logs are in arbitrary order.
    // We do not want the test to fail because of this.
    ppm.add(new PatternReplacementPair(
        Pattern.compile("Vertex killed, vertexName=(.*?),.*\\[\\1\\] killed\\/failed due to:OTHER_VERTEX_FAILURE\\]"),
        "[Masked Vertex killed due to OTHER_VERTEX_FAILURE]"));

    partialPlanMask = ppm.toArray(new PatternReplacementPair[ppm.size()]);
  }

  @SuppressWarnings("serial")
  private ArrayList<Pair<Pattern, String>> initPatternWithMaskComments() {
    return new ArrayList<Pair<Pattern, String>>() {
      {
        add(toPatternPair("(pblob|s3.?|swift|wasb.?).*hive-staging.*",
            "### BLOBSTORE_STAGING_PATH ###"));
        add(toPatternPair(PATH_HDFS_WITH_DATE_USER_GROUP_REGEX, String.format("%s %s$3$4 %s $6%s",
            HDFS_USER_MASK, HDFS_GROUP_MASK, HDFS_DATE_MASK, HDFS_MASK)));
        add(toPatternPair(PATH_HDFS_REGEX, String.format("$1%s", HDFS_MASK)));
        add(toPatternPair("(.*totalSize\\s*=*\\s*)\\d+\\s*(.*)", "$1#Masked#$2"));
      }
    };
  }

  /* This list may be modified by specific cli drivers to mask strings that change on every test */
  private List<Pair<Pattern, String>> patternsWithMaskComments = initPatternWithMaskComments();

  private Pair<Pattern, String> toPatternPair(String patternStr, String maskComment) {
    return ImmutablePair.of(Pattern.compile(patternStr), maskComment);
  }

  public void addPatternWithMaskComment(String patternStr, String maskComment) {
    patternsWithMaskComments.add(toPatternPair(patternStr, maskComment));
  }

  public void initMasks(String query) {
    queryMasks.clear();
    for (Mask m : Mask.values()) {
      if (m.existsIn(query)) {
        queryMasks.add(m);
      }
    }
  }

  public void resetPatternwithMaskComments() {
    patternsWithMaskComments = initPatternWithMaskComments();
  }

}
