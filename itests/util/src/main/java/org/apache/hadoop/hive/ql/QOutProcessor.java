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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.ql.QTestUtil.FsType;

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
  private static final PatternReplacementPair MASK_LINEAGE = new PatternReplacementPair(
      Pattern.compile("POSTHOOK: Lineage: .*"),
      "POSTHOOK: Lineage: ###Masked###");

  private FsType fsType = FsType.local;

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
      ".*file:.*",
      ".*pfile:.*",
      ".*/tmp/.*",
      ".*invalidscheme:.*",
      ".*lastUpdateTime.*",
      ".*lastAccessTime.*",
      ".*lastModifiedTime.*",
      ".*[Oo]wner.*",
      ".*CreateTime.*",
      ".*LastAccessTime.*",
      ".*Location.*",
      ".*LOCATION '.*",
      ".*transient_lastDdlTime.*",
      ".*last_modified_.*",
      ".*at org.*",
      ".*at sun.*",
      ".*at java.*",
      ".*at junit.*",
      ".*Caused by:.*",
      ".*LOCK_QUERYID:.*",
      ".*LOCK_TIME:.*",
      ".*grantTime.*",
      ".*[.][.][.] [0-9]* more.*",
      ".*job_[0-9_]*.*",
      ".*job_local[0-9_]*.*",
      ".*USING 'java -cp.*",
      "^Deleted.*",
      ".*DagName:.*",
      ".*DagId:.*",
      ".*Input:.*/data/files/.*",
      ".*Output:.*/data/files/.*",
      ".*total number of created files now is.*",
      ".*.hive-staging.*",
      "pk_-?[0-9]*_[0-9]*_[0-9]*",
      "fk_-?[0-9]*_[0-9]*_[0-9]*",
      "uk_-?[0-9]*_[0-9]*_[0-9]*",
      "nn_-?[0-9]*_[0-9]*_[0-9]*", // not null constraint name
      "dc_-?[0-9]*_[0-9]*_[0-9]*", // default constraint name
      ".*at com\\.sun\\.proxy.*",
      ".*at com\\.jolbox.*",
      ".*at com\\.zaxxer.*",
      "org\\.apache\\.hadoop\\.hive\\.metastore\\.model\\.MConstraint@([0-9]|[a-z])*",
      "^Repair: Added partition to metastore.*",
      "^latestOffsets.*",
      "^minimumLag.*"
  });

  public QOutProcessor(FsType fsType) {
    this.fsType = fsType;
  }

  public QOutProcessor() {
    this.fsType = FsType.hdfs;
  }
  
  private Pattern[] toPattern(String[] patternStrs) {
    Pattern[] patterns = new Pattern[patternStrs.length];
    for (int i = 0; i < patternStrs.length; i++) {
      patterns[i] = Pattern.compile(patternStrs[i]);
    }
    return patterns;
  }

  public void maskPatterns(String fname, boolean maskStats, boolean maskDataSize, boolean maskLineage) throws Exception {
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
      LineProcessingResult result = processLine(line, maskStats, maskDataSize, maskLineage);

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

  public LineProcessingResult processLine(String line, boolean maskStats, boolean maskDataSize, boolean maskLineage) {
    LineProcessingResult result = new LineProcessingResult(line);
    
    Matcher matcher = null;

    if (fsType == FsType.encrypted_hdfs) {
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

      if (!result.partialMaskWasMatched && maskStats) {
        matcher = MASK_STATS.pattern.matcher(result.line);
        if (matcher.find()) {
          result.line = result.line.replaceAll(MASK_STATS.pattern.pattern(), MASK_STATS.replacement);
          result.partialMaskWasMatched = true;
        }
      }

      if (!result.partialMaskWasMatched && maskDataSize) {
        matcher = MASK_DATA_SIZE.pattern.matcher(result.line);
        if (matcher.find()) {
          result.line = result.line.replaceAll(MASK_DATA_SIZE.pattern.pattern(), MASK_DATA_SIZE.replacement);
          result.partialMaskWasMatched = true;
        }
      }

      if (!result.partialMaskWasMatched && maskLineage) {
        matcher = MASK_LINEAGE.pattern.matcher(result.line);
        if (matcher.find()) {
          result.line = result.line.replaceAll(MASK_LINEAGE.pattern.pattern(), MASK_LINEAGE.replacement);
          result.partialMaskWasMatched = true;
        }
      }

      for (Pattern pattern : planMask) {
        result.line = pattern.matcher(result.line).replaceAll(MASK_PATTERN);
      }
    }
    
    return result;
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
    partialPlanMask = ppm.toArray(new PatternReplacementPair[ppm.size()]);
  }
  /* This list may be modified by specific cli drivers to mask strings that change on every test */
  @SuppressWarnings("serial")
  private final List<Pair<Pattern, String>> patternsWithMaskComments =
      new ArrayList<Pair<Pattern, String>>() {
        {
          add(toPatternPair("(pblob|s3.?|swift|wasb.?).*hive-staging.*",
              "### BLOBSTORE_STAGING_PATH ###"));
          add(toPatternPair(PATH_HDFS_WITH_DATE_USER_GROUP_REGEX, String.format("%s %s$3$4 %s $6%s",
              HDFS_USER_MASK, HDFS_GROUP_MASK, HDFS_DATE_MASK, HDFS_MASK)));
          add(toPatternPair(PATH_HDFS_REGEX, String.format("$1%s", HDFS_MASK)));
        }
      };

  private Pair<Pattern, String> toPatternPair(String patternStr, String maskComment) {
    return ImmutablePair.of(Pattern.compile(patternStr), maskComment);
  }

  public void addPatternWithMaskComment(String patternStr, String maskComment) {
    patternsWithMaskComments.add(toPatternPair(patternStr, maskComment));
  }
  
}
