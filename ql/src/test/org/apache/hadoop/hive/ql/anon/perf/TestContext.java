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

package org.apache.hadoop.hive.ql.anon.perf;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.ql.anon.AnonStatementAnalyzer;
import org.apache.hadoop.hive.ql.anon.FileType;
import org.apache.hadoop.hive.ql.anon.tez.Stats;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;
import static org.apache.hadoop.hive.ql.anon.TestUtils.WH_DIR;
public class TestContext {
  private static final int KB = 1024;

  public String whDir = WH_DIR;
  private int id;
  public int totalMessages = -1;
  public int statsMessagesPerUser;
  public int numUniqueUsers;
  public int allToPiiRatio;
  public int piiMsgFieldLen;
  public int numFiles;
  public ColumnInternalFormat internalFormat;
  public FileType fileType;
  public String indexedColumnName = "b";
  public int[] pageSize = new int[]{128 * KB, 256 * KB, 512 * KB, 1024 * KB};
  public int bufferPoolSize = 200;
  public int numKeys;
  public boolean verify = true;

  private String fileNameTemplate = "000000_";
  private String[] fileNames;

  private String tableName;
  private String[] btIxName = new String[pageSize.length];
  private String[] btRunners = new String[pageSize.length];
  private String dirIxName;
  private String tabIxName;

  private Path tblPath;
  private Path[] tblFilePath;

  private Path[] btIxPath = new Path[pageSize.length];
  private Path dirIxPath;
  private Path tabIxPath;

  private Path[] outAnonNoIx;
  private Path[][] outAnonBt;
  private Path[] outAnonLkp;
  private Path[] outAnonTab;

  private final int numRepetitions = Integer.getInteger("perf.reps", 7);
  private final int numWarmup = Integer.getInteger("perf.warmup", 3);
  private List<WritableComparable> keys;
  private final List<Stats> lstStats = new ArrayList<>();

  public TestContext() {
  }

  public TestContext(final TestParams params) {
    this.totalMessages = params.totalMessages;
    this.allToPiiRatio = params.allToPiiRatio;
    this.numUniqueUsers = params.numUniqueUsers;
    this.piiMsgFieldLen = params.piiMsgFieldLen;
    this.internalFormat = params.internalFormat;
    this.numKeys = params.numKeys;
    this.numFiles = params.numFiles;
    this.fileType = params.fileType;

    setId(params.id);
  }

  public TestContext(final int totalMessages, final int allToPiiRatio, final int numUniqueUsers, final int fieldLen,
                     final ColumnInternalFormat internalFormat, final int numKeys, final int numFiles, final FileType fileType
  ) {
    this.totalMessages = totalMessages;
    this.allToPiiRatio = allToPiiRatio;
    this.numUniqueUsers = numUniqueUsers;
    this.piiMsgFieldLen = fieldLen;
    this.internalFormat = internalFormat;
    this.numKeys = numKeys;
    this.numFiles = numFiles;
    this.fileType = fileType;
  }

  public void setId(int id) {
    final String fix3 = String.format("%04d", id);
    this.tableName = "t_" + fix3;
    for (int i = 0; i < pageSize.length; i++) {
      this.btIxName[i] = "ix_bt_" + fix3 + "_" + i;
      this.btRunners[i] = "B" + (i + 1);
    }
    this.dirIxName = "ix_dir_" + fix3;
    this.tabIxName = "ix_tab_" + fix3;

    tblPath = new Path(whDir, getTableName());
    tblFilePath = new Path[numFiles];
    fileNames = new String[numFiles];
    outAnonNoIx = new Path[numFiles];
    outAnonLkp = new Path[numFiles];
    outAnonTab = new Path[numFiles];
    outAnonBt = new Path[pageSize.length][];
    for (int p = 0; p < pageSize.length; p++) {
      outAnonBt[p] = new Path[numFiles];
    }

    for (int fi = 0; fi < numFiles; fi++) {
      fileNames[fi] = fileNameTemplate + fi;
      tblFilePath[fi] = new Path(tblPath, fileNames[fi]);
      outAnonNoIx[fi] = new Path(whDir + "anon_out_no_ix_" + fix3, fileNames[fi]);
      outAnonLkp[fi] = new Path(whDir + "anon_out_lkp_" + fix3, fileNames[fi]);
      outAnonTab[fi] = new Path(whDir + "anon_out_tab_" + fix3, fileNames[fi]);

      for (int i = 0; i < pageSize.length; i++) {
        outAnonBt[i][fi] = new Path(whDir + "anon_out_bt_" + fix3 + "_" + i, fileNames[fi]);
      }

    }

    final TableName tn = new TableName(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, getTableName());
    for (int i = 0; i < pageSize.length; i++) {
      btIxPath[i] = new Path(whDir + AnonStatementAnalyzer.getIndexDirName(tn, getBtIxName()[i], indexedColumnName), fileNames[0]);
    }
    dirIxPath = new Path(whDir + AnonStatementAnalyzer.getIndexDirName(tn, getDirIxName(), indexedColumnName), fileNames[0]);
    tabIxPath = new Path(whDir + AnonStatementAnalyzer.getIndexDirName(tn, getTabIxName(), indexedColumnName), fileNames[0]);

    keys = getKeys(numKeys);

    this.id = id;
  }

  private List<WritableComparable> getKeys(final int count) {
    final List<WritableComparable> keys = new ArrayList<>();
    if (count == 1) {
      final int uid = numUniqueUsers / 2;
      keys.add(new IntWritable(uid));
    } else if (count == 10) {
      final int inc = numUniqueUsers / count;
      for (int i = 0; i < count; i++) {
        keys.add(new IntWritable(i * inc));
      }
    } else if (count == 100) {
      final int inc = numUniqueUsers / count;
      for (int i = 0; i < count; i++) {
        keys.add(new IntWritable(i * inc));
      }
    } else {
      throw new RuntimeException("Unexpected count " + count);
    }

    return keys;
  }

  public int getId() {
    return id;
  }

  public String getTableName() {
    return tableName;
  }

  public String[] getBtIxName() {
    return btIxName;
  }

  public String getDirIxName() {
    return dirIxName;
  }

  public String getTabIxName() {
    return tabIxName;
  }

  public Path getTblPath() {
    return tblPath;
  }

  public Path[] getTblFilePath() {
    return tblFilePath;
  }

  public Path[] getOutAnonNoIx() {
    return outAnonNoIx;
  }

  public Path[][] getOutAnonBt() {
    return outAnonBt;
  }

  public Path[] getOutAnonLkp() {
    return outAnonLkp;
  }

  public Path[] getOutAnonTab() {
    return outAnonTab;
  }

  public Path[] getBtIxPath() {
    return btIxPath;
  }

  public Path getLkpIxPath() {
    return dirIxPath;
  }

  public Path getTabIxPath() {
    return tabIxPath;
  }

  public String[] getBtRunners() {
    return btRunners;
  }

  public int getNumRepetitions() {
    return numRepetitions;
  }

  public int getNumWarmup() {
    return numWarmup;
  }

  public List<WritableComparable> getKeys() {
    return keys;
  }

  public List<Stats> getLstStats() {
    return lstStats;
  }

  public void addStats(final Stats stats) {
    lstStats.add(stats);
  }

  public void convertStats(List<String> lines) {
    for (final Stats stats : getLstStats()) {
      final int pii_msg = totalMessages / allToPiiRatio;
      final int msg_per_user = pii_msg / numUniqueUsers;
      final double bw = (totalMessages / 1000.0) / ((double) Math.max(1L, stats.totalProcessingTime));
      final String line = String.format("%d,%d,%d,%d,%d,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%f,%d,%s\n",
        id, numKeys, allToPiiRatio, numUniqueUsers, piiMsgFieldLen, stats.colFormat, stats.runnerName,
        stats.totalProcessingTime, stats.totalAnonTime, stats.indexTime, stats.seekTime, stats.runNumber, stats.error, totalMessages, pii_msg, msg_per_user, bw, numFiles,
        fileType.name()
      );
      lines.add(line);
    }
  }

  public void convertSummary(final List<String> lines) {
    final int pii_msg = totalMessages / allToPiiRatio;
    final int msg_per_user = pii_msg / numUniqueUsers;
    final Map<String, List<Stats>> byRunner = new LinkedHashMap<>();
    for (final Stats s : getLstStats()) {
      byRunner.computeIfAbsent(s.runnerName + "|" + s.colFormat, k -> new ArrayList<>()).add(s);
    }
    for (final Map.Entry<String, List<Stats>> e : byRunner.entrySet()) {
      final List<Stats> g = e.getValue();
      final long[] proc = g.stream().mapToLong(s -> s.totalProcessingTime).sorted().toArray();
      final long[] anon = g.stream().mapToLong(s -> s.totalAnonTime).sorted().toArray();
      final long[] idx = g.stream().mapToLong(s -> s.indexTime).sorted().toArray();
      final int errs = g.stream().mapToInt(s -> s.error).sum();
      final Stats first = g.get(0);
      lines.add(String.format("%d,%d,%d,%d,%d,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s\n",
        id, numKeys, allToPiiRatio, numUniqueUsers, piiMsgFieldLen, first.colFormat, first.runnerName,
        g.size(), median(proc), proc.length == 0 ? 0 : proc[0], proc.length == 0 ? 0 : proc[proc.length - 1],
        iqr(proc), median(anon), median(idx), errs, totalMessages, pii_msg, msg_per_user, numFiles, fileType.name()));
    }
  }

  private static long median(final long[] s) {
    final int n = s.length;
    if (n == 0) {
      return 0;
    }
    return (n % 2 == 1) ? s[n / 2] : (s[n / 2 - 1] + s[n / 2]) / 2;
  }

  private static long iqr(final long[] s) {
    if (s.length < 4) {
      return s.length == 0 ? 0 : s[s.length - 1] - s[0];
    }
    return s[(3 * s.length) / 4] - s[s.length / 4];
  }

  @Override
  public String toString() {
    return String.format("id: %d, tm: %,d, fl: %,d, r: %,d, uu: %,d, nk: %,d, nf: %d, fmt: %s, ft: %s, nr: %d",
      id, totalMessages, piiMsgFieldLen, allToPiiRatio, numUniqueUsers, numKeys, numFiles, internalFormat, fileType, numRepetitions);
  }
}
