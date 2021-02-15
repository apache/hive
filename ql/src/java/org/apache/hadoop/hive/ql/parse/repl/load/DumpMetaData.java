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
package org.apache.hadoop.hive.ql.parse.repl.load;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.repl.ReplScope;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DumpMetaData {
  // wrapper class for reading and writing metadata about a dump
  // responsible for _dumpmetadata files
  private static final String DUMP_METADATA = "_dumpmetadata";
  private static final Logger LOG = LoggerFactory.getLogger(DumpMetaData.class);

  private DumpType dumpType;
  private Long eventFrom = null;
  private Long eventTo = null;
  private Path cmRoot;
  private String payload = null;
  private ReplScope replScope = null;

  private boolean initialized = false;
  private final Path dumpFile;
  private final HiveConf hiveConf;
  private Long dumpExecutionId;
  private boolean replScopeModified = false;
  private int dumpFormatVersion;

  public DumpMetaData(Path dumpRoot, HiveConf hiveConf) {
    this.hiveConf = hiveConf;
    dumpFile = new Path(dumpRoot, DUMP_METADATA);
  }

  public DumpMetaData(Path dumpRoot, DumpType lvl, Long eventFrom, Long eventTo, Path cmRoot,
      HiveConf hiveConf) {
    this(dumpRoot, hiveConf);
    setDump(lvl, eventFrom, eventTo, cmRoot, 0L, false);
  }

  public void setDump(DumpType lvl, Long eventFrom, Long eventTo, Path cmRoot, Long dumpExecutionId,
                      boolean replScopeModified) {
    this.dumpType = lvl;
    this.eventFrom = eventFrom;
    this.eventTo = eventTo;
    this.cmRoot = cmRoot;
    this.initialized = true;
    this.dumpExecutionId = dumpExecutionId;
    this.replScopeModified = replScopeModified;
  }

  public void setPayload(String payload) {
    this.payload = payload;
  }

  public void setReplScope(ReplScope replScope) {
    this.replScope = replScope;
  }

  public void setDumpType(DumpType dumpType) {
    this.dumpType = dumpType;
  }

  private void readReplScope(String line) throws IOException {
    if (line == null) {
      return;
    }

    String[] lineContents = line.split("\t");
    replScope = new ReplScope();
    for (int idx = 0; idx < lineContents.length; idx++) {
      String value = lineContents[idx];
      switch (idx) {
      case 0:
        LOG.info("Read ReplScope: Set Db Name: {}.", value);
        replScope.setDbName(value);
        break;
      case 1:
        LOG.info("Read ReplScope: Include table name list: {}.", value);
        replScope.setIncludedTablePatterns(value);
        break;
      case 2:
        LOG.info("Read ReplScope: Exclude table name list: {}.", value);
        replScope.setExcludedTablePatterns(value);
        break;
      default:
        throw new IOException("Invalid repl tables list data in dump metadata file");
      }
    }
  }

  private void loadDumpFromFile() throws SemanticException {
    BufferedReader br = null;
    try {
      // read from dumpfile and instantiate self
      FileSystem fs = dumpFile.getFileSystem(hiveConf);
      br = new BufferedReader(new InputStreamReader(fs.open(dumpFile)));
      String line;
      if ((line = br.readLine()) != null) {
        String[] lineContents = line.split("\t", 8);
        setDump(lineContents[0].equals(Utilities.nullStringOutput) ? null : DumpType.valueOf(lineContents[0]),
          lineContents[1].equals(Utilities.nullStringOutput) ? null : Long.valueOf(lineContents[1]),
          lineContents[2].equals(Utilities.nullStringOutput) ? null : Long.valueOf(lineContents[2]),
          lineContents[3].equals(Utilities.nullStringOutput) ? null : new Path(lineContents[3]),
          lineContents[4].equals(Utilities.nullStringOutput) ? null : Long.valueOf(lineContents[4]),
          Boolean.valueOf(lineContents[6]));
        setPayload(lineContents[5].equals(Utilities.nullStringOutput) ? null : lineContents[5]);
        setDumpFormatVersion((lineContents.length < 8
            || lineContents[7].equals(Utilities.nullStringOutput)) ? -1 : Integer.parseInt(lineContents[7]));
      } else {
        throw new IOException(
            "Unable to read valid values from dumpFile:" + dumpFile.toUri().toString());
      }
      readReplScope(br.readLine());
    } catch (IOException ioe) {
      throw new SemanticException(ioe);
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          throw new SemanticException(e);
        }
      }
    }
  }

  public DumpType getDumpType() throws SemanticException {
    initializeIfNot();
    return this.dumpType;
  }

  public String getPayload() throws SemanticException {
    initializeIfNot();
    return this.payload;
  }

  public Long getEventFrom() throws SemanticException {
    initializeIfNot();
    return eventFrom;
  }

  public Long getEventTo() throws SemanticException {
    initializeIfNot();
    return eventTo;
  }

  public Long getDumpExecutionId() throws SemanticException {
    initializeIfNot();
    return dumpExecutionId;
  }

  public boolean isReplScopeModified() throws SemanticException {
    initializeIfNot();
    return replScopeModified;
  }

  public ReplScope getReplScope() throws SemanticException {
    initializeIfNot();
    return replScope;
  }
  public Path getDumpFilePath() {
    return dumpFile;
  }

  public static String getDmdFileName() {
    return DUMP_METADATA;
  }

  public boolean isIncrementalDump() throws SemanticException {
    initializeIfNot();
    return (this.dumpType == DumpType.INCREMENTAL);
  }

  private void initializeIfNot() throws SemanticException {
    if (!initialized) {
      loadDumpFromFile();
    }
  }

  private List<String> prepareReplScopeValues() {
    assert(replScope != null);

    List<String> values = new ArrayList<>();
    values.add(replScope.getDbName());

    String includedTableNames = replScope.getIncludedTableNames();
    String excludedTableNames = replScope.getExcludedTableNames();
    if (includedTableNames != null) {
      values.add(includedTableNames);
    }
    if (excludedTableNames != null) {
      values.add(excludedTableNames);
    }
    LOG.info("Preparing ReplScope {} to dump.", values);
    return values;
  }

  public void write() throws SemanticException {
    write(false);
  }

  public void write(boolean replace) throws SemanticException {
    List<List<String>> listValues = new ArrayList<>();
    listValues.add(
        Arrays.asList(
            dumpType != null ? dumpType.toString() : null,
            eventFrom != null ? eventFrom.toString() : null,
            eventTo != null ? eventTo.toString() : null,
            cmRoot != null ? cmRoot.toString() : null,
            dumpExecutionId != null ? dumpExecutionId.toString() : null,
            payload,
            String.valueOf(replScopeModified),
            String.valueOf(dumpFormatVersion))
    );
    if (replScope != null) {
      listValues.add(prepareReplScopeValues());
    }
    Utils.writeOutput(listValues, dumpFile, hiveConf, replace);
  }

  public int getDumpFormatVersion() {
    return this.dumpFormatVersion;
  }

  public void setDumpFormatVersion(int dumpFormatVersion) {
    this.dumpFormatVersion = dumpFormatVersion;
  }

  public boolean isVersionCompatible() throws SemanticException {
    initializeIfNot();
    return this.dumpFormatVersion >= Utilities.MIN_VERSION_FOR_NEW_DUMP_FORMAT;
  }
}
