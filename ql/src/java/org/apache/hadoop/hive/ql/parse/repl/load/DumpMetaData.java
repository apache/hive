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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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

@JsonIgnoreProperties(ignoreUnknown = true)
public class DumpMetaData {
  // wrapper class for reading and writing metadata about a dump
  // responsible for _dumpmetadata files
  public static final String DUMP_METADATA = "_dumpmetadata";

  // New version of dump metadata file to store top level dumpmetadata content in JSON format
  public static final String DUMP_METADATA_V2 = "_dumpmetadata_v2";
  private static final Logger LOG = LoggerFactory.getLogger(DumpMetaData.class);
  private static ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper(); // Thread-safe.

  @JsonProperty
  private DumpType dumpType;
  @JsonProperty
  private Long eventFrom = null;
  @JsonProperty
  private Long eventTo = null;
  @JsonProperty
  private Path cmRoot;
  @JsonProperty
  private String payload = null;
  @JsonProperty
  private Long dumpExecutionId;
  @JsonProperty
  private boolean replScopeModified = false;
  @JsonProperty
  private String replScopeStr = null;
  //Ignore rest of the properties
  @JsonIgnore
  private ReplScope replScope = null;
  @JsonIgnore
  private Path dumpFile;
  @JsonIgnore
  private final HiveConf hiveConf;
  @JsonIgnore
  private boolean isTopLevel;
  @JsonIgnore
  private Path dumpRoot;
  @JsonIgnore
  private boolean initialized = false;

  public DumpMetaData() {
    //to be instantiated by JSON ObjectMapper.
    hiveConf = null;
  }

  public DumpMetaData(Path dumpRoot, HiveConf hiveConf) {
    this(dumpRoot, hiveConf, false);
  }

  public DumpMetaData(Path dumpRoot, HiveConf hiveConf, boolean isTopLevel) {
    this.dumpRoot = dumpRoot;
    this.hiveConf = hiveConf;
    this.isTopLevel = isTopLevel;
  }
  public DumpMetaData(Path dumpRoot, DumpType lvl, Long eventFrom, Long eventTo, Path cmRoot,
      HiveConf hiveConf) {
    this(dumpRoot, hiveConf, true);
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
    boolean isInJSONFormat = resolveDumpFilePathAndGetIfV2();
    if (isInJSONFormat) {
      loadDumpFromFileV2();
    } else {
      loadDumpFromFileV1();
    }
  }

  //Returns true if dumpmetaData is in V2 Format
  private boolean resolveDumpFilePathAndGetIfV2() throws SemanticException {
    if (isTopLevel) {
      dumpFile = new Path(dumpRoot, DUMP_METADATA_V2);
      if (Utils.fileExists(dumpFile, hiveConf)) {
        return true;
      }
      //Backward-compatibility: fall back to old version. Dump might be generated by old version
      dumpFile = new Path(dumpRoot, DUMP_METADATA);
      LOG.info("Falling back to old version of dump meta data {}", dumpFile);
    } else {
      // The nested level _dumpmetadata file content is still in old format: To save JSON parsing cost.
      dumpFile = new Path(dumpRoot, DUMP_METADATA);
    }
    return false;
  }

  private void loadDumpFromFileV1() throws SemanticException {
    BufferedReader br = null;
    try {
      // read from dumpfile and instantiate self
      FileSystem fs = dumpFile.getFileSystem(hiveConf);
      br = new BufferedReader(new InputStreamReader(fs.open(dumpFile)));
      String line;
      if ((line = br.readLine()) != null) {
        String[] lineContents = line.split("\t", 7);
        setDump(lineContents[0].equals(Utilities.nullStringOutput) ? null : DumpType.valueOf(lineContents[0]),
          lineContents[1].equals(Utilities.nullStringOutput) ? null : Long.valueOf(lineContents[1]),
          lineContents[2].equals(Utilities.nullStringOutput) ? null :  Long.valueOf(lineContents[2]),
          lineContents[3].equals(Utilities.nullStringOutput) ? null : new Path(lineContents[3]),
          lineContents[4].equals(Utilities.nullStringOutput) ? null : Long.valueOf(lineContents[4]),
          (lineContents.length < 7 || lineContents[6].equals(Utilities.nullStringOutput)) ?
                        Boolean.valueOf(false) : Boolean.valueOf(lineContents[6]));
        setPayload(lineContents[5].equals(Utilities.nullStringOutput) ? null : lineContents[5]);
      } else {
        throw new IOException("Unable to read valid values from dumpFile:" + dumpFile.toUri().toString());
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

  private void loadDumpFromFileV2() throws SemanticException {
    BufferedReader br = null;
    try {
      // read from dumpfile and instantiate self
      FileSystem fs = dumpFile.getFileSystem(hiveConf);
      br = new BufferedReader(new InputStreamReader(fs.open(dumpFile)));
      String line;
      if ((line = br.readLine()) != null) {
        DumpMetaData otherDMD = JSON_OBJECT_MAPPER.readValue(line, DumpMetaData.class);
        otherDMD.initialized = true;
        setDump(otherDMD.dumpType,
                otherDMD.eventFrom,
                otherDMD.eventTo,
                otherDMD.cmRoot,
                otherDMD.dumpExecutionId,
                otherDMD.replScopeModified);
        setPayload(otherDMD.getPayload());
        readReplScope(otherDMD.replScopeStr);
      } else {
        throw new IOException("Unable to read valid values from dumpFile:" + dumpFile.toUri().toString());
      }

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

  public String getCmRoot() throws SemanticException {
    initializeIfNot();
    return cmRoot == null ? null : cmRoot.toString();
  }

  public Long getDumpExecutionId() throws SemanticException {
    initializeIfNot();
    return dumpExecutionId;
  }

  public boolean isReplScopeModified() throws SemanticException {
    initializeIfNot();
    return replScopeModified;
  }

  public String getReplScopeStr() throws SemanticException {
    initializeIfNot();
    return replScopeStr;
  }

  @JsonIgnore
  public ReplScope getReplScope() throws SemanticException {
    initializeIfNot();
    return replScope;
  }

  @JsonIgnore
  public boolean isIncrementalDump() throws SemanticException {
    initializeIfNot();
    return (this.dumpType == DumpType.INCREMENTAL);
  }

  private void initializeIfNot() throws SemanticException {
    if (!initialized) {
      loadDumpFromFile();
    }
  }

  public List<String> prepareReplScopeValues() {
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
    if (isTopLevel) {
      // JSON based format
      writeV2();
    } else {
      // Nested level files still have old format - Tab separated String.
      writeV1(replace);
    }
  }

  private void writeV1(boolean replace) throws SemanticException {
    dumpFile = new Path(dumpRoot, DUMP_METADATA);
    List<List<String>> listValues = new ArrayList<>();
    listValues.add(
        Arrays.asList(
            dumpType != null ? dumpType.toString() : null,
            eventFrom != null ? eventFrom.toString() : null,
            eventTo != null ? eventTo.toString() : null,
            cmRoot != null ? cmRoot.toString() : null,
            dumpExecutionId != null ? dumpExecutionId.toString() : null,
            payload,
            String.valueOf(replScopeModified))
    );
    if (replScope != null) {
      listValues.add(prepareReplScopeValues());
    }
    Utils.writeOutput(listValues, dumpFile, hiveConf, replace);
  }

  private void writeV2() throws SemanticException {
    dumpFile = new Path(dumpRoot, DUMP_METADATA_V2) ;
    if (this.replScope != null) {
      StringBuilder replScopeStrBuildr = new StringBuilder();
      List<String> replScopeVals =  prepareReplScopeValues();
      replScopeStrBuildr.append((replScopeVals.get(0) == null ? Utilities.nullStringOutput : replScopeVals.get(0)));
      for (int i = 1; i < replScopeVals.size(); i++) {
        replScopeStrBuildr.append((char)Utilities.tabCode);
        replScopeStrBuildr.append((replScopeVals.get(i) == null ? Utilities.nullStringOutput : replScopeVals.get(i)));
      }
      this.replScopeStr = replScopeStrBuildr.toString();
    }
    String dmdContentAsJSON = null;
    try {
      dmdContentAsJSON = JSON_OBJECT_MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      new SemanticException(e);
    }
    Utils.writeOutput(dmdContentAsJSON, dumpFile, hiveConf);
  }
}
