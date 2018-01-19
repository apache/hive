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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

public class DumpMetaData {
  // wrapper class for reading and writing metadata about a dump
  // responsible for _dumpmetadata files
  private static final String DUMP_METADATA = "_dumpmetadata";

  private DumpType dumpType;
  private Long eventFrom = null;
  private Long eventTo = null;
  private String payload = null;
  private boolean initialized = false;

  private final Path dumpFile;
  private final HiveConf hiveConf;
  private Path cmRoot;

  public DumpMetaData(Path dumpRoot, HiveConf hiveConf) {
    this.hiveConf = hiveConf;
    dumpFile = new Path(dumpRoot, DUMP_METADATA);
  }

  public DumpMetaData(Path dumpRoot, DumpType lvl, Long eventFrom, Long eventTo, Path cmRoot,
      HiveConf hiveConf) {
    this(dumpRoot, hiveConf);
    setDump(lvl, eventFrom, eventTo, cmRoot);
  }

  public void setDump(DumpType lvl, Long eventFrom, Long eventTo, Path cmRoot) {
    this.dumpType = lvl;
    this.eventFrom = eventFrom;
    this.eventTo = eventTo;
    this.initialized = true;
    this.cmRoot = cmRoot;
  }

  private void loadDumpFromFile() throws SemanticException {
    BufferedReader br = null;
    try {
      // read from dumpfile and instantiate self
      FileSystem fs = dumpFile.getFileSystem(hiveConf);
      br = new BufferedReader(new InputStreamReader(fs.open(dumpFile)));
      String line = null;
      if ((line = br.readLine()) != null) {
        String[] lineContents = line.split("\t", 5);
        setDump(DumpType.valueOf(lineContents[0]), Long.valueOf(lineContents[1]),
            Long.valueOf(lineContents[2]),
            new Path(lineContents[3]));
        setPayload(lineContents[4].equals(Utilities.nullStringOutput) ? null : lineContents[4]);
        ReplChangeManager.setCmRoot(cmRoot);
      } else {
        throw new IOException(
            "Unable to read valid values from dumpFile:" + dumpFile.toUri().toString());
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

  public void setPayload(String payload) {
    this.payload = payload;
  }

  public Long getEventFrom() throws SemanticException {
    initializeIfNot();
    return eventFrom;
  }

  public Long getEventTo() throws SemanticException {
    initializeIfNot();
    return eventTo;
  }

  public Path getDumpFilePath() {
    return dumpFile;
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


  public void write() throws SemanticException {
    Utils.writeOutput(
        Arrays.asList(
            dumpType.toString(),
            eventFrom.toString(),
            eventTo.toString(),
            cmRoot.toString(),
            payload),
        dumpFile,
        hiveConf
    );
  }
}
