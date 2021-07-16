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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FailoverMetaData {
    public static final String FAILOVER_METADATA = "_failovermetadata";
    private static final Logger LOG = LoggerFactory.getLogger(FailoverMetaData.class);

    private static ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();

    @JsonProperty
    private Long failoverEventId = null;
    @JsonProperty
    private Long cursorPoint = null;
    @JsonProperty
    private List<Long> abortedTxns;
    @JsonProperty
    private List<Long> openTxns;
    @JsonProperty
    private List<Long> txnsWithoutLock;

    @JsonIgnore
    private volatile boolean initialized = false;
    @JsonIgnore
    private final Path metadataFile;
    @JsonIgnore
    private final HiveConf hiveConf;

    public FailoverMetaData() {
        metadataFile = null;
        hiveConf = null;
    }

    public FailoverMetaData(Path dumpDir, HiveConf hiveConf) {
        this.hiveConf = hiveConf;
        this.metadataFile = new Path(dumpDir, FAILOVER_METADATA);
    }

    private void initializeIfNot() throws SemanticException {
        if (!initialized) {
            loadMetadataFromFile();
            initialized = true;
        }
    }

    public void setMetaData(FailoverMetaData otherDMD) {
        this.failoverEventId = otherDMD.failoverEventId;
        this.abortedTxns = otherDMD.abortedTxns;
        this.openTxns = otherDMD.openTxns;
        this.cursorPoint = otherDMD.cursorPoint;
        this.txnsWithoutLock = otherDMD.txnsWithoutLock;
        this.initialized = true;
    }

    private synchronized void loadMetadataFromFile() throws SemanticException {
        if (!initialized) {
            LOG.info("Reading failover metadata from file: ", metadataFile);
            BufferedReader br = null;
            try {
                FileSystem fs = metadataFile.getFileSystem(hiveConf);
                br = new BufferedReader(new InputStreamReader(fs.open(metadataFile)));
                String line;
                if ((line = br.readLine()) != null) {
                    FailoverMetaData otherDMD = JSON_OBJECT_MAPPER.readValue(line, FailoverMetaData.class);
                    setMetaData(otherDMD);
                } else {
                    throw new IOException("Unable to read valid values from failover Metadata file:"
                            + metadataFile.toUri().toString());
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
    }

    public void setFailoverEventId(Long failoverEventId) {
        this.failoverEventId = failoverEventId;
        initialized = true;
    }

    public void setAbortedTxns(List<Long> abortedTxns) {
        this.abortedTxns = abortedTxns;
        initialized = true;
    }

    public void addToAbortedTxns(List<Long> abortedTxns) {
        if (this.abortedTxns == null) {
            this.abortedTxns = abortedTxns;
        } else {
            this.abortedTxns.addAll(abortedTxns);
        }
    }

    public void setCursorPoint(Long cursorPoint) {
        this.cursorPoint = cursorPoint;
        initialized = true;
    }

    public void setOpenTxns(List<Long> openTxns) {
        this.openTxns = openTxns;
        initialized = true;
    }

    public void setTxnsWithoutLock(List<Long> txnsWithoutLock) {
        this.txnsWithoutLock = txnsWithoutLock;
        initialized = true;
    }

    public Long getCursorPoint() throws SemanticException {
        initializeIfNot();
        return cursorPoint;
    }

    public List<Long> getOpenTxns() throws SemanticException {
        initializeIfNot();
        return openTxns;
    }

    public Long getFailoverEventId() throws SemanticException {
        initializeIfNot();
        return failoverEventId;
    }

    public List<Long> getAbortedTxns() throws SemanticException {
        initializeIfNot();
        return abortedTxns;
    }

    public List<Long> getTxnsWithoutLock() throws SemanticException {
        initializeIfNot();
        return txnsWithoutLock;
    }

    @JsonIgnore
    public boolean isValidMetadata() throws SemanticException {
        initializeIfNot();
        return openTxns != null && abortedTxns != null && failoverEventId != null
                && cursorPoint != null && txnsWithoutLock != null;
    }

    public boolean equals(FailoverMetaData otherFmd) throws SemanticException {
        this.initializeIfNot();
        return this.failoverEventId.equals(otherFmd.getFailoverEventId()) &&
                this.openTxns.equals(otherFmd.getOpenTxns()) &&
                this.abortedTxns.equals(otherFmd.getAbortedTxns()) &&
                this.txnsWithoutLock.equals(otherFmd.getTxnsWithoutLock()) &&
                this.cursorPoint.equals(otherFmd.getCursorPoint());
    }

    @JsonIgnore
    public String getFilePath() {
        return (metadataFile == null) ? null : metadataFile.toString();
    }

    public void write() throws SemanticException {
        try {
            String failoverContentAsJSON = JSON_OBJECT_MAPPER.writeValueAsString(this);
            Utils.writeOutput(failoverContentAsJSON, metadataFile, hiveConf);
        } catch (JsonProcessingException e) {
            throw new SemanticException(e);
        }
    }
}
