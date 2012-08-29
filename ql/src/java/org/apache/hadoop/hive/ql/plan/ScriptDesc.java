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

import org.apache.hadoop.hive.ql.exec.RecordReader;
import org.apache.hadoop.hive.ql.exec.RecordWriter;

/**
 * ScriptDesc.
 *
 */
@Explain(displayName = "Transform Operator")
public class ScriptDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;
  private String scriptCmd;
  // Describe how to deserialize data back from user script
  private TableDesc scriptOutputInfo;
  private Class<? extends RecordWriter> inRecordWriterClass;

  // Describe how to serialize data out to user script
  private TableDesc scriptInputInfo;
  private Class<? extends RecordReader> outRecordReaderClass;

  private TableDesc scriptErrInfo;
  private Class<? extends RecordReader> errRecordReaderClass;

  public ScriptDesc() {
  }

  public ScriptDesc(final String scriptCmd, final TableDesc scriptInputInfo,
      final Class<? extends RecordWriter> inRecordWriterClass,
      final TableDesc scriptOutputInfo,
      final Class<? extends RecordReader> outRecordReaderClass,
      final Class<? extends RecordReader> errRecordReaderClass,
      final TableDesc scriptErrInfo) {

    this.scriptCmd = scriptCmd;
    this.scriptInputInfo = scriptInputInfo;
    this.inRecordWriterClass = inRecordWriterClass;
    this.scriptOutputInfo = scriptOutputInfo;
    this.outRecordReaderClass = outRecordReaderClass;
    this.errRecordReaderClass = errRecordReaderClass;
    this.scriptErrInfo = scriptErrInfo;
  }

  @Explain(displayName = "command")
  public String getScriptCmd() {
    return scriptCmd;
  }

  public void setScriptCmd(final String scriptCmd) {
    this.scriptCmd = scriptCmd;
  }

  @Explain(displayName = "output info")
  public TableDesc getScriptOutputInfo() {
    return scriptOutputInfo;
  }

  public void setScriptOutputInfo(final TableDesc scriptOutputInfo) {
    this.scriptOutputInfo = scriptOutputInfo;
  }

  public TableDesc getScriptErrInfo() {
    return scriptErrInfo;
  }

  public void setScriptErrInfo(final TableDesc scriptErrInfo) {
    this.scriptErrInfo = scriptErrInfo;
  }

  public TableDesc getScriptInputInfo() {
    return scriptInputInfo;
  }

  public void setScriptInputInfo(TableDesc scriptInputInfo) {
    this.scriptInputInfo = scriptInputInfo;
  }

  /**
   * @return the outRecordReaderClass
   */
  public Class<? extends RecordReader> getOutRecordReaderClass() {
    return outRecordReaderClass;
  }

  /**
   * @param outRecordReaderClass
   *          the outRecordReaderClass to set
   */
  public void setOutRecordReaderClass(
      Class<? extends RecordReader> outRecordReaderClass) {
    this.outRecordReaderClass = outRecordReaderClass;
  }

  /**
   * @return the errRecordReaderClass
   */
  public Class<? extends RecordReader> getErrRecordReaderClass() {
    return errRecordReaderClass;
  }

  /**
   * @param errRecordReaderClass
   *          the errRecordReaderClass to set
   */
  public void setErrRecordReaderClass(
      Class<? extends RecordReader> errRecordReaderClass) {
    this.errRecordReaderClass = errRecordReaderClass;
  }

  /**
   * @return the inRecordWriterClass
   */
  public Class<? extends RecordWriter> getInRecordWriterClass() {
    return inRecordWriterClass;
  }

  /**
   * @param inRecordWriterClass
   *          the inRecordWriterClass to set
   */
  public void setInRecordWriterClass(
      Class<? extends RecordWriter> inRecordWriterClass) {
    this.inRecordWriterClass = inRecordWriterClass;
  }

}
