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

package org.apache.hadoop.hive.ql.plan;

import java.util.Objects;

import org.apache.hadoop.hive.ql.exec.RecordReader;
import org.apache.hadoop.hive.ql.exec.RecordWriter;
import org.apache.hadoop.hive.ql.optimizer.signature.Signature;
import org.apache.hadoop.hive.ql.plan.Explain.Level;


/**
 * ScriptDesc.
 *
 */
@Explain(displayName = "Transform Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
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

  @Signature
  @Explain(displayName = "command", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getScriptCmd() {
    return scriptCmd;
  }

  public void setScriptCmd(final String scriptCmd) {
    this.scriptCmd = scriptCmd;
  }

  @Signature
  @Explain(displayName = "output info", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
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

  @Override
  public boolean isSame(OperatorDesc other) {
    if (getClass().getName().equals(other.getClass().getName())) {
      ScriptDesc otherDesc = (ScriptDesc) other;
      return Objects.equals(getScriptCmd(), otherDesc.getScriptCmd()) &&
          Objects.equals(getScriptOutputInfo(), otherDesc.getScriptOutputInfo());
    }
    return false;
  }

}
