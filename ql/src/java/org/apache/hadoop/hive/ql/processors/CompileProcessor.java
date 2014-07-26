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

package org.apache.hadoop.hive.ql.processors;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.compress.archivers.jar.JarArchiveEntry;
import org.apache.commons.compress.archivers.jar.JarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.VariableSubstitution;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.session.SessionState.ResourceType;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.types.Path;
import org.codehaus.groovy.ant.Groovyc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Files;

/**
 * Processor allows users to build code inside a hive session, then
 * use this code as a UDF, Serde, or even a more complex entity like an
 * input format or hook.
 *
 * Note: This class is stateful and not thread safe. Create a new instance for
 * each invocation of CompileProcessor.
 *
 */
public class CompileProcessor implements CommandProcessor {

  public static final Log LOG = LogFactory.getLog(CompileProcessor.class.getName());
  public static final LogHelper console = new LogHelper(LOG);
  public static final String IO_TMP_DIR = "java.io.tmpdir";
  public static final String GROOVY = "GROOVY";
  public static final String AS = "AS";
  public static final String NAMED = "NAMED";
  private static final String SYNTAX = "syntax: COMPILE ` some code here ` AS groovy NAMED something.groovy";
  private static final AtomicInteger runCount;

  /**
   * The language of the compiled code. Used to select the appropriate compiler.
   */
  private String lang;
  /**
   * The code to be compiled
   */
  private String code;
  /**
   * The name of the file the code will be written to
   */
  private String named;
  /**
   * The entire command sent to the processor
   */
  private String command;
  /**
   * Used as part of a file name to help avoid collisions.
   */
  private int myId;

  static {
    runCount = new AtomicInteger(0);
  }

  @Override
  public void init() {
    //no init needed
  }

  /**
   * User supplies dynamic code in this format:
   * COMPILE ` some code here ` AS groovy NAMED something.groovy;
   * CompileProcessor will compile and package this code into a jar. The jar
   * will be added to the session state via the session state's
   * ADD RESOURCE command.
   * @param command a String to be compiled
   * @return CommandProcessorResponse with 0 for success and 1 for failure
   */
  @Override
  public CommandProcessorResponse run(String command) throws CommandNeedRetryException {
    SessionState ss = SessionState.get();
    this.command = command;

    CommandProcessorResponse authErrResp =
        CommandUtil.authorizeCommand(ss, HiveOperationType.COMPILE, Arrays.asList(command));
    if(authErrResp != null){
      // there was an authorization issue
      return authErrResp;
    }

    myId = runCount.getAndIncrement();

    try {
      parse(ss);
    } catch (CompileProcessorException e) {
      return CommandProcessorResponse.create(e);
    }
    CommandProcessorResponse result = null;
    try {
      result = compile(ss);
    } catch (CompileProcessorException e) {
      return CommandProcessorResponse.create(e);
    }
    return result;
  }

  /**
   * Parses the supplied command
   * @param ss
   * @throws CompileProcessorException if the code can not be compiled or the jar can not be made
   */
  @VisibleForTesting
  void parse(SessionState ss) throws CompileProcessorException {
    if (ss != null){
      command = new VariableSubstitution().substitute(ss.getConf(), command);
    }
    if (command == null || command.length() == 0) {
      throw new CompileProcessorException("Command was empty");
    }
    StringBuilder toCompile = new StringBuilder();
    int startPosition = 0;
    int endPosition = -1;
    /* TODO Escape handling may be changed by a follow on.
     * The largest issue is ; which are treated as statement
     * terminators for the cli. Once the cli is fixed this
     * code should be re-investigated
     */
    while (command.charAt(startPosition++) != '`' && startPosition < command.length()){

    }
    if (startPosition == command.length()){
      throw new CompileProcessorException(SYNTAX);
    }
    for (int i = startPosition; i < command.length(); i++) {
      if (command.charAt(i) == '\\') {
        toCompile.append(command.charAt(i + 1));
        i = i + 1;
        continue;
      } else if (command.charAt(i) == '`'){
        endPosition = i;
        break;
      } else {
        toCompile.append(command.charAt(i));
      }
    }
    if (endPosition == -1){
      throw new CompileProcessorException(SYNTAX);
    }
    StringTokenizer st = new StringTokenizer(command.substring(endPosition+1), " ");
    if (st.countTokens() != 4){
      throw new CompileProcessorException(SYNTAX);
    }
    String shouldBeAs = st.nextToken();
    if (!shouldBeAs.equalsIgnoreCase(AS)){
      throw new CompileProcessorException(SYNTAX);
    }
    setLang(st.nextToken());
    if (!lang.equalsIgnoreCase(GROOVY)){
      throw new CompileProcessorException("Can not compile " + lang + ". Hive can only compile " + GROOVY);
    }
    String shouldBeNamed = st.nextToken();
    if (!shouldBeNamed.equalsIgnoreCase(NAMED)){
      throw new CompileProcessorException(SYNTAX);
    }
    setNamed(st.nextToken());
    setCode(toCompile.toString());
  }

  @VisibleForTesting
  /**
   * Method converts statement into a file, compiles the file and then packages the file.
   * @param ss
   * @return Response code of 0 for success 1 for failure
   * @throws CompileProcessorException
   */
  CommandProcessorResponse compile(SessionState ss) throws CompileProcessorException {
    Project proj = new Project();
    String ioTempDir = System.getProperty(IO_TMP_DIR);
    File ioTempFile = new File(ioTempDir);
    if (!ioTempFile.exists()){
      throw new CompileProcessorException(ioTempDir + " does not exists");
    }
    if (!ioTempFile.isDirectory() || !ioTempFile.canWrite()){
      throw new CompileProcessorException(ioTempDir + " is not a writable directory");
    }
    Groovyc g = new Groovyc();
    long runStamp = System.currentTimeMillis();
    String jarId = myId + "_" + runStamp;
    g.setProject(proj);
    Path sourcePath = new Path(proj);
    File destination = new File(ioTempFile, jarId + "out");
    g.setDestdir(destination);
    File input = new File(ioTempFile, jarId + "in");
    sourcePath.setLocation(input);
    g.setSrcdir(sourcePath);
    input.mkdir();

    File fileToWrite = new File(input, this.named);
    try {
      Files.write(this.code, fileToWrite, Charset.forName("UTF-8"));
    } catch (IOException e1) {
      throw new CompileProcessorException("writing file", e1);
    }
    destination.mkdir();
    try {
      g.execute();
    } catch (BuildException ex){
      throw new CompileProcessorException("Problem compiling", ex);
    }
    File testArchive = new File(ioTempFile, jarId + ".jar");
    JarArchiveOutputStream out = null;
    try {
      out = new JarArchiveOutputStream(new FileOutputStream(testArchive));
      for (File f: destination.listFiles()){
        JarArchiveEntry jentry = new JarArchiveEntry(f.getName());
        FileInputStream fis = new FileInputStream(f);
        out.putArchiveEntry(jentry);
        IOUtils.copy(fis, out);
        fis.close();
        out.closeArchiveEntry();
      }
      out.finish();
    } catch (IOException e) {
      throw new CompileProcessorException("Exception while writing jar", e);
    } finally {
      if (out!=null){
        try {
          out.close();
        } catch (IOException WhatCanYouDo) {
        }
      }
    }

    if (ss != null){
      ss.add_resource(ResourceType.JAR, testArchive.getAbsolutePath());
    }
    CommandProcessorResponse good = new CommandProcessorResponse(0, testArchive.getAbsolutePath(), null);
    return good;
  }

  public String getLang() {
    return lang;
  }

  public void setLang(String lang) {
    this.lang = lang;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public String getNamed() {
    return named;
  }

  public void setNamed(String named) {
    this.named = named;
  }

  public String getCommand() {
    return command;
  }

  class CompileProcessorException extends HiveException {

    private static final long serialVersionUID = 1L;

    CompileProcessorException(String s, Throwable t) {
      super(s, t);
    }

    CompileProcessorException(String s) {
      super(s);
    }
  }
}
