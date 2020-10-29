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

package org.apache.hadoop.hive.ql.processors;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.compress.archivers.jar.JarArchiveEntry;
import org.apache.commons.compress.archivers.jar.JarArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.conf.HiveVariableSource;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.session.SessionState.ResourceType;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.types.Path;
import org.codehaus.groovy.ant.Groovyc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

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

  public static final Logger LOG = LoggerFactory.getLogger(CompileProcessor.class.getName());
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

  /**
   * User supplies dynamic code in this format:
   * COMPILE ` some code here ` AS groovy NAMED something.groovy;
   * CompileProcessor will compile and package this code into a jar. The jar
   * will be added to the session state via the session state's
   * ADD RESOURCE command.
   * @param command a String to be compiled
   * @return CommandProcessorResponse with some message
   */
  @Override
  public CommandProcessorResponse run(String command) throws CommandProcessorException {
    SessionState ss = SessionState.get();
    this.command = command;

    CommandProcessorResponse authErrResp =
        CommandUtil.authorizeCommand(ss, HiveOperationType.COMPILE, Arrays.asList(command));
    if(authErrResp != null){
      // there was an authorization issue
      return authErrResp;
    }

    myId = runCount.getAndIncrement();

    parse(ss);
    CommandProcessorResponse result = compile(ss);
    return result;
  }

  /**
   * Parses the supplied command
   * @param ss
   * @throws CompileProcessorException if the code can not be compiled or the jar can not be made
   */
  @VisibleForTesting
  void parse(SessionState ss) throws CommandProcessorException {
    if (ss != null){
      command = new VariableSubstitution(new HiveVariableSource() {
        @Override
        public Map<String, String> getHiveVariable() {
          return SessionState.get().getHiveVariables();
        }
      }).substitute(ss.getConf(), command);
    }
    if (command == null || command.length() == 0) {
      throw new CommandProcessorException("Command was empty");
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
      throw new CommandProcessorException(SYNTAX);
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
      throw new CommandProcessorException(SYNTAX);
    }
    StringTokenizer st = new StringTokenizer(command.substring(endPosition+1), " ");
    if (st.countTokens() != 4){
      throw new CommandProcessorException(SYNTAX);
    }
    String shouldBeAs = st.nextToken();
    if (!shouldBeAs.equalsIgnoreCase(AS)){
      throw new CommandProcessorException(SYNTAX);
    }
    setLang(st.nextToken());
    if (!lang.equalsIgnoreCase(GROOVY)){
      throw new CommandProcessorException("Can not compile " + lang + ". Hive can only compile " + GROOVY);
    }
    String shouldBeNamed = st.nextToken();
    if (!shouldBeNamed.equalsIgnoreCase(NAMED)){
      throw new CommandProcessorException(SYNTAX);
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
  CommandProcessorResponse compile(SessionState ss) throws CommandProcessorException {
    String lockout = "rwx------";
    Project proj = new Project();
    String ioTempDir = System.getProperty(IO_TMP_DIR);
    File ioTempFile = new File(ioTempDir);
    if (!ioTempFile.exists()){
      throw new CommandProcessorException(ioTempDir + " does not exists");
    }
    if (!ioTempFile.isDirectory() || !ioTempFile.canWrite()){
      throw new CommandProcessorException(ioTempDir + " is not a writable directory");
    }
    long runStamp = System.currentTimeMillis();
    String user = (ss != null) ? ss.getUserName() : "anonymous";
    File sessionTempFile = new File(ioTempDir, user + "_" + runStamp);
    if (!sessionTempFile.exists()) {
      sessionTempFile.mkdir();
      setPosixFilePermissions(sessionTempFile, lockout, true);
    }
    Groovyc g = new Groovyc();
    String jarId = myId + "_" + runStamp;
    g.setProject(proj);
    Path sourcePath = new Path(proj);
    File destination = new File(sessionTempFile, jarId + "out");
    g.setDestdir(destination);
    File input = new File(sessionTempFile, jarId + "in");
    sourcePath.setLocation(input);
    g.setSrcdir(sourcePath);
    input.mkdir();

    File fileToWrite = new File(input, this.named);
    try {
      Files.write(Paths.get(fileToWrite.toURI()), code.getBytes(Charset.forName("UTF-8")), StandardOpenOption.CREATE_NEW);
    } catch (IOException e1) {
      throw new CommandProcessorException("writing file", e1);
    }
    destination.mkdir();
    try {
      g.execute();
    } catch (BuildException ex){
      throw new CommandProcessorException("Problem compiling", ex);
    }
    File testArchive = new File(sessionTempFile, jarId + ".jar");
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
      setPosixFilePermissions(testArchive, lockout, false);
    } catch (IOException e) {
      throw new CommandProcessorException("Exception while writing jar", e);
    } finally {
      if (out!=null){
        try {
          out.close();
        } catch (IOException WhatCanYouDo) { }
        try {
          if (input.exists())
            FileUtils.forceDeleteOnExit(input);
        } catch (IOException WhatCanYouDo) { /* ignore */ }
        try {
          if (destination.exists())
            FileUtils.forceDeleteOnExit(destination);
        } catch (IOException WhatCanYouDo) { /* ignore */ }
        try {
          if (testArchive != null && testArchive.exists())
            testArchive.deleteOnExit();
        } catch (Exception WhatCanYouDo) { /* ignore */ }
      }
    }

    if (ss != null){
      ss.add_resource(ResourceType.JAR, testArchive.getAbsolutePath());
    }
    CommandProcessorResponse good = new CommandProcessorResponse(null, testArchive.getAbsolutePath());
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

  @Override
  public void close() throws Exception {
  }

  private static synchronized void setPosixFilePermissions(File file, String permsAsString, boolean warnOnly) {
      Set<PosixFilePermission> perms = PosixFilePermissions.fromString(permsAsString);
      try {
        Files.setPosixFilePermissions(Paths.get(file.toURI()), perms);
      } catch (IOException ioe) {
        LOG.warn("Failed to set file permissions on " + file.getAbsolutePath());
        if (!warnOnly) {
          throw new RuntimeException("Exception setting file permissions on " + file.getAbsolutePath());
        }
      }
    }
}
