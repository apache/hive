/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.http;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcJarDownloadServlet extends HttpServlet {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcJarDownloadServlet.class);

  private static final String JDBC_JAR_DIR = System.getenv("HIVE_HOME") + "/jdbc/";
  private static final String JDBC_JAR_PATTERN = "hive-jdbc-*-standalone.jar";
  private static final String JAR_CONTENT_DISPOSITION = "attachment; filename=hive-jdbc-standalone.jar";
  private static final String JAR_CONTENT_TYPE = "application/java-archive";

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {

    LOG.info("Requesting jdbc standalone jar download");

    response.setHeader("Content-disposition", JAR_CONTENT_DISPOSITION);
    response.setContentType(JAR_CONTENT_TYPE);

    File dir = new File(JDBC_JAR_DIR);
    FileFilter fileFilter = new WildcardFileFilter(JDBC_JAR_PATTERN);
    File[] files = dir.listFiles(fileFilter);
    if (files == null || files.length != 1) {
      handleError(files);
      response.sendError(HttpServletResponse.SC_NOT_FOUND);
      return;
    }
    File file = files[0];
    LOG.info("Jdbc standalone jar found: " + file.getAbsolutePath());

    try (FileInputStream in = new FileInputStream(file);
         OutputStream out = response.getOutputStream()) {
      byte[] buffer = new byte[4096];
      int length;
      while ((length = in.read(buffer)) > 0) {
        out.write(buffer, 0, length);
      }
    } catch (Exception e) {
      LOG.error("Exception during downloading standalone jdbc jar", e);
      response.sendError(HttpServletResponse.SC_NOT_FOUND);
    }

    LOG.info("Jdbc standalone jar " + file.getAbsolutePath() + " was downloaded");
  }

  private void handleError(File[] files) {
    if (ArrayUtils.isEmpty(files)) {
      LOG.error("No jdbc standalone jar found in the directory " + JDBC_JAR_DIR);
    } else {
      StringBuilder fileNames = new StringBuilder();
      for (File file : files) {
        fileNames.append("\t" + file.getAbsolutePath() + "\n");
      }
      LOG.error("Multiple jdbc standalone jars exist in the directory " + JDBC_JAR_DIR + ":\n" + fileNames);
    }
  }
}