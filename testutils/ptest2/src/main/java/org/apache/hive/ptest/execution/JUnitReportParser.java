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
package org.apache.hive.ptest.execution;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.slf4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.helpers.DefaultHandler;

import com.google.common.collect.Sets;


public class JUnitReportParser {

  private final File directory;
  private final Logger logger;
  private final Set<String> executedTests;
  private final Set<String> failedTests;
  private final Set<String> testClassesWithAvailableReports;
  private final Set<File> testOutputFiles;
  private boolean parsed;
  public JUnitReportParser(Logger logger, File directory) throws Exception {
    this.logger = logger;
    this.directory = directory;
    executedTests = Sets.newHashSet();
    failedTests =  Sets.newHashSet();
    testOutputFiles = Sets.newHashSet();
    testClassesWithAvailableReports = Sets.newHashSet();
    parsed = false;
  }

  public Set<String> getAllExecutedTests() {
    parseIfRequired();
    return executedTests;
  }

  public Set<String> getAllFailedTests() {
    parseIfRequired();
    return failedTests;
  }

  public Set<String> getTestClassesWithReportAvailable() {
    return Collections.unmodifiableSet(testClassesWithAvailableReports);
  }

  public int getNumAttemptedTests() {
    parseIfRequired();
    return getAllExecutedTests().size() + getAllFailedTests().size();
  }

  private void parseIfRequired() {
    if (!parsed) {
      parse();
      parsed = true;
    }
  }

  private void parse() {
    populateTestFileList(directory);
    for(File file : testOutputFiles) {
      FileInputStream stream = null;
      try {
        stream = new FileInputStream(file);
        SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser saxParser = factory.newSAXParser();
        saxParser.parse(new InputSource( stream ), new DefaultHandler() {
          private String name;
          private boolean failedOrErrored;
          @Override
          public void startElement(String uri, String localName, String qName, Attributes attributes) {
            if ("testcase".equals(qName)) {
              name = attributes.getValue("classname");
              failedOrErrored = false;
              if(name == null || "junit.framework.TestSuite".equals(name)) {
                name = attributes.getValue("name");
              } else {
                name = name + "." + attributes.getValue("name");
              }
            } else if (name != null) {
              if ("failure".equals(qName) || "error".equals(qName)) {
                failedOrErrored = true;
              } else if("skipped".equals(qName)) {
                name = null;
              }
            }
          }
          @Override
          public void endElement(String uri, String localName, String qName)  {
            if ("testcase".equals(qName)) {
              if(name != null) {
                executedTests.add(name);
                if(failedOrErrored) {
                  failedTests.add(name);
                }
              }
            }
          }
        });
      } catch (Exception e) {
        logger.error("Error parsing file " + file, e);
      } finally {
        if(stream != null) {
          try {
            stream.close();
          } catch (IOException e) {
            logger.warn("Error closing file " + file, e);
          }
        }
      }
    }
  }

  private void populateTestFileList(File directory) {
    File[] files = directory.listFiles();
    if(files != null) {
      for(File file : files) {
        if(file.isFile()) {
          String name = file.getName();
          if(name.startsWith("TEST-") && name.endsWith(".xml")) {
            testOutputFiles.add(file);
            int idx = name.lastIndexOf("TEST-");
            name = name.substring(idx + "TEST-".length());
            String parts[] = name.split("\\.");
            String testClass = parts[parts.length-2];
            testClassesWithAvailableReports.add(testClass);
          }
        }
      }
    }
  }
}