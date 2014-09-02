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

package org.apache.hadoop.hive.ant;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Text;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.net.URL;

/**
 * Generates hive-default.xml.template from HiveConf.ConfVars
 */
public class GenHiveTemplate extends Task {

  private String templateFile;

  public String getTemplateFile() {
    return templateFile;
  }

  public void setTemplateFile(String templateFile) {
    this.templateFile = templateFile;
  }

  private void generate() throws Exception {
    File current = new File(templateFile);
    if (current.exists()) {
      ClassLoader loader = GenHiveTemplate.class.getClassLoader();
      URL url = loader.getResource("org/apache/hadoop/hive/conf/HiveConf.class");
      if (url != null) {
        File file = new File(url.getFile());
        if (file.exists() && file.lastModified() < current.lastModified()) {
          return;
        }
      }
    }
    writeToFile(current, generateTemplate());
  }

  private Document generateTemplate() throws Exception {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder docBuilder = dbf.newDocumentBuilder();
    Document doc = docBuilder.newDocument();
    doc.appendChild(doc.createProcessingInstruction(
        "xml-stylesheet", "type=\"text/xsl\" href=\"configuration.xsl\""));

    doc.appendChild(doc.createComment("\n" +
        "   Licensed to the Apache Software Foundation (ASF) under one or more\n" +
        "   contributor license agreements.  See the NOTICE file distributed with\n" +
        "   this work for additional information regarding copyright ownership.\n" +
        "   The ASF licenses this file to You under the Apache License, Version 2.0\n" +
        "   (the \"License\"); you may not use this file except in compliance with\n" +
        "   the License.  You may obtain a copy of the License at\n" +
        "\n" +
        "       http://www.apache.org/licenses/LICENSE-2.0\n" +
        "\n" +
        "   Unless required by applicable law or agreed to in writing, software\n" +
        "   distributed under the License is distributed on an \"AS IS\" BASIS,\n" +
        "   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n" +
        "   See the License for the specific language governing permissions and\n" +
        "   limitations under the License.\n"));

    Element root = doc.createElement("configuration");
    doc.appendChild(root);

    root.appendChild(doc.createComment(
        " WARNING!!! This file is auto generated for documentation purposes ONLY! "));
    root.appendChild(doc.createComment(
        " WARNING!!! Any changes you make to this file will be ignored by Hive.   "));
    root.appendChild(doc.createComment(
        " WARNING!!! You must make your changes in hive-site.xml instead.         "));

    root.appendChild(doc.createComment(" Hive Execution Parameters "));

    Thread.currentThread().setContextClassLoader(ShimLoader.class.getClassLoader());
    for (HiveConf.ConfVars confVars : HiveConf.ConfVars.values()) {
      if (confVars.isExcluded()) {
        // thought of creating template for each shims, but I couldn't generate proper mvn script
        continue;
      }
      Element property = appendElement(root, "property", null);
      appendElement(property, "name", confVars.varname);
      appendElement(property, "value", confVars.getDefaultExpr());
      appendElement(property, "description", normalize(confVars.getDescription()));
      // wish to add new line here.
    }
    return doc;
  }

  private String normalize(String description) {
    int index = description.indexOf('\n');
    if (index < 0) {
      return description;
    }
    int prev = 0;
    StringBuilder builder = new StringBuilder(description.length() << 1);
    for (;index > 0; index = description.indexOf('\n', prev = index + 1)) {
      builder.append("\n      ").append(description.substring(prev, index));
    }
    if (prev < description.length()) {
      builder.append("\n      ").append(description.substring(prev));
    }
    builder.append("\n    ");
    return builder.toString();
  }

  private void writeToFile(File template, Document document) throws Exception {
    Transformer transformer = TransformerFactory.newInstance().newTransformer();
    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
    transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
    DOMSource source = new DOMSource(document);
    StreamResult result = new StreamResult(template);
    transformer.transform(source, result);
  }

  private Element appendElement(Element parent, String name, String text) {
    Document document = parent.getOwnerDocument();
    Element child = document.createElement(name);
    parent.appendChild(child);
    if (text != null) {
      Text textNode = document.createTextNode(text);
      child.appendChild(textNode);
    }
    return child;
  }

  @Override
  public void execute() throws BuildException {
    try {
      generate();
    } catch (Exception e) {
      throw new BuildException(e);
    }
  }

  public static void main(String[] args) throws Exception {
    GenHiveTemplate gen = new GenHiveTemplate();
    gen.generate();
  }
}
