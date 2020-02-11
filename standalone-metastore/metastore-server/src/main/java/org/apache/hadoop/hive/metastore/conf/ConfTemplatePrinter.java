/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.conf;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Text;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;

public class ConfTemplatePrinter {

  private Document generateTemplate() throws ParserConfigurationException {
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
        " WARNING!!! Any changes you make to this file will be ignored by the metastore.   "));
    root.appendChild(doc.createComment(
        " WARNING!!! You must make your changes in metastore-site.xml instead.         "));

    root.appendChild(doc.createComment(" Metastore Execution Parameters "));

    root.appendChild(doc.createComment("================================"));
    root.appendChild(doc.createComment("All time unit values have a time unit abbreviation suffix"));
    root.appendChild(doc.createComment("Any time value can take any of the units"));
    root.appendChild(doc.createComment("d = day"));
    root.appendChild(doc.createComment("h = hour"));
    root.appendChild(doc.createComment("m = minute"));
    root.appendChild(doc.createComment("s = second"));
    root.appendChild(doc.createComment("ms = millisecond"));
    root.appendChild(doc.createComment("us = microsecond"));
    root.appendChild(doc.createComment("ns = nanosecond"));
    root.appendChild(doc.createComment("================================"));

    for (MetastoreConf.ConfVars confVars : MetastoreConf.ConfVars.values()) {
      Element property = appendElement(root, "property", null);
      appendElement(property, "name", confVars.getVarname());
      appendElement(property, "value", confVars.getDefaultVal().toString());
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

  private void writeToFile(File template, Document document) throws TransformerException {
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

  private void print(String fileName) throws ParserConfigurationException, TransformerException {
    Document doc = generateTemplate();
    File file = new File(fileName);
    File dir = file.getParentFile();
    // Make certain the target directory exists.
    dir.mkdirs();
    writeToFile(file, doc);
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      String msg = "Usage: ConfTemplatePrinter filename";
      System.err.println(msg);
      throw new RuntimeException(msg);
    }
    ConfTemplatePrinter printer = new ConfTemplatePrinter();
    printer.print(args[0]);
  }
}
