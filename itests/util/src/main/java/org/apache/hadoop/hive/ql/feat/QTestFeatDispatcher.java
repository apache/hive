package org.apache.hadoop.hive.ql.feat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.ql.QTestUtil;

public class QTestFeatDispatcher {

  Map<String, QTestFeatHandler> handlers = new HashMap<String, QTestFeatHandler>();

  public void register(String prefix, QTestFeatHandler datasetHandler) {
    if (handlers.containsKey(prefix)) {
      throw new RuntimeException();
    }
    handlers.put(prefix, datasetHandler);
  }

  public void process(File file) {
    synchronized (QTestUtil.class) {

      parse(file);

      //      Set<String> missingDatasets = datasets.getTables();
      //      missingDatasets.removeAll(getSrcTables());
      //      if (missingDatasets.isEmpty()) {
      //        return;
      //      }
      //      qt.newSession(true);
      //      for (String table : missingDatasets) {
      //        if (initDataset(table, cliDriver)) {
      //          addSrcTable(table);
      //        }
      //      }
      //      qt.newSession(true);
    }
  }

  public void parse(File file) {
    Pattern p = Pattern.compile(" *--! ?qt:([a-z]+):?(.*)");
    
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        String l = line.trim();
        Matcher m = p.matcher(l);
        if (m.matches()) {
          String sub = m.group(1);
          String arguments = m.group(2);
          if (!handlers.containsKey(sub)) {
            throw new IOException("Don't know how to handle " + sub + "  line: " + l);
          }
          handlers.get(sub).processArguments(arguments);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Error while processing file: " + file, e);
    }
  }

  public void beforeTest(QTestUtil qt) throws Exception {
    for (QTestFeatHandler h : handlers.values()) {
      h.beforeTest(qt);
    }
  }

}
