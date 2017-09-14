package org.apache.hadoop.hive.metastore.repl;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public class DumpDirCleanerTask extends TimerTask {
  public static final Logger LOG = LoggerFactory.getLogger(DumpDirCleanerTask.class);
  private final HiveConf conf;
  private final Path dumpRoot;
  private final long ttl;

  public DumpDirCleanerTask(HiveConf conf) {
    this.conf = conf;
    dumpRoot = new Path(conf.getVar(HiveConf.ConfVars.REPLDIR));
    ttl = conf.getTimeVar(ConfVars.REPL_DUMPDIR_TTL, TimeUnit.MILLISECONDS);
  }

  @Override
  public void run() {
    LOG.debug("Trying to delete old dump dirs");
    try {
      FileSystem fs = FileSystem.get(dumpRoot.toUri(), conf);
      FileStatus[] statuses = fs.listStatus(dumpRoot);
      for (FileStatus status : statuses)
      {
        if (status.getModificationTime() < System.currentTimeMillis() - ttl)
        {
          fs.delete(status.getPath(), true);
          LOG.info("Deleted old dump dir: " + status.getPath());
        }
      }
    } catch (IOException e) {
      LOG.error("Error while trying to delete dump dir", e);
    }
  }
}
