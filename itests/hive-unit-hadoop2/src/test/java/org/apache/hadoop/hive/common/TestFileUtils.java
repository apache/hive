package org.apache.hadoop.hive.common;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for {{@link FileUtils}. Tests run against a {@link HadoopShims.MiniDFSShim}.
 */
public class TestFileUtils {

  private static final Path basePath = new Path("/tmp/");

  private static HiveConf conf;
  private static FileSystem fs;
  private static HadoopShims.MiniDFSShim dfs;

  @BeforeClass
  public static void setup() throws Exception {
    conf = new HiveConf(TestFileUtils.class);
    dfs = ShimLoader.getHadoopShims().getMiniDfs(conf, 4, true, null);
    fs = dfs.getFileSystem();
  }

  @Test
  public void testCopySingleEmptyFile() throws IOException {
    String file1Name = "file1.txt";
    Path copySrc = new Path(basePath, "copySrc");
    Path copyDst = new Path(basePath, "copyDst");
    try {
      fs.create(new Path(basePath, new Path(copySrc, file1Name))).close();
      Assert.assertTrue("FileUtils.copy failed to copy data",
              FileUtils.copy(fs, copySrc, fs, copyDst, false, false, conf));

      Path dstFileName1 = new Path(copyDst, file1Name);
      Assert.assertTrue(fs.exists(new Path(copyDst, file1Name)));
      Assert.assertEquals(fs.getFileStatus(dstFileName1).getLen(), 0);
    } finally {
      try {
        fs.delete(copySrc, true);
        fs.delete(copyDst, true);
      } catch (IOException e) {
        // Do nothing
      }
    }
  }

  @Test
  public void testCopyWithDistcp() throws IOException {
    String file1Name = "file1.txt";
    String file2Name = "file2.txt";
    Path copySrc = new Path(basePath, "copySrc");
    Path copyDst = new Path(basePath, "copyDst");
    Path srcFile1 = new Path(basePath, new Path(copySrc, file1Name));
    Path srcFile2 = new Path(basePath, new Path(copySrc, file2Name));
    try {
      OutputStream os1 = fs.create(srcFile1);
      os1.write(new byte[]{1, 2, 3});
      os1.close();

      OutputStream os2 = fs.create(srcFile2);
      os2.write(new byte[]{1, 2, 3});
      os2.close();

      conf.set(HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname, "1");
      conf.set(HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname, "1");
      Assert.assertTrue("FileUtils.copy failed to copy data",
              FileUtils.copy(fs, copySrc, fs, copyDst, false, false, conf));

      Path dstFileName1 = new Path(copyDst, file1Name);
      Assert.assertTrue(fs.exists(new Path(copyDst, file1Name)));
      Assert.assertEquals(fs.getFileStatus(dstFileName1).getLen(), 3);

      Path dstFileName2 = new Path(copyDst, file2Name);
      Assert.assertTrue(fs.exists(new Path(copyDst, file2Name)));
      Assert.assertEquals(fs.getFileStatus(dstFileName2).getLen(), 3);
    } finally {
      try {
        fs.delete(copySrc, true);
        fs.delete(copyDst, true);
      } catch (IOException e) {
        // Do nothing
      }
    }
  }

  @AfterClass
  public static void shutdown() throws IOException {
    fs.close();
    dfs.shutdown();
  }
}
