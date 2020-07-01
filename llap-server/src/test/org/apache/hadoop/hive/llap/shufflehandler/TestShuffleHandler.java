/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.shufflehandler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.ShuffleHeader;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.Checksum;

public class TestShuffleHandler {

  @Test(timeout = 5000)
  public void testDagDelete() throws Exception {
    final ArrayList<Throwable> failures = new ArrayList<Throwable>(1);
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, ShuffleHandler.DEFAULT_SHUFFLE_PORT);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "simple");
    UserGroupInformation.setConfiguration(conf);
    File absLogDir = new File("target", TestShuffleHandler.class.
        getSimpleName() + "LocDir").getAbsoluteFile();
    conf.set(ShuffleHandler.SHUFFLE_HANDLER_LOCAL_DIRS, absLogDir.getAbsolutePath());
    ApplicationId appId = ApplicationId.newInstance(12345, 1);
    String appAttemptId = "attempt_12345_1_m_1_0";
    String user = "randomUser";
    List<File> fileMap = new ArrayList<File>();
    createShuffleHandlerFiles(absLogDir, user, appId.toString(), appAttemptId,
        conf, fileMap);
    ShuffleHandler shuffleHandler = new ShuffleHandler(conf) {
      @Override
      protected Shuffle getShuffle(Configuration conf) {
        // replace the shuffle handler with one stubbed for testing
        return new Shuffle(conf) {
          @Override
          protected void sendError(ChannelHandlerContext ctx, String message,
                                   HttpResponseStatus status) {
            if (failures.size() == 0) {
              failures.add(new Error(message));
              ctx.getChannel().close();
            }
          }
        };
      }
    };
    try {
      shuffleHandler.start();
      DataOutputBuffer outputBuffer = new DataOutputBuffer();
      outputBuffer.reset();
      Token<JobTokenIdentifier> jt =
          new Token<JobTokenIdentifier>("identifier".getBytes(),
              "password".getBytes(), new Text(user), new Text("shuffleService"));
      jt.write(outputBuffer);
      shuffleHandler.registerDag(appId.toString(), 1, jt, user, null);
      URL url =
          new URL(
              "http://127.0.0.1:"
                  + conf.get(
                  ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY)
                  + "/mapOutput?dagAction=delete&job=job_12345_0001&dag=1");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      String dagDirStr =
          StringUtils.join(Path.SEPARATOR,
              new String[] { absLogDir.getAbsolutePath(),
                  ShuffleHandler.USERCACHE, user,
                  ShuffleHandler.APPCACHE, appId.toString(),"dag_1/"});
      File dagDir = new File(dagDirStr);
      Assert.assertTrue("Dag Directory does not exist!", dagDir.exists());
      conn.connect();
      try {
        DataInputStream is = new DataInputStream(conn.getInputStream());
        is.close();
        Assert.assertFalse("Dag Directory was not deleted!", dagDir.exists());
      } catch (EOFException e) {
        // ignore
      }
      Assert.assertEquals("sendError called due to shuffle error",
          0, failures.size());
    } finally {
      shuffleHandler.stop();
      FileUtil.fullyDelete(absLogDir);
    }
  }

  private static void createShuffleHandlerFiles(File logDir, String user,
                                                String appId, String appAttemptId, Configuration conf,
                                                List<File> fileMap) throws IOException {
    String attemptDir =
        StringUtils.join(Path.SEPARATOR,
            new String[] { logDir.getAbsolutePath(),
                ShuffleHandler.USERCACHE, user,
                ShuffleHandler.APPCACHE, appId,"dag_1/" + "output",
                appAttemptId });
    File appAttemptDir = new File(attemptDir);
    appAttemptDir.mkdirs();
    System.out.println(appAttemptDir.getAbsolutePath());
    File indexFile = new File(appAttemptDir, "file.out.index");
    fileMap.add(indexFile);
    createIndexFile(indexFile, conf);
    File mapOutputFile = new File(appAttemptDir, "file.out");
    fileMap.add(mapOutputFile);
    createMapOutputFile(mapOutputFile, conf);
  }

  private static void
  createMapOutputFile(File mapOutputFile, Configuration conf)
      throws IOException {
    FileOutputStream out = new FileOutputStream(mapOutputFile);
    out.write("Creating new dummy map output file. Used only for testing"
        .getBytes());
    out.flush();
    out.close();
  }

  private static void createIndexFile(File indexFile, Configuration conf)
      throws IOException {
    if (indexFile.exists()) {
      System.out.println("Deleting existing file");
      indexFile.delete();
    }
    Checksum crc = new PureJavaCrc32();
    TezSpillRecord tezSpillRecord = new TezSpillRecord(2);
    tezSpillRecord.putIndex(new TezIndexRecord(0, 10, 10), 0);
    tezSpillRecord.putIndex(new TezIndexRecord(10, 10, 10), 1);
    tezSpillRecord.writeToFile(new Path(indexFile.getAbsolutePath()), conf, crc);
  }
}
