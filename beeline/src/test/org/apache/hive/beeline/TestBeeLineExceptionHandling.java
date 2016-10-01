/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.beeline;

import junit.framework.Assert;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestBeeLineExceptionHandling {

  public class TestBeeline extends BeeLine {
    private String expectedLoc;
    private int logCount;
    public TestBeeline(String expectedLoc) {
      this.expectedLoc = expectedLoc;
      this.logCount = 0;
    }

    @Override
    boolean error(String log) {
      if (logCount == 0) {
        Assert.assertEquals(loc(expectedLoc), log);
      } else {
        Assert.assertEquals("Error: org.apache.thrift.transport.TTransportException "
            + "(state=,code=0)", log);
      }
      logCount++;
      return false;
    }
  }

  @Test
  public void testHandleSQLExceptionLog() throws Exception {
    checkException(TTransportException.ALREADY_OPEN, "hs2-connection-already-open");
    checkException(TTransportException.END_OF_FILE, "hs2-unexpected-end-of-file");
    checkException(TTransportException.NOT_OPEN, "hs2-could-not-open-connection");
    checkException(TTransportException.TIMED_OUT, "hs2-connection-timed-out");
    checkException(TTransportException.UNKNOWN, "hs2-unknown-connection-problem");
    checkException(-1, "hs2-unexpected-error");
  }

  private void checkException(int type, String loc) {
    BeeLine testBeeLine = new TestBeeline(loc);
    TTransportException tTransportException = new TTransportException(type);
    SQLException sqlException = new SQLException(tTransportException);
    testBeeLine.handleSQLException(sqlException);
  }
}
