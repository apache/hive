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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.exec.tez;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Before;
import org.junit.Test;

public class TestYarnQueueHelper {
  private YarnQueueHelper helper;
  private HttpURLConnection connection;

  @Before
  public void setUp() {
    HiveConf conf = new HiveConf();
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, false);
    conf.set(YarnConfiguration.YARN_HTTP_POLICY_KEY, "HTTP_ONLY");
    conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, "localhost:8088");
    helper = new YarnQueueHelper(conf);
    connection = mock(HttpURLConnection.class);
  }

  @Test
  public void testClosesErrorStream() throws IOException {
    InputStream stream = responseStream("Access denied");
    when(connection.getErrorStream()).thenReturn(stream);

    assertEquals("Received 403 (check queue administrators): Access denied",
        helper.handleUnexpectedStatusCode(connection, 403, "check queue administrators"));

    verify(stream).close();
    verify(connection, never()).getInputStream();
  }

  @Test
  public void testClosesFallbackInputStream() throws IOException {
    InputStream stream = responseStream("Unavailable");
    when(connection.getInputStream()).thenReturn(stream);

    assertEquals("Received 503: Unavailable", helper.handleUnexpectedStatusCode(connection, 503, null));

    verify(stream).close();
  }

  @Test
  public void testClosesEmptyErrorStreamWithoutFallingBack() throws IOException {
    InputStream stream = responseStream("");
    when(connection.getErrorStream()).thenReturn(stream);

    assertEquals("Received 500: ", helper.handleUnexpectedStatusCode(connection, 500, null));

    verify(stream).close();
    verify(connection, never()).getInputStream();
  }

  @Test
  public void testMissingResponseStreams() throws IOException {
    assertEquals("Received 200 (No input on successful API call)",
        helper.handleUnexpectedStatusCode(connection, 200, "No input on successful API call"));
  }

  @Test
  public void testClosesErrorStreamOnReadFailure() throws IOException {
    assertClosesStreamOnReadFailure(true);
    verify(connection, never()).getInputStream();
  }

  @Test
  public void testClosesFallbackInputStreamOnReadFailure() throws IOException {
    assertClosesStreamOnReadFailure(false);
  }

  @Test
  public void testPropagatesFallbackInputStreamFailure() throws IOException {
    IOException failure = new IOException("Cannot get input stream");
    when(connection.getInputStream()).thenThrow(failure);

    assertSame(failure, assertThrows(IOException.class,
        () -> helper.handleUnexpectedStatusCode(connection, 500, null)));
  }

  @Test
  public void testPropagatesCloseFailure() throws IOException {
    InputStream stream = responseStream("Unavailable");
    IOException failure = new IOException("Cannot close response");
    doThrow(failure).when(stream).close();
    when(connection.getErrorStream()).thenReturn(stream);

    assertSame(failure, assertThrows(IOException.class,
        () -> helper.handleUnexpectedStatusCode(connection, 503, null)));
    verify(stream).close();
  }

  @Test
  public void testPreservesReadFailureWhenCloseAlsoFails() throws IOException {
    InputStream stream = mock(InputStream.class);
    IOException readFailure = new IOException("Cannot read response");
    IOException closeFailure = new IOException("Cannot close response");
    when(stream.read(any(byte[].class), anyInt(), anyInt())).thenThrow(readFailure);
    doThrow(closeFailure).when(stream).close();
    when(connection.getErrorStream()).thenReturn(stream);

    IOException actual = assertThrows(IOException.class,
        () -> helper.handleUnexpectedStatusCode(connection, 500, null));

    assertSame(readFailure, actual);
    assertEquals(1, actual.getSuppressed().length);
    assertSame(closeFailure, actual.getSuppressed()[0]);
    verify(stream).close();
  }

  private void assertClosesStreamOnReadFailure(boolean useErrorStream) throws IOException {
    InputStream stream = mock(InputStream.class);
    IOException failure = new IOException("Cannot read response");
    when(stream.read(any(byte[].class), anyInt(), anyInt())).thenThrow(failure);
    if (useErrorStream) {
      when(connection.getErrorStream()).thenReturn(stream);
    } else {
      when(connection.getInputStream()).thenReturn(stream);
    }

    assertSame(failure, assertThrows(IOException.class,
        () -> helper.handleUnexpectedStatusCode(connection, 500, null)));
    verify(stream).close();
  }

  private InputStream responseStream(String body) {
    return spy(new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)));
  }
}
