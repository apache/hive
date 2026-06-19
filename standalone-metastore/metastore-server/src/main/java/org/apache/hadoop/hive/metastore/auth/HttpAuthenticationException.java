/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.hive.metastore.auth;

import java.util.Optional;
import javax.servlet.http.HttpServletResponse;

/*
Encapsulates any exceptions thrown by HiveMetastore server
when authenticating http requests
 */
public class HttpAuthenticationException extends Exception {

  private static final long serialVersionUID = 0;

  private final int statusCode;
  private final String wwwAuthenticateHeader;

  /**
   * @param msg exception message
   */
  public HttpAuthenticationException(String msg) {
    this(msg, null);
  }

  /**
   * @param msg   exception message
   * @param cause original exception
   */
  public HttpAuthenticationException(String msg, Throwable cause) {
    this(msg, cause, HttpServletResponse.SC_UNAUTHORIZED);
  }

  public HttpAuthenticationException(String msg, Throwable cause, int statusCode) {
    this(msg, cause, statusCode, null);
  }

  public HttpAuthenticationException(String msg, int statusCode, String wwwAuthenticateHeader) {
    this(msg, null, statusCode, wwwAuthenticateHeader);
  }

  public HttpAuthenticationException(String msg, Throwable cause, int statusCode, String wwwAuthenticateHeader) {
    super(msg, cause);
    this.statusCode = statusCode;
    this.wwwAuthenticateHeader = wwwAuthenticateHeader;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public Optional<String> getWwwAuthenticateHeader() {
    return Optional.ofNullable(wwwAuthenticateHeader);
  }
}
