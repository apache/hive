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

package org.apache.hive.hcatalog.messaging.jms;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.messaging.HCatEventMessage;
import org.apache.hive.hcatalog.messaging.MessageFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

/**
 * Helper Utility to assist consumers of HCat Messages in extracting
 * message-content from JMS messages.
 */
public class MessagingUtils {

  /**
   * Method to return HCatEventMessage contained in the JMS message.
   * @param message The JMS Message instance
   * @return The contained HCatEventMessage
   */
  public static HCatEventMessage getMessage(Message message) {
    try {
      String messageBody = ((TextMessage)message).getText();
      String eventType   = message.getStringProperty(HCatConstants.HCAT_EVENT);
      String messageVersion = message.getStringProperty(HCatConstants.HCAT_MESSAGE_VERSION);
      String messageFormat = message.getStringProperty(HCatConstants.HCAT_MESSAGE_FORMAT);

      if (StringUtils.isEmpty(messageBody) || StringUtils.isEmpty(eventType))
        throw new IllegalArgumentException("Could not extract HCatEventMessage. " +
                           "EventType and/or MessageBody is null/empty.");

      return MessageFactory.getDeserializer(messageFormat, messageVersion).getHCatEventMessage(eventType, messageBody);
    }
    catch (JMSException exception) {
      throw new IllegalArgumentException("Could not extract HCatEventMessage. ", exception);
    }
  }

  // Prevent construction.
  private MessagingUtils() {}
}
