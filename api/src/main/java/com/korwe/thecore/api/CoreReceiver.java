/*
 * Copyright (c) 2010.  Korwe Software
 *
 *  This file is part of TheCore.
 *
 *  TheCore is free software: you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  TheCore is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with TheCore.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.korwe.thecore.api;

import com.korwe.thecore.messages.CoreMessage;
import com.korwe.thecore.messages.CoreMessageSerializer;
import com.korwe.thecore.messages.CoreMessageXmlSerializer;
import org.apache.qpid.AMQException;
import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.client.AMQQueue;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.transport.MessageAcceptMode;
import org.apache.qpid.url.URLSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

/**
 * @author <a href="mailto:nithia.govender@korwe.com>Nithia Govender</a>
 */
public class CoreReceiver implements MessageListener {

    private static final Logger LOG = LoggerFactory.getLogger(CoreReceiver.class);

    private final MessageQueue queue;
    protected Connection connection;
    private CoreMessageSerializer serializer;
    private Session session;
    private CoreMessageHandler handler;
    private String queueName;

    public CoreReceiver(MessageQueue queue) {
        this.queue = queue;
        serializer = new CoreMessageXmlSerializer();
    }

    public void connect(CoreMessageHandler handler) {
        try {
            this.handler = handler;

            CoreConfig config = CoreConfig.getConfig();

            connection = new AMQConnection(config.getSetting("amqp_server"),
                                                                   config.getIntSetting("amqp_port"),
                                                                   config.getSetting("amqp_user"),
                                                                   config.getSetting("amqp_password"),
                                                                   null,
                                                                   config.getSetting("amqp_vhost"));
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            queueName = getQueueName(queue);
            bindToQueue(queueName, session);
            LOG.info("Connected and waiting for messages: " + queueName);
        }
        catch (Exception e) {
            LOG.error("Error connecting to broker", e);
            throw new RuntimeException(e);
        }

    }

    public void close() {
        LOG.info("Closing receiver session and connection");
        try {
            session.close();
            connection.close();
        }
        catch (JMSException e) {
            LOG.warn("Error connecting to broker", e);
        }
    }

    protected void bindToQueue(String queueName, Session session) throws JMSException {
        AMQShortString amqName = AMQShortString.valueOf(queueName);
        Destination destination = new AMQQueue(AMQShortString.valueOf(MessageQueue.DIRECT_EXCHANGE), amqName, amqName);

        MessageConsumer consumer = session.createConsumer(destination);
        consumer.setMessageListener(this);
        connection.start();
    }

    protected String getQueueName(MessageQueue queue) {
        return queue.getQueueName();
    }

    @Override
    public void onMessage(Message jmsMessage) {
        try {
            String msgText = "";
            if (jmsMessage instanceof BytesMessage) {
                msgText = ((BytesMessage) jmsMessage).readUTF();
            }
            else if (jmsMessage instanceof TextMessage) {
                msgText = ((TextMessage) jmsMessage).getText();
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Received: " + msgText);
            }
            else {
                int endIndex = msgText.indexOf("</function");
                LOG.info("Received: " + msgText.substring(0, endIndex > 0 ? endIndex : Math.min(350, msgText.length())));
            }
            CoreMessage message = serializer.deserialize(msgText);
            handler.handleMessage(message);
        }
        catch (JMSException e) {
            LOG.error("Error receiving message", e);
            throw new RuntimeException(e);
        }
    }

}
