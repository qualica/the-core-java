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
import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.client.AMQQueue;
import org.apache.qpid.client.AMQTopic;
import org.apache.qpid.framing.AMQShortString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * @author <a href="mailto:nithia.govender@korwe.com>Nithia Govender</a>
 */
public class CoreSender {

    private static final Logger LOG = LoggerFactory.getLogger(CoreSender.class);

    protected MessageQueue queue;
    protected Connection connection;
    protected CoreMessageSerializer serializer;
    protected Session session;

    protected CoreSender() {

    }


    public CoreSender(MessageQueue queue) {
        try {
            CoreConfig config = CoreConfig.getConfig();
            this.queue = queue;
            LOG.info("Connecting to queue server " + config.getSetting("amqp_server"));
            connection = new AMQConnection(config.getSetting("amqp_server"), config.getIntSetting("amqp_port"),
                                           config.getSetting("amqp_user"),
                                           config.getSetting("amqp_password"), null, config.getSetting("amqp_vhost"));
            LOG.info("Connected");

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            serializer = new CoreMessageXmlSerializer();
        }
        catch (Exception e) {
            LOG.error("Connection failed", e);
            throw new RuntimeException(e);
        }
    }

    public void close() {
        LOG.info("Closing sender session and connection");
        try {
            session.close();
            connection.close();
        }
        catch (JMSException e) {
            LOG.warn("Unable to close sender", e);
        }
    }

    public void sendMessage(CoreMessage message) {
        if (queue.isDirect()) {
            String exchange = MessageQueue.DIRECT_EXCHANGE;
            String routing = queue.getQueueName();
            LOG.debug("Sending to " + routing);
            AMQShortString queueName = AMQShortString.valueOf(routing);
            Destination destination = new AMQQueue(AMQShortString.valueOf(exchange), queueName, queueName);
            send(message, destination, routing);
        }
        else {
            LOG.error("Message to topic queue must be explicitly addressed");
        }
    }

    public void sendMessage(CoreMessage message, String recipient) {
        if (queue.isTopic()) {
            String exchange = MessageQueue.TOPIC_EXCHANGE;
            String routing = queue.getQueueName() + "." + recipient;
            LOG.debug("Sending to " + routing);
            AMQShortString queueName = AMQShortString.valueOf(routing);
            Destination destination = new AMQTopic(AMQShortString.valueOf(exchange), queueName);

            send(message, destination, routing);
        }
        else {
            LOG.error("Cannot send to explicitly addressed message direct point to point queue");
        }

    }

    private void send(CoreMessage message, Destination destination, String routing) {
        try {
            String serialized = serializer.serialize(message);

            TextMessage jmsMessage = session.createTextMessage(serialized);
            jmsMessage.setStringProperty("sessionId", message.getSessionId());
            jmsMessage.setStringProperty("guid", message.getGuid());
            jmsMessage.setStringProperty("choreography", message.getChoreography());
            jmsMessage.setStringProperty("messageType", message.getMessageType().name());

            LOG.debug("Sending message [{}]", message.getGuid());

            MessageProducer producer = session.createProducer(destination);
            producer.send(jmsMessage);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sent: " + serialized);
            }
            else {
                int endIndex = serialized.indexOf("</function");
                LOG.info("Sent: " + serialized.substring(0, endIndex > 0 ? endIndex : Math.min(350, serialized.length())));
            }
        }
        catch (JMSException e) {
            LOG.error("Error sending message", e);
            throw new RuntimeException(e);
        }
    }

    public MessageQueue getQueue() {
        return queue;
    }

}
