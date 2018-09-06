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

import com.korwe.thecore.messages.*;
import com.rabbitmq.client.*;
import org.slf4j.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author <a href="mailto:nithia.govender@korwe.com>Nithia Govender</a>
 */
public class CoreSender implements ShutdownListener, RecoveryListener {

    private static final Logger LOG = LoggerFactory.getLogger(CoreSender.class);

    protected MessageQueue queue;
    protected Connection connection;
    protected CoreMessageSerializer serializer;
    protected Channel channel;

    protected CoreSender() {
        this.serializer = new CoreMessageXmlSerializer();
    }

    public CoreSender(MessageQueue queue) {
        this();
        this.queue = queue;
        try {
            this.connection = CoreConnection.coreConnection(CoreConfig.getConfig());
            if (connection != null) {
                this.channel = connection.createChannel();
                channel.addShutdownListener(this);
                LOG.info("Successfully Created Channel");
            }
        }
        catch (IOException e) {
            LOG.error("Error creating channel and connection", e);
        }
    }

    public void close() {
        LOG.info("Closing sender channel and connection");
        try {
            if (channel != null) {
                channel.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        catch (TimeoutException | IOException e) {
            LOG.error("Error closing channel", e);
        }
    }

    public void sendMessage(CoreMessage message) {
        if (connection == null) {
            throw new RuntimeException("No Connection To RabbitMq");
        }
        if (queue.isDirect()) {
            String exchange = MessageQueue.DIRECT_EXCHANGE;
            String routing = queue.getQueueName();
            LOG.debug("Sending to " + routing);
            try {
                channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT, true, true, null);
                String queueName = channel.queueDeclare(queue.getQueueName(), true, false, true, null).getQueue();
                channel.queueBind(queueName, exchange, routing);
                send(message, exchange, routing);
            }
            catch (IOException e) {
                LOG.error("Error sending message", e);
            }
        }
        else {
            LOG.error("Message to topic queue must be explicitly addressed");
        }
    }

    public void sendMessage(CoreMessage message, String recipient) {
        if (connection == null) {
            throw new RuntimeException("No Connection To RabbitMq");
        }
        if (queue.isTopic()) {
            String exchange = MessageQueue.TOPIC_EXCHANGE;
            String routing = queue.getQueueName() + "." + recipient;
            LOG.debug("Sending to " + routing);
            try {
                channel.exchangeDeclare(exchange, BuiltinExchangeType.TOPIC, true, true, null);
                routing = channel.queueDeclare(routing, true, false, true, null).getQueue();
                channel.queueBind(routing, exchange, routing);
                send(message, exchange, routing);
            }
            catch (IOException e) {
                LOG.error("Error sending message", e);
            }
        }
        else {
            LOG.error("Cannot send to explicitly addressed message direct point to point queue");
        }

    }

    private void send(CoreMessage message, String exchange, String routing) {
        try {
            String serialized = serializer.serialize(message);
            Map<String, Object> messagePropertyBag = new HashMap<>();
            messagePropertyBag.put("sessionId", message.getSessionId());
            messagePropertyBag.put("guid", message.getGuid());
            messagePropertyBag.put("choreography", message.getChoreography());
            messagePropertyBag.put("messageType", message.getMessageType().name());
            AMQP.BasicProperties.Builder messageProperties = new AMQP.BasicProperties.Builder().headers(messagePropertyBag);
            LOG.debug("Sending message [{}]", message.getGuid());
            channel.basicPublish(exchange, routing, messageProperties.build(), serialized.getBytes());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sent: " + serialized);
            }
            else {
                int endIndex = serialized.indexOf("</function");
                LOG.info("Sent: " + serialized.substring(0, endIndex > 0 ? endIndex : Math.min(350, serialized.length())));
            }
        }
        catch (IOException e) {
            LOG.error("Error sending message", e);
        }
    }

    public MessageQueue getQueue() {
        return queue;
    }

    @Override
    public void shutdownCompleted(ShutdownSignalException cause) {
        LOG.warn("Connection or Channel Shutdown: {}", cause.getReason());
    }

    @Override
    public void handleRecovery(Recoverable recoverable) {
        LOG.info("Connection or Channel Recovered");
    }

    @Override
    public void handleRecoveryStarted(Recoverable recoverable) {
        LOG.info("Connection or Channel Recovering");
    }
}
