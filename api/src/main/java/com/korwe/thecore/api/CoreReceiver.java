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
import java.util.concurrent.*;

/**
 * @author <a href="mailto:nithia.govender@korwe.com>Nithia Govender</a>
 */
public class CoreReceiver implements Consumer {

    private static final Logger LOG = LoggerFactory.getLogger(CoreReceiver.class);

    protected final MessageQueue queue;
    protected Connection connection;
    private CoreMessageSerializer serializer;
    private Channel channel;
    private CoreMessageHandler handler;

    public CoreReceiver(MessageQueue queue) {
        this.queue = queue;
        this.serializer = new CoreMessageXmlSerializer();
    }

    public void connect(CoreMessageHandler handler) {
        this.handler = handler;
        try {
            this.connection = CoreConnection.coreConnection(CoreConfig.getConfig());
            if (connection != null) {
                this.channel = connection.createChannel();
            }
            bindToQueue(getQueueName(queue), channel);
            LOG.info("Successfully bound to queue or topic");
        }
        catch (IOException e) {
            LOG.error("Error creating channel and connection", e);
        }
    }

    public void close() {
        LOG.info("Closing receiver channel and connection");
        try {
            if (channel != null) {
                channel.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        catch (TimeoutException | IOException e) {
            LOG.error("Error closing channel and connection", e);
        }
    }

    protected void bindToQueue(String queueName, Channel channel) {
        LOG.info("Binding to queue " + queueName);
        try {
            channel.exchangeDeclare(MessageQueue.DIRECT_EXCHANGE, BuiltinExchangeType.DIRECT, true, true, null);
            queueName = channel.queueDeclare(queueName, true, false, true, null).getQueue();
            channel.queueBind(queueName, MessageQueue.DIRECT_EXCHANGE, queueName);
            channel.basicConsume(queueName, true, this);
        }
        catch (IOException e) {
            LOG.error("Error binding to queue", e);
        }
    }

    protected String getQueueName(MessageQueue queue) {
        return queue.getQueueName();
    }

    @Override
    public void handleConsumeOk(String consumerTag) {
        LOG.info("Consumer Connected: {}", consumerTag);
    }

    @Override
    public void handleCancelOk(String consumerTag) {
        LOG.info("Consumer Cancelled: {}", consumerTag);
    }

    @Override
    public void handleCancel(String consumerTag) {
        LOG.warn("Consumer Cancelled Unexpectedly: {}", consumerTag);
    }

    @Override
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
        LOG.warn("Connection or Channel Shutdown: {} - {}", consumerTag, sig.getReason());
    }

    @Override
    public void handleRecoverOk(String consumerTag) {
        LOG.info("Connection or Channel Recovered: {}", consumerTag);
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws
                                                                                                                    IOException {
        String msgText = new String(body);

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
}
