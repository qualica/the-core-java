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

import brave.Span;
import brave.Tracing;
import brave.internal.HexCodec;
import brave.propagation.TraceContext;
import com.korwe.thecore.messages.CoreMessage;
import com.korwe.thecore.messages.CoreMessageSerializer;
import com.korwe.thecore.messages.CoreMessageXmlSerializer;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * @author <a href="mailto:nithia.govender@korwe.com>Nithia Govender</a>
 */
public class CoreSender implements ShutdownListener, RecoveryListener {

    private static final Logger LOG = LoggerFactory.getLogger(CoreSender.class);

    /**
     * 128 or 64-bit trace ID lower-hex encoded into 32 or 16 characters (required)
     */
    static final String TRACE_ID_NAME = "X-B3-TraceId";
    /**
     * 64-bit span ID lower-hex encoded into 16 characters (required)
     */
    static final String SPAN_ID_NAME = "X-B3-SpanId";
    /**
     * "1" means report this span to the tracing system, "0" means do not. (absent means defer the
     * decision to the receiver of this header).
     */
    static final String SAMPLED_NAME = "X-B3-Sampled";

    protected MessageQueue queue;

    protected Connection connection;
    protected CoreMessageSerializer serializer;
    protected Channel channel;

    protected CoreSender() {
        this.serializer = new CoreMessageXmlSerializer();
    }

    /**
     * @param queue             The message queue constant settings to use when declaring and binding exchanges and
     *                          queues with routing information
     */
    public CoreSender(MessageQueue queue) {
        this(new CoreConnectionFactory(), queue);
    }

    /**
     * @param coreConnectionFactory The {@link CoreConnectionFactory CoreConnectionFactory} used to
     *                          create the connection
     * @param queue             The message queue constant settings to use when declaring and binding exchanges and
     *                          queues with routing information
     */
    public CoreSender(CoreConnectionFactory coreConnectionFactory, MessageQueue queue) {
        this();
        try {
            this.connection = coreConnectionFactory.newConnection();
            initialise(queue);
        }
        catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param queue The message queue constant settings to use when declaring and binding exchanges and
     *              queues with routing information
     */
    protected void initialise(MessageQueue queue) {
        this.queue = queue;
        try {
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

            TraceContext traceContext = null;
            Tracing tracing = Tracing.current();

            if (tracing != null) {
                Span currentSpan = tracing.tracer().currentSpan();
                if (currentSpan != null) {
                    traceContext = currentSpan.context();
                }
            }


            String serialized = serializer.serialize(message);
            Map<String, Object> messagePropertyBag = new HashMap<>();
            messagePropertyBag.put("sessionId", message.getSessionId());
            messagePropertyBag.put("guid", message.getGuid());
            messagePropertyBag.put("choreography", message.getChoreography());
            messagePropertyBag.put("messageType", message.getMessageType().name());

            if (traceContext != null) {
                messagePropertyBag.put(TRACE_ID_NAME, getTraceId(traceContext));
                messagePropertyBag.put(SPAN_ID_NAME, getSpanId(traceContext));
                messagePropertyBag.put(SAMPLED_NAME, getSampled(traceContext));
            }

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

    private String getSampled(TraceContext traceContext) {
        if (traceContext != null && traceContext.sampled() != null) {
            return traceContext.sampled() ? "1" : "0";
        }
        return null;
    }
    private String getSpanId(TraceContext traceContext) {
        return traceContext != null ? HexCodec.toLowerHex(traceContext.spanId()) : null;
    }
    private String getTraceId(TraceContext traceContext) {
        return traceContext != null ? traceContext.traceIdString() : null;
    }

}
