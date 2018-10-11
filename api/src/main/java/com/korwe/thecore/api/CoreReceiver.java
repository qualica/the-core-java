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
import brave.internal.HexCodec;
import brave.propagation.ThreadLocalSpan;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import com.korwe.thecore.messages.CoreMessage;
import com.korwe.thecore.messages.CoreMessageSerializer;
import com.korwe.thecore.messages.CoreMessageXmlSerializer;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * @author <a href="mailto:nithia.govender@korwe.com>Nithia Govender</a>
 */
public class CoreReceiver implements Consumer {

    private static final Logger LOG = LoggerFactory.getLogger(CoreReceiver.class);

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

    protected final MessageQueue queue;
    private Connection connection;
    private CoreMessageSerializer serializer;
    private Channel channel;
    private CoreMessageHandler handler;


    private ThreadLocalSpan threadLocalSpan;

    protected CoreReceiver(MessageQueue queue) {
        this.queue = queue;
        this.serializer = new CoreMessageXmlSerializer();
        this.threadLocalSpan = ThreadLocalSpan.CURRENT_TRACER;
    }

    public CoreReceiver(CoreConnectionFactory coreConnectionFactory, MessageQueue queue) {
        this(queue);
        initialiseConnection(coreConnectionFactory, queue);
    }

    protected void initialiseConnection(CoreConnectionFactory coreConnectionFactory, MessageQueue queue) {
        try {
            this.connection = coreConnectionFactory.newConnection();
            if (connection != null) {
                this.channel = connection.createChannel();
                bindToQueue(getQueueName(queue), channel);
            }
        }
        catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public void connect(CoreMessageHandler handler) {
        this.handler = handler;
        try {
            channel.basicConsume(getQueueName(queue), true, this);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
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
            channel.queueDeclare(queueName, true, false, true, null).getQueue();
            channel.queueBind(queueName, MessageQueue.DIRECT_EXCHANGE, queueName);
            LOG.info("Successfully bound to queue");
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
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {

        Span span = extractB3Headers(properties.getHeaders());

        if (span != null) {
            span.start();
        }

        handleMessage(body);

        if (span != null) {
            span.finish();
            threadLocalSpan.remove();
        }

    }

    private void handleMessage(byte[] body) {
        String msgText = new String(body);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received: " + msgText);
        }
        else {
            int endIndex = msgText.indexOf("</function");
            LOG.info("Received: " + msgText.substring(0, endIndex > 0 ? endIndex : Math.min(350, msgText.length())));
        }
        CoreMessage message = serializer.deserialize(msgText);
        if (handler != null) {
            handler.handleMessage(message);
        }
    }

    private Span extractB3Headers(Map<String, Object> headers) {

        String traceId = headers.get(TRACE_ID_NAME) != null ? headers.get(TRACE_ID_NAME).toString() : null;
        String spanId = headers.get(SPAN_ID_NAME) != null ? headers.get(SPAN_ID_NAME).toString() : null;
        String sampled = headers.get(SAMPLED_NAME) != null ? headers.get(SAMPLED_NAME).toString() : null;

        if (traceId != null) {

            TraceContext.Builder builder = TraceContext.newBuilder()
                    .traceId(HexCodec.lowerHexToUnsignedLong(traceId))
                    .spanId(HexCodec.lowerHexToUnsignedLong(spanId))
                    .sampled(sampled != null && sampled.equals("1"));

            TraceContext traceContext = builder.build();

            TraceContextOrSamplingFlags samplingFlags = TraceContextOrSamplingFlags.create(traceContext);

            Span span = threadLocalSpan.next(samplingFlags);
            return span;
        }

        return null;
    }
}
