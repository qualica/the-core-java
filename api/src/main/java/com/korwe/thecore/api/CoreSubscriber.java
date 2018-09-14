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

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author <a href="mailto:nithia.govender@korwe.com>Nithia Govender</a>
 */
public class CoreSubscriber extends CoreReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(CoreSubscriber.class);

    private final String filter;

    public CoreSubscriber(MessageQueue queue, String filter) {
        this(new CoreConnectionFactory(), queue, filter);
    }

    public CoreSubscriber(CoreConnectionFactory coreConnectionFactory, MessageQueue queue, String filter) {
        super(queue);
        this.filter = filter;
        initialiseConnection(coreConnectionFactory, queue);
    }

    @Override
    protected void bindToQueue(String queueName, Channel channel) {
        LOG.info("Binding to topic " + queueName);
        try {
            channel.exchangeDeclare(MessageQueue.TOPIC_EXCHANGE, BuiltinExchangeType.TOPIC, true, true, null);
            channel.queueDeclare(queueName, true, false, true, null).getQueue();
            channel.queueBind(queueName, MessageQueue.TOPIC_EXCHANGE, getQueueName(queue));
            LOG.info("Successfully bound to topic");
        }
        catch (IOException e) {
            LOG.error("Error binding to topic", e);
        }
    }

    @Override
    protected String getQueueName(MessageQueue queue) {
        return queue.getQueueName() + "." + filter;
    }
}
