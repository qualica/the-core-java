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

import org.apache.qpid.client.AMQTopic;
import org.apache.qpid.framing.AMQShortString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

/**
 * @author <a href="mailto:nithia.govender@korwe.com>Nithia Govender</a>
 */
public class CoreSubscriber extends CoreReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(CoreSubscriber.class);

    private final String filter;

    public CoreSubscriber(MessageQueue queue, String filter) {
        super(queue);
        this.filter = filter;
    }

    @Override
    protected void bindToQueue(String queueName, Session session) throws JMSException {
        LOG.info("Binding to topic " + queueName);

        AMQShortString amqName = AMQShortString.valueOf(queueName);
        Destination destination = new AMQTopic(AMQShortString.valueOf(MessageQueue.TOPIC_EXCHANGE), amqName, amqName);

        MessageConsumer consumer = session.createConsumer(destination);
        consumer.setMessageListener(this);
        connection.start();
    }

    @Override
    protected String getQueueName(MessageQueue queue) {
        return queue.getQueueName() + "." + filter;
    }
}
