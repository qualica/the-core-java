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
import com.rabbitmq.client.impl.recovery.*;
import org.slf4j.*;

import java.io.*;
import java.util.concurrent.*;

/**
 * @author <a href="mailto:walker.fraser@gmail.com>Fraser Walker</a>
 */
public class CoreConnection {

    private static final Logger LOG = LoggerFactory.getLogger(CoreConnection.class);

    public static Connection coreConnection(CoreConfig config) {

        int connectionRetries = config.getIntSetting("connection_retries") < 0 ? Integer.MAX_VALUE
                : config.getIntSetting("connection_retries");
        int connectionBackoffMillis = config.getIntSetting("retry_backoff_millis");
        int connectionBackoffMax = config.getIntSetting("retry_backoff_max_millis");

        for (int n = 0; n <= connectionRetries; n++) {
            try {
                LOG.debug("Connection Attempt: {}", n + 1);
                return createConnection(config);
            }
            catch (IOException | TimeoutException e) {
                if (n == connectionRetries) {
                    throw new RuntimeException(e);
                }
                try {
                    Thread.sleep(connectionBackoffMax >= (connectionBackoffMillis + (n * connectionBackoffMillis)) ?
                            connectionBackoffMax : connectionBackoffMillis + (n * connectionBackoffMillis));
                }
                catch (InterruptedException e1) {
                }
            }
        }
        return null;
    }

    private static Connection createConnection(CoreConfig config) throws IOException, TimeoutException {
        return getConnectionFactory(config).newConnection();
    }

    private static ConnectionFactory getConnectionFactory(CoreConfig config) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(config.getSetting("amqp_server"));
        connectionFactory.setPort(config.getIntSetting("amqp_port"));
        connectionFactory.setUsername(config.getSetting("amqp_user"));
        connectionFactory.setPassword(config.getSetting("amqp_password"));
        connectionFactory.setVirtualHost(config.getSetting("amqp_vhost"));
        return connectionFactory;
    }

}
