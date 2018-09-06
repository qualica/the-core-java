package com.korwe.thecore.api;

import com.rabbitmq.client.*;
import org.slf4j.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author <a href="mailto:dario.matonicki@korwe.com>Dario Matonicki</a>
 */
public class CoreSenderFactory {

    private static final Logger LOG = LoggerFactory.getLogger(CoreSenderFactory.class);

    private Map<String, Connection> connections = new ConcurrentHashMap<>();

    public CoreSender createCoreSender(MessageQueue queue,
                                       CoreSenderConnectionType senderConnectionType,
                                       String serviceName) {
        CoreSender coreSender = null;
        switch (senderConnectionType) {
            case NewConnection:
                coreSender = new CoreSender(queue);
                break;
            case SharedConnection:
                Connection connection = getConnection(serviceName);
                coreSender = new CoreConnectionSharingSender(queue, connection);
                break;
        }
        return coreSender;
    }

    public void close(String serviceName) {
        Connection connection = getConnection(serviceName);
        if (connection != null) {
            try {
                connection.close();
            }
            catch (IOException e) {
                LOG.warn("Error closing connection", e);
            }
        }
    }

    private synchronized Connection getConnection(String serviceName) {
        Connection connection;
        if (connections.containsKey(serviceName)) {
            connection = connections.get(serviceName);
        }
        else {
            try {
                Connection newConnection = CoreConnection.coreConnection(CoreConfig.getConfig());
                connections.put(serviceName, newConnection);
                connection = newConnection;
            }
            catch (Exception e) {
                LOG.error("Connection failed", e);
                throw new RuntimeException(e);
            }
        }

        return connection;
    }
}
