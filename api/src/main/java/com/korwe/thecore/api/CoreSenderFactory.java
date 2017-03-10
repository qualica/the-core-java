package com.korwe.thecore.api;

import org.apache.qpid.client.AMQConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author <a href="mailto:dario.matonicki@korwe.com>Dario Matonicki</a>
 */
public class CoreSenderFactory {

    private static final Logger LOG = LoggerFactory.getLogger(CoreSenderFactory.class);

    private Map<String, Connection> connections = new ConcurrentHashMap<>();

    public  CoreSender createCoreSender(MessageQueue queue,
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
            catch (JMSException e) {
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
                CoreConfig config = CoreConfig.getConfig();
                LOG.info("Connecting to queue server " + config.getSetting("amqp_server"));
                Connection newConnection = new AMQConnection(config.getSetting("amqp_server"),
                                                             config.getIntSetting("amqp_port"),
                                                             config.getSetting("amqp_user"),
                                                             config.getSetting("amqp_password"),
                                                             null,
                                                             config.getSetting("amqp_vhost"));
                LOG.info("Connected");

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
