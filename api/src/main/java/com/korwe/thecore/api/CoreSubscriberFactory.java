package com.korwe.thecore.api;

import com.rabbitmq.client.*;
import org.slf4j.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Factory to create {@link CoreSubscriber CoreSubscriber} using provided
 * {@link CoreConnectionFactory CoreConnectionFactory}
 * @author <a href="mailto:walker.fraser@gmail.com>Fraser Walker</a>
 * @see CoreConnectionFactory
 * @see ConnectionFactory
 * @since 2.1.0-b1
 */
public class CoreSubscriberFactory {

    private static final Logger LOG = LoggerFactory.getLogger(CoreSubscriberFactory.class);

    private Map<String, Connection> connections = new ConcurrentHashMap<>();

    private CoreConnectionFactory coreConnectionFactory;

    /**
     * @param coreConnectionFactory The {@link CoreConnectionFactory CoreConnectionFactory} used to create a
     *                              {@link Connection Connection} used by a {@link CoreSender CoreSender}
     * @since 2.1.0-b1
     */
    public CoreSubscriberFactory(CoreConnectionFactory coreConnectionFactory) {
        this.coreConnectionFactory = coreConnectionFactory;
    }

    public CoreSubscriber createCoreSubscriber(MessageQueue queue,
                                               String serviceName) {
        return new CoreSubscriber(coreConnectionFactory, queue, serviceName);
    }

}
