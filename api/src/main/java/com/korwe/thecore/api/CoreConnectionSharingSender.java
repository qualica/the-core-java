package com.korwe.thecore.api;

import com.korwe.thecore.messages.*;
import com.rabbitmq.client.*;
import org.slf4j.*;

import java.io.*;
import java.util.concurrent.*;


/**
 * @author <a href="mailto:dario.matonicki@korwe.com>Dario Matonicki</a>
 */
public class CoreConnectionSharingSender extends CoreSender {

    private static final Logger LOG = LoggerFactory.getLogger(CoreConnectionSharingSender.class);


    protected CoreConnectionSharingSender(MessageQueue queue, Connection sharedConnection) {
        try {
            this.queue = queue;
            this.connection = sharedConnection;
            channel = connection.createChannel();
            serializer = new CoreMessageXmlSerializer();
        } catch (IOException e) {
            LOG.error("Error creating sender", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        LOG.info("Closing sender channel only");
        try {
            channel.close();
        } catch (TimeoutException | IOException e) {
            LOG.warn("Error closing channel", e);
        }
    }
}
