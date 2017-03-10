package com.korwe.thecore.api;

import com.korwe.thecore.messages.CoreMessageXmlSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

/**
 * @author <a href="mailto:dario.matonicki@korwe.com>Dario Matonicki</a>
 */
public class CoreConnectionSharingSender extends CoreSender {

    private static final Logger LOG = LoggerFactory.getLogger(CoreConnectionSharingSender.class);


    protected CoreConnectionSharingSender(MessageQueue queue, Connection sharedConnection) {
        try {
            this.queue = queue;
            this.connection = sharedConnection;

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            serializer = new CoreMessageXmlSerializer();
        }
        catch (JMSException e) {
            LOG.error("Error creating sender", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        LOG.info("Closing sender session only");
        try {
            session.close();
        }
        catch (JMSException e) {
            LOG.warn("Error closing session", e);
        }
    }
}
