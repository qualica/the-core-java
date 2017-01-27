package com.korwe.thecore.api;

import com.korwe.thecore.messages.CoreMessageXmlSerializer;
import org.apache.qpid.transport.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:dario.matonicki@korwe.com>Dario Matonicki</a>
 */
public class CoreConnectionSharingSender extends CoreSender {

    private static final Logger LOG = LoggerFactory.getLogger(CoreConnectionSharingSender.class);


    protected CoreConnectionSharingSender(MessageQueue queue, Connection sharedConnection) {
        this.queue = queue;
        this.connection = sharedConnection;

        session = connection.createSession();
        serializer = new CoreMessageXmlSerializer();
    }

    @Override
    public void close() {
        LOG.info("Closing sender session only");
        session.sync();
        session.close();
    }
}
