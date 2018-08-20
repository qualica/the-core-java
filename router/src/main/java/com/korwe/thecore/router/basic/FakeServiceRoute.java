package com.korwe.thecore.router.basic;

import com.korwe.thecore.api.MessageQueue;
import com.korwe.thecore.router.AmqpUriPart;
import org.apache.camel.spring.SpringRouteBuilder;
import org.springframework.stereotype.Component;

/**
 * @author <a href="mailto:nithia.govender@korwe.com>Nithia Govender</a>
 */
@Component
public class FakeServiceRoute extends SpringRouteBuilder {

    private static final String SERVICE_NAME = "SyndicationService";

    @Override
    public void configure() throws Exception {
        from(String.format("rabbitmq://%s?exchangeType=direct&declare=false&queue=%s&%s", MessageQueue.DIRECT_EXCHANGE,
                MessageQueue.ClientToCore.getQueueName(), AmqpUriPart.Options.getValue()))
                .to(String.format("rabbitmq://%s?exchangeType=direct&declare=false&queue=%s&%s", MessageQueue.DIRECT_EXCHANGE,
                                  MessageQueue.ServiceToCore.getQueueName(), AmqpUriPart.Options.getValue()));
    }
}
