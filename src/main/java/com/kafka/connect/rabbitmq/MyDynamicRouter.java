package com.kafka.connect.rabbitmq;

import org.apache.camel.DynamicRouter;
import org.apache.camel.Exchange;
import org.apache.camel.Header;

import java.util.Map;

public class MyDynamicRouter {

    public String route(String body, @Header(Exchange.SLIP_ENDPOINT) String previous) {
        if (previous == null) {
            // 1st time
            return "rabbitmq://localhost:5672/oxygen";
        } else if ("rabbitmq://localhost:5672/oxygen".equals(previous)) {
            // 2nd time - transform the message body using the simple language
            return "rabbitmq://localhost:5672/oxygenLL";
        } else {
            // no more, so return null to indicate end of dynamic router
            return null;
        }
    }
}

