package com.kafka.connect.rabbitmq;

import org.apache.camel.builder.RouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleRouteBuilder extends RouteBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleRouteBuilder.class);

    @Override
    public void configure() throws Exception {
        LOG.info("Overiding Route");
        from("direct:start")
                .to("rabbitmq://localhost:5672/oxygenLL");
//                .dynamicRouter(method(MyDynamicRouter.class,
//                        "route"));
    }
}
