package com.kafka.connect.rabbitmq;

import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.kafkaconnector.CamelSinkConnector;
import org.apache.camel.kafkaconnector.CamelSinkConnectorConfig;
import org.apache.camel.kafkaconnector.utils.CamelMainSupport;
import org.apache.camel.kafkaconnector.utils.TaskHelper;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CamelRabbitmqSinkTask extends SinkTask {

    public static final String KAFKA_RECORD_KEY_HEADER = "camel.kafka.connector.record.key";

    private static final Logger LOG = LoggerFactory.getLogger(CamelRabbitmqSinkTask.class);

    private static final String LOCAL_URL = "direct:start";
    private static final String HEADER_CAMEL_PREFIX = "CamelHeader";
    private static final String PROPERTY_CAMEL_PREFIX = "CamelProperty";

    private CamelMainSupport cms;
    private ProducerTemplate producer;
    private CamelSinkConnectorConfig config;
    private Map<String, String> actualProps;

    @Override
    public String version() {
        return new CamelSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            LOG.info("Starting CamelSinkTask connector task - ");
            actualProps = TaskHelper.mergeProperties(getDefaultConfig(), props);
            config = getCamelSinkConnectorConfig(actualProps);

            String remoteUrl = config.getString(CamelSinkConnectorConfig.CAMEL_SINK_URL_CONF);
            LOG.info("Remote URL - " + remoteUrl);

            CamelContext camelContext = new DefaultCamelContext();
            cms = new CamelMainSupport(actualProps, LOCAL_URL, remoteUrl, null, null, camelContext);
            producer = cms.createProducerTemplate();
            LOG.info("producer"+producer.getCamelContext().toString());
            cms.start();
            LOG.info("CamelSinkTask connector task started - ");
            producer.getCamelContext().getRoutes().forEach(r -> LOG.info("route at context: " + r.toString()));
        } catch (Exception e) {
            throw new ConnectException("Failed to create and start Camel context", e);
        }
    }


    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord record : sinkRecords) {
            ObjectMapper mapper = new ObjectMapper();
            LOG.info("record: " + record.value());
            try {
                Output out = mapper.readValue(record.value().toString(), Output.class);
//                CamelContext camelContext1 = new DefaultCamelContext();
//                CamelMainSupport cms1 = new CamelMainSupport(actualProps, LOCAL_URL, "rabbitmq:"+record.value()+"?hostname=localhost&skipExchangeDeclare=true", null, null, camelContext1);
//                ProducerTemplate producer1 = cms.createProducerTemplate();
//                cms1.start();
                configureRoute(record, out.getKey());
                Map<String, Object> headers = new HashMap<String, Object>();
                Exchange exchange = new DefaultExchange(producer.getCamelContext());
                headers.put(KAFKA_RECORD_KEY_HEADER, record.key());
                LOG.info("Header key:" + record.key());
                exchange.getMessage().setHeaders(headers);
                exchange.getMessage().setBody(out.getValue());
                LOG.info("Sending {} to {}", exchange, LOCAL_URL);
                producer.send(LOCAL_URL, exchange);
                //cms.stop();
            } catch (Exception e) {
                throw new ConnectException("Failed to create and start Camel context", e);
            }

        }
    }

    private void configureRoute(SinkRecord record, String exchange) throws Exception {
        producer.getCamelContext().getRoutes().forEach(r -> {
            if (r.getId() != null) {
                try {
                    producer.getCamelContext().stop();
                    producer.getCamelContext().removeRoute(r.getRouteId());
                    LOG.info("Removed route");
                } catch (Exception e) {
                    LOG.info("Removing reoute error");
                }
            }
        });
        producer.getCamelContext().addRoutes(new RouteBuilder() {
            public void configure() {
                from(LOCAL_URL)
                        .to("rabbitmq://localhost:5672/"+exchange);
            }
        });
        producer.getCamelContext().start();
        producer = cms.createProducerTemplate();
    }

    CamelSinkConnectorConfig getCamelSinkConnectorConfig(
            Map<String, String> props) {
        LOG.info("Properties : "+ props);
        return new CamelRabbitmqSinkConnectorConfig(props);
    }

    Map<String, String> getDefaultConfig() {
        return new HashMap<String, String>() {{
            put(CamelSinkConnectorConfig.CAMEL_SINK_COMPONENT_CONF, "rabbitmq");
        }};
    }

    @Override
    public void stop() {
        LOG.info("Stopping CamelSinkTask connector task - ");
        try {
            cms.stop();
        } catch (Exception e) {
            throw new ConnectException("Failed to stop Camel context", e);
        } finally {
            LOG.info("CamelSinkTask connector task stopped");
        }
    }


    public CamelMainSupport getCms() {
        return cms;
    }
}