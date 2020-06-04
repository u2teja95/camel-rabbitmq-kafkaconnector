package com.kafka.connect.rabbitmq;

import java.util.*;

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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
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
            LOG.info("record: " + record.value());
            try {
//            CamelContext camelContext1 = new DefaultCamelContext();
//            CamelMainSupport cms1 = new CamelMainSupport(actualProps, LOCAL_URL, "rabbitmq://localhost:5672/"+record.value(), null, null, camelContext1);
//            ProducerTemplate producer1 = cms.createProducerTemplate();
//            cms1.start();
            Map<String, Object> headers = new HashMap<String, Object>();
            producer.getCamelContext().getRoutes().forEach(r -> {
                if (r.getId() != null) {
                    try {
                        producer.getCamelContext().stop();
                        LOG.info("Stopped: ");
                        boolean removed = producer.getCamelContext().removeRoute(r.getRouteId());
                        LOG.info("Removed: " + removed);
                    } catch (Exception e) {
                        LOG.info("Removing reoute error");
                    }
                }
            }
            );
            producer.getCamelContext().addRoutes(new RouteBuilder() {
                public void configure() {
                    from(LOCAL_URL)
                            .to("rabbitmq://localhost:5672/"+record.value());
                }
            });
            producer.getCamelContext().start();

            Exchange exchange = new DefaultExchange(producer.getCamelContext());
            LOG.info("exhange: " + exchange.getExchangeId() + exchange.getFromRouteId());
            headers.put(KAFKA_RECORD_KEY_HEADER, record.key());
            for (Iterator<Header> iterator = record.headers().iterator(); iterator.hasNext();) {
                Header header = (Header)iterator.next();
                if (header.key().startsWith(HEADER_CAMEL_PREFIX)) {
                    addHeader(headers, header);
                } else if (header.key().startsWith(PROPERTY_CAMEL_PREFIX)) {
                    addProperty(exchange, header);
                }
            }
            LOG.info("headers: " + headers);
            exchange.getMessage().setHeaders(headers);
            exchange.getMessage().setBody(record.value());
            LOG.info("Sending {} to {}", exchange, LOCAL_URL);
            producer.send(LOCAL_URL, exchange);
            //cms1.stop();
        } catch (Exception e) {
            throw new ConnectException("Failed to create and start Camel context", e);
        }

        }
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

    private void addHeader(Map<String, Object> map, Header singleHeader) {
        Schema schema = singleHeader.schema();
        if (schema.type().getName().equals(Schema.STRING_SCHEMA.type().getName())) {
            map.put(singleHeader.key(), (String)singleHeader.value());
        } else if (schema.type().getName().equalsIgnoreCase(Schema.BOOLEAN_SCHEMA.type().getName())) {
            map.put(singleHeader.key(), (Boolean)singleHeader.value());
        } else if (schema.type().getName().equalsIgnoreCase(Schema.INT32_SCHEMA.type().getName())) {
            map.put(singleHeader.key(), singleHeader.value());
        } else if (schema.type().getName().equalsIgnoreCase(Schema.BYTES_SCHEMA.type().getName())) {
            map.put(singleHeader.key(), (byte[])singleHeader.value());
        } else if (schema.type().getName().equalsIgnoreCase(Schema.FLOAT32_SCHEMA.type().getName())) {
            map.put(singleHeader.key(), (float)singleHeader.value());
        } else if (schema.type().getName().equalsIgnoreCase(Schema.FLOAT64_SCHEMA.type().getName())) {
            map.put(singleHeader.key(), (double)singleHeader.value());
        } else if (schema.type().getName().equalsIgnoreCase(Schema.INT16_SCHEMA.type().getName())) {
            map.put(singleHeader.key(), (short)singleHeader.value());
        } else if (schema.type().getName().equalsIgnoreCase(Schema.INT64_SCHEMA.type().getName())) {
            map.put(singleHeader.key(), (long)singleHeader.value());
        } else if (schema.type().getName().equalsIgnoreCase(Schema.INT8_SCHEMA.type().getName())) {
            map.put(singleHeader.key(), (byte)singleHeader.value());
        } else if (schema.type().getName().equalsIgnoreCase(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).type().getName())) {
            map.put(singleHeader.key(), (Map<?, ?>)singleHeader.value());
        } else if (schema.type().getName().equalsIgnoreCase(SchemaBuilder.array(Schema.STRING_SCHEMA).type().getName())) {
            map.put(singleHeader.key(), (List<?>)singleHeader.value());
        }
    }

    private void addProperty(Exchange exchange, Header singleHeader) {
        Schema schema = singleHeader.schema();
        if (schema.type().getName().equals(Schema.STRING_SCHEMA.type().getName())) {
            exchange.getProperties().put(singleHeader.key(), (String)singleHeader.value());
        } else if (schema.type().getName().equalsIgnoreCase(Schema.BOOLEAN_SCHEMA.type().getName())) {
            exchange.getProperties().put(singleHeader.key(), (Boolean)singleHeader.value());
        } else if (schema.type().getName().equalsIgnoreCase(Schema.INT32_SCHEMA.type().getName())) {
            exchange.getProperties().put(singleHeader.key(), singleHeader.value());
        } else if (schema.type().getName().equalsIgnoreCase(Schema.BYTES_SCHEMA.type().getName())) {
            exchange.getProperties().put(singleHeader.key(), (byte[])singleHeader.value());
        } else if (schema.type().getName().equalsIgnoreCase(Schema.FLOAT32_SCHEMA.type().getName())) {
            exchange.getProperties().put(singleHeader.key(), (float)singleHeader.value());
        } else if (schema.type().getName().equalsIgnoreCase(Schema.FLOAT64_SCHEMA.type().getName())) {
            exchange.getProperties().put(singleHeader.key(), (double)singleHeader.value());
        } else if (schema.type().getName().equalsIgnoreCase(Schema.INT16_SCHEMA.type().getName())) {
            exchange.getProperties().put(singleHeader.key(), (short)singleHeader.value());
        } else if (schema.type().getName().equalsIgnoreCase(Schema.INT64_SCHEMA.type().getName())) {
            exchange.getProperties().put(singleHeader.key(), (long)singleHeader.value());
        } else if (schema.type().getName().equalsIgnoreCase(Schema.INT8_SCHEMA.type().getName())) {
            exchange.getProperties().put(singleHeader.key(), (byte)singleHeader.value());
        } else if (schema.type().getName().equalsIgnoreCase(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).type().getName())) {
            exchange.getProperties().put(singleHeader.key(), (Map<?, ?>)singleHeader.value());
        } else if (schema.type().getName().equalsIgnoreCase(SchemaBuilder.array(Schema.STRING_SCHEMA).type().getName())) {
            exchange.getProperties().put(singleHeader.key(), (List<?>)singleHeader.value());
        }
    }

    public CamelMainSupport getCms() {
        return cms;
    }
}