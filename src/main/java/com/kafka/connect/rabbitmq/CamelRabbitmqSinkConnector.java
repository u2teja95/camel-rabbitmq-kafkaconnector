package com.kafka.connect.rabbitmq;

import org.apache.camel.kafkaconnector.CamelSinkConnector;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CamelRabbitmqSinkConnector extends SinkConnector {

    private static final Logger LOG = LoggerFactory.getLogger(CamelRabbitmqSinkConnector.class);

    private Map<String, String> configProps;

    @Override
    public String version() {
        return new CamelSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> configProps) {
        LOG.info("Connector rabbit config keys: {}", String.join(", ", configProps.keySet()));
        this.configProps = configProps;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CamelRabbitmqSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        LOG.info("Setting rabbit task configurations for {} workers.", maxTasks);
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            configs.add(configProps);
        }
        return configs;
    }

    @Override
    public void stop() {
        //Nothing to do
    }

    @Override
    public ConfigDef config() {
        LOG.info("config "+ CamelRabbitmqSinkConnectorConfig.conf());
        return CamelRabbitmqSinkConnectorConfig.conf();
    }

}