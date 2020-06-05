: ${DEBUG_SUSPEND_FLAG:='n'}
export KAFKA_DEBUG='y'

set -e

mvn clean package
connect-standalone config/connect-standalone.properties config/CamelRabbitMQSinkConnector.properties