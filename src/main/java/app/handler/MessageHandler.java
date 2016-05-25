package app.handler;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.util.UDPTraceBrokerPlugin;
import org.apache.activemq.broker.view.ConnectionDotFilePlugin;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.network.NetworkConnector;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

/**
 * Обработчик сообщений
 * 1. На входе сообщение, полученное из очереди
 * 2. Создаём networkConnector для связи с другим брокером ActiveMQ
 * 3. Отправляем сообщение другому брокеру
 */
public class MessageHandler implements Processor {

  private Logger log = LoggerFactory.getLogger(MessageHandler.class);

  /**
   * Связывает 2 брокера
   * @param from брокер отправляющий сообщения
   * @param to брокер принимающий сообщения
   * @param queue название очереди
   * @return {@link org.apache.activemq.network.NetworkConnector}
   * @throws Exception
   */
  private NetworkConnector bridge(BrokerService from, BrokerService to, String queue) throws Exception {
    TransportConnector toConnector = to.getTransportConnectors().get(0);
    NetworkConnector bridge = from.addNetworkConnector("static://" + toConnector.getPublishableConnectString());
    bridge.addStaticallyIncludedDestination(
        ActiveMQDestination.createDestination(queue, ActiveMQDestination.QUEUE_TYPE)
    );
    return bridge;
  }

  /**
   * Создаёт {@link org.apache.activemq.network.NetworkConnector} до другого брокера
   * @param localBroker локальный брокер
   * @param remoteBrokerAddress адрес удалённого брокера, например "tcp://somehost:61616"
   * @param queue название очереди
   * @param networkTTL время жизни пакета
   * @return {@link org.apache.activemq.network.NetworkConnector}
   * @throws Exception
   */
  private NetworkConnector bridge(BrokerService localBroker,
                                           String remoteBrokerAddress,
                                           String queue,
                                           int networkTTL) throws Exception {
    NetworkConnector connector = localBroker.addNetworkConnector("static://"+remoteBrokerAddress);
    connector.addStaticallyIncludedDestination(
        ActiveMQDestination.createDestination(queue, ActiveMQDestination.QUEUE_TYPE)
    );
    connector.setNetworkTTL(networkTTL);
    return connector;
  }

  @Override
  public void process(Exchange exchange) throws Exception {
    String message = (String) exchange.getIn().getBody();
    log.info("Получено сообщение из очереди: " + message);

    BrokerService broker = new BrokerService();
    broker.setBrokerName("otu_broker");
    broker.setPersistent(false);
    broker.setUseJmx(true);
    broker.setPlugins(new BrokerPlugin[] {new ConnectionDotFilePlugin(), new UDPTraceBrokerPlugin()});
    TransportConnector tcpConnector = broker.addConnector("tcp://localhost:61696");
    bridge(broker, "tcp://localhost:61646", "PKRBP_INQUEUE", 2);
    broker.start();

    // Create a ConnectionFactory
    ActiveMQConnectionFactory connectionFactory =
        new ActiveMQConnectionFactory(tcpConnector.getConnectUri());

    // Create a Connection
    Connection connection = connectionFactory.createConnection();
    connection.start();

    // Create a Session
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

    // Create the destination Queue
    Destination destination = session.createQueue("PKRBP_INQUEUE");

    // Create a MessageProducer from the Session to the Topic or Queue
    MessageProducer producer = session.createProducer(destination);
    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

    // Create a messages
    TextMessage sentMessage = session.createTextMessage(message);

    // Tell the producer to send the message
    producer.send(sentMessage);

    // Clean up
    producer.close();
    session.close();
    connection.close();
    broker.stop();

  }
}
