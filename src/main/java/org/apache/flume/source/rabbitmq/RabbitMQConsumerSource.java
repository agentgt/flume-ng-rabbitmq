package org.apache.flume.source.rabbitmq;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.RabbitMQConstants;
import org.apache.flume.RabbitMQUtil;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;


public class RabbitMQConsumerSource extends AbstractSource implements EventDrivenSource, Configurable {

	private static final Logger log = LoggerFactory.getLogger(RabbitMQConsumerSource.class);
	
	private CounterGroup _CounterGroup = new CounterGroup();
	private ConnectionFactory _ConnectionFactory;
//    private Connection _Connection;
//    private Channel _Channel;
    private String _QueueName;
//    private String _ExchangeName;
//    private String[] _Topics;
    private MyConsumer consumer;
    
    @Override
    public void configure(Context context) {
        _ConnectionFactory = RabbitMQUtil.getFactory(context);        
        _QueueName = RabbitMQUtil.getQueueName(context);  
//        _ExchangeName = RabbitMQUtil.getExchangeName(context);
//        _Topics = RabbitMQUtil.getTopics(context);
        
        ensureConfigCompleteness( context );
    }
    

    @Override
    public synchronized void start() {
    	init();
    }


	private void init() {
		try {
			Connection connection = _ConnectionFactory.newConnection();
			Channel channel = connection.createChannel();
            if(log.isInfoEnabled())
            	log.info(this.getName() + " - Opening connection to " + _ConnectionFactory.getHost() + ":" + _ConnectionFactory.getPort());
			consumer = new MyConsumer(connection,channel,_QueueName) {
				@Override
				public void handleEvent(Event event) {
					getChannelProcessor().processEvent(event);
					_CounterGroup.incrementAndGet(RabbitMQConstants.COUNTER_ACK);
				}
			};
		} catch (IOException e) {
			log.error("Failure in RabbitMQ connection", e);
			throw new RuntimeException(e);
		}
	}

    @Override
    public synchronized void stop() {
    	if (consumer != null)
    		RabbitMQUtil.close(consumer.getConnection(), consumer.getChannel());      
        super.stop();
    }
    
    private abstract static class MyConsumer {
    	private final String queueName;
    	private final Connection connection;
    	private final Channel channel;
		public MyConsumer(final Connection connection, final Channel channel, final String queueName) throws IOException {
			super();
			this.connection = connection;
			this.channel = channel;
			this.queueName = queueName;
			channel.basicConsume(queueName, false, new DefaultConsumer(channel)  {
				@Override
				public void handleDelivery(
						String consumerTag, 
						Envelope envelope, 
						BasicProperties properties,
						byte[] body) throws IOException {
					Map<String, String> props = RabbitMQUtil.getHeaders(properties);
					Event event = new SimpleEvent();
					event.setBody(body);
					event.setHeaders(props);
					handleEvent(event);
					long tag = envelope.getDeliveryTag();
					channel.basicAck(tag, false);
				}
			});
			
		}
		
		public abstract void handleEvent(Event event);

		
		@SuppressWarnings("unused")
		public String getQueueName() {
			return queueName;
		}

		public Connection getConnection() {
			return connection;
		}

		
		public Channel getChannel() {
			return channel;
		}
    	
    }
    
    
    /**
     * Verify that the required configuration is set
     * 
     * @param context
     */
    private void ensureConfigCompleteness( Context context ) {
    	
    	if( StringUtils.isEmpty(context.getString( RabbitMQConstants.CONFIG_EXCHANGENAME ) ) &&
    			StringUtils.isEmpty( context.getString( RabbitMQConstants.CONFIG_QUEUENAME ) ) ) {

    		throw new IllegalArgumentException( "You must configure at least one of queue name or exchange name parameters" );
    	}
    }
	

}
