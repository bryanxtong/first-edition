package org.sia.webstats;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.*;

/**
 * A thread for running a Kafka consumer, receiving messages and forwarding them to
 * registered LogStatsObservers.
 *
 * Static methods enable LogStatsReceiver to function as a singleton and to register
 * and deregister LogStatsObservers. When the last LogStatsObserver deregisters
 * LogStatsReceiver stops receiving messages and resumes when the first LogStatsObserver
 * registers again.
 *
 */
public class LogStatsReceiver extends Thread
{
	private static LogStatsReceiver instance;
	private static Set<LogStatsObserver> listeners = Collections.synchronizedSet(new HashSet<>());

	private static void startUp()
	{
		System.out.println("LogStatsReceiver startUp. instance: "+instance);
		if(instance == null)
		{
			instance = new LogStatsReceiver();
			instance.start();
		}
	}

	private static void shutdown()
	{
		System.out.println("LogStatsReceiver shutdown. instance: "+instance);
		if(instance != null)
		{
			instance.pleaseStop();
			instance = null;
		}
	}

	public static void addObserver(LogStatsObserver o)
	{
		System.out.println("LogStatsReceiver addObserver "+o);
		if(listeners.add(o))
			startUp();
	}

	public static void removeObserver(LogStatsObserver o)
	{
		System.out.println("LogStatsReceiver removeObserver "+o);
		listeners.remove(o);
		if(listeners.isEmpty())
			shutdown();
	}

	private AtomicBoolean shouldStop = new AtomicBoolean(false);

	public void pleaseStop()
	{
		shouldStop.set(true);
	}

	private Consumer<String,String> consumer;

	public void run()
	{
		try {
			System.out.println("Starting LogStatsReceiver thread");

			/*String zkaddress = System.getProperty("zookeeper.address");

			if(zkaddress == null)
			{
				System.err.println("zookeeper.address property is not set! Exiting.");
				return;
			}*/

			String brokerList = System.getProperty("bootstrap.servers");

			if(brokerList == null)
			{
				System.err.println("bootstrap.server property is not set! Exiting.");
				return;
			}

			String topicName = System.getProperty("kafka.topic");

			if(topicName == null)
			{
				System.err.println("kafka.topic property is not set! Exiting.");
				return;
			}

			System.out.println("LogStatsReceiver params: "+brokerList+", "+topicName);

			Properties props = new Properties();
			props.put("bootstrap.servers", brokerList);
			props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			//props.put("zookeeper.connect", zkaddress);
	        props.put("group.id", "groupname");
	        //props.put("zookeeper.session.timeout.ms", "2400");
	        //props.put("zookeeper.sync.time.ms", "1200");
	        props.put("auto.commit.interval.ms", "1000");

	        System.out.println("LogStatsReceiver getting consumer");

	        try {
	        	consumer = new KafkaConsumer<>(props);
	        }
	        catch(Exception e)
	        {
	        	e.printStackTrace();
	        	listeners.clear();
	        	instance = null;
	        	return;
	        }

	        System.out.println("LogStatsReceiver getting KafkaStream");
			System.out.println("LogStatsReceiver iterating");

			consumer.subscribe(Collections.singletonList(topicName));
	        while (!shouldStop.get())
	        {
				ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(300));
				if(!records.isEmpty()){
					records.forEach(o -> {
						String message = o.value();
						for(LogStatsObserver listener : listeners) {
							listener.onStatsMessage(message);
						}
					});
				}
	        }
	        System.out.println("Stopping LogStatsReceiver.");
	        consumer.close();
	        System.out.println("LogStatsReceiver stopped.");
		}
		catch(Exception e)
		{
			System.err.println("Exception during LogStatsReceiver run: ");
			e.printStackTrace();
		}
	}
}
