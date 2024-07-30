package org.sia.logsimulator;


import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Class implementing Kafka Partitioner interface. If the key is an IP address,
 * the partitioner partitions by the lowest number in the address. 
 * 
 */
public class IPPartitioner implements Partitioner {

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster){
		int partition = 0;
		String stringKey = (String) key;
		int offset = stringKey.lastIndexOf('.');
		if (offset > 0)
		{
			partition = Integer.parseInt(stringKey.substring(offset + 1)) % cluster.availablePartitionsForTopic(topic).size();
		}
		return partition;
	}

	@Override
	public void close() {

	}

	@Override
	public void configure(Map<String, ?> map) {

	}
}
