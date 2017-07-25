package topologies;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spouts.KafkaConsumerSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import bolts.SplitSentenceBolt;
import bolts.WordCountBolt;


public final class KafkaWordCountTopology {
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaWordCountTopology.class);
	private static final String TOPOLOGY_NAME = "KafkaWordCount";
	
	public static final void main(final String[] args) {
		
		boolean localMode = true;
		
	
		try {
			final Config config = new Config();
			config.setMessageTimeoutSecs(120);
			config.setDebug(false);

			final TopologyBuilder topologyBuilder = new TopologyBuilder();
			
			topologyBuilder.setSpout("kafkaSpout", new KafkaConsumerSpout());
			topologyBuilder.setBolt("splitsentencebolt", new SplitSentenceBolt()).shuffleGrouping("kafkaSpout");
			topologyBuilder.setBolt("wordcountbolt", new WordCountBolt()).fieldsGrouping("splitsentencebolt", new Fields("word"));

			if(localMode){
				//Submit to local mode
				config.setMaxTaskParallelism(10);
				final LocalCluster localCluster = new LocalCluster();
				localCluster.submitTopology(TOPOLOGY_NAME, config, topologyBuilder.createTopology());
				//Run this topology for 120 seconds so that we can complete processing of decent # of listings.
				Utils.sleep(120 * 1000);

				LOGGER.info("Shutting down the cluster...");
				localCluster.killTopology(TOPOLOGY_NAME);
				localCluster.shutdown();
			}else{
				//Submit to the cluster mode
				config.setNumWorkers(3);
				StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
			}
			
		} catch (final Exception exception) {
			exception.printStackTrace();
		}
		LOGGER.info("\n\n\n\t\t*****Please clean your temp folder \"{}\" now!!!*****", System.getProperty("java.io.tmpdir"));
	}
}