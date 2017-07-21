package topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import bolts.SplitSentenceBolt;
import bolts.WordCountBolt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spouts.RandomSentenceSpout;


public final class WordCountTopology {
	private static final Logger LOGGER = LoggerFactory.getLogger(WordCountTopology.class);
	private static final String TOPOLOGY_NAME = "WordCount";

	public static final void main(final String[] args) {
		
		boolean localMode = false;
		
		try {
			final Config config = new Config();
			config.setMessageTimeoutSecs(120);
			config.setDebug(false);

			final TopologyBuilder topologyBuilder = new TopologyBuilder();
			topologyBuilder.setSpout("randomsentencespout", new RandomSentenceSpout());
			topologyBuilder.setBolt("splitsentencebolt", new SplitSentenceBolt())
					.shuffleGrouping("randomsentencespout");
			topologyBuilder.setBolt("wordcountbolt", new WordCountBolt())
					.fieldsGrouping("splitsentencebolt", new Fields("word"));

			if(localMode){
				//Submit to local
				config.setMaxTaskParallelism(10);
				final LocalCluster localCluster = new LocalCluster();
				localCluster.submitTopology(TOPOLOGY_NAME, config, topologyBuilder.createTopology());
				//Run this topology for 120 seconds so that we can complete processing of decent # of listings.
				Utils.sleep(120 * 1000);

				LOGGER.info("Shutting down the cluster...");
				localCluster.killTopology(TOPOLOGY_NAME);
				localCluster.shutdown();
			}else{
				//Submit it to the cluster
				config.setNumWorkers(3);
				StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
			}
		} catch (final Exception exception) {
			exception.printStackTrace();
		}
		LOGGER.info("\n\n\n\t\t*****Please clean your temp folder \"{}\" now!!!*****", System.getProperty("java.io.tmpdir"));
	}
}