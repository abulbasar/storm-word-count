package spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.ConsumerListener;

public final class KafkaConsumerSpout extends BaseRichSpout {
    private static final long serialVersionUID = -3830075232094686690L;
    private SpoutOutputCollector _collector;
    private ConsumerListener _consumerListener;
    private String topic = "demo";

	@Override
	public final void open(final Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
		_collector = collector;		
		_consumerListener = new ConsumerListener();
		System.out.println("Consuming messages from topic: " + topic);
	}

	@Override
	public final void nextTuple() {
		String msg = null;
        try {
        	LinkedBlockingQueue<String> queue = _consumerListener.consume(topic);            
			while ((msg = queue.take()) != null) {
				_collector.emit(new Values(msg));
				System.out.println(msg);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public final void ack(final Object id) {
	}

	@Override
	public final void fail(final Object id) {
	}

	@Override
	public final void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}





}