package twitter.buzz.storm.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
 * 単語の出現回数を計算するBolt.
 * @author fuji-151a
 *
 */
public class WordCountBolt extends BaseRichBolt {

    /** collector. */
    private OutputCollector collector;

    /** Key:Word Value:CountのMap. */
    private Map<String, Integer> counters;

    @Override
    public final void prepare(final Map stormConf,
            final TopologyContext context,
            final OutputCollector collector) {
        this.counters = new HashMap<String, Integer>();
        this.collector = collector;
    }

    @Override
    public final void execute(final Tuple tuple) {
        String word = tuple.getStringByField("word");
        Integer cnt = counters.get(word);
        if (cnt == null) {
            cnt = 0;
        }
        cnt++;
        counters.put(word, cnt);
        collector.emit(new Values(word, cnt));
        collector.ack(tuple);
    }

    @Override
    public final void cleanup() {
        for (Map.Entry<String, Integer> entry:counters.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }

    @Override
    public final void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "cnt"));
    }

}
