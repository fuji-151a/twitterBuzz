package twitter.buzz.storm.bolt;

import java.util.Map;

import twitter4j.internal.org.json.JSONException;
import twitter4j.internal.org.json.JSONObject;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 必要なデータだけfilterするBolt.
 * @author yuya
 *
 */
public class FilterBolt extends BaseRichBolt {

    /** collector. */
    private OutputCollector outputCollector;

    @Override
    public final void prepare(final Map conf,
                        final TopologyContext context,
                        final OutputCollector collector) {
        outputCollector = collector;
    }

    @Override
    public final void execute(final Tuple tuple) {
        try {
            JSONObject jsonObject = new JSONObject(tuple.getString(0));
            JSONObject jsonObjUser = jsonObject.getJSONObject("user");
            String timestampMs = jsonObject.getString("timestamp_ms");
            String text = jsonObject.getString("text");
            String screenName = jsonObjUser.getString("screen_name");
            outputCollector.emit(new Values(timestampMs, text, screenName));
            outputCollector.ack(tuple);
        } catch (JSONException e) {
            e.printStackTrace();
        }

    }

    @Override
    public final void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("data"));
    }

}
