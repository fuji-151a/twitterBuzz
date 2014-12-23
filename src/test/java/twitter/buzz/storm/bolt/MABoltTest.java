package twitter.buzz.storm.bolt;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MABoltTest {

    /** MABolt. */
    private MABolt mabolt;

    /** collector. */
    private OutputCollector collector;

    @Before
    public void setUp() throws Exception {
        mabolt = new MABolt();
        collector = mock(OutputCollector.class);
        mabolt.prepare(new HashMap(), mock(TopologyContext.class), collector);
    }

    /**
     * Textに名詞がある場合のテスト.
     */
    @Test
    public void maBoltTest() {
        List<String> data = new ArrayList<String>();
        String text = "日本人として知っておきたい"
                + " \"おいしいお茶の入れ方\" http://t.co/wAwUGH93KB";
        String screenName = "jyosi_up_1";
        String timestampMs = "1415059203664";
        String expectedNode1 = "日本人";
        String expectedNode2 = "お茶";
        Tuple tuple = mock(Tuple.class);
        when(tuple.getStringByField("timestamp_ms")).thenReturn(timestampMs);
        when(tuple.getStringByField("text")).thenReturn(text);
        when(tuple.getStringByField("screenName")).thenReturn(screenName);
        mabolt.execute(tuple);
        ArgumentCaptor<Values> args = ArgumentCaptor.forClass(Values.class);
        verify(collector, times(2)).emit(args.capture());
        List<Values> nodes = args.getAllValues();
        String actualNode1 = nodes.get(0).get(0).toString();
        String actualNode2 = nodes.get(1).get(0).toString();
        assertThat(actualNode1, is(expectedNode1));
        assertThat(actualNode2, is(expectedNode2));
    }

}
