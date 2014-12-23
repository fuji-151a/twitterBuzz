package twitter.buzz.storm.bolt;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import twitter.buzz.util.TestEnv;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * FilterBoltのTest.
 * @author yuya
 *
 */
public class ParseBoltTest {

    /** filterBolt. */
    private ParseBolt pbolt;

    /** collector. */
    private OutputCollector collector;

    /** testDataのfileName. */
    private String testDataFileName = "twitterTestData.json";

    /** testDataを格納する変数. */
    private String testData;


    @Before
    public void setUp() throws Exception {
        pbolt = new ParseBolt();
        collector = mock(OutputCollector.class);
        pbolt.prepare(new HashMap(), mock(TopologyContext.class), collector);
        String file = ParseBoltTest.class.getClassLoader()
                        .getResource(testDataFileName).getPath();
        testData = TestEnv.readData(file);
    }

    @Test
    public void parseTest() {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getString(0)).thenReturn(testData);
        pbolt.execute(tuple);
        ArgumentCaptor<Values> args = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(args.capture());
        String timestampMs = args.getValue().get(0).toString();
        String text = args.getValue().get(1).toString();
        String screenName = args.getValue().get(2).toString();
        String expectedTimestampMs = "1415059203664";
        String expectedText = "日本人として知っておきたい"
                + " \"おいしいお茶の入れ方\" http://t.co/wAwUGH93KB";
        String expectedScreenName = "jyosi_up_1";
        assertThat(timestampMs, is(expectedTimestampMs));
        assertThat(text, is(expectedText));
        assertThat(screenName, is(expectedScreenName));
    }

}
