package twitter.buzz.storm.bolt;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
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

/**
 * WordCountBolt Test.
 * @author fuji-151a
 *
 */
public class WordCountBoltTest {

    /** WordCountBolt. */
    private WordCountBolt wcBolt;

    /** collector. */
    private OutputCollector collector;

    @Before
    public void setUp() throws Exception {
        wcBolt = new WordCountBolt();
        collector = mock(OutputCollector.class);
        wcBolt.prepare(new HashMap(), mock(TopologyContext.class), collector);
    }

    @Test
    public void wordCountTest() {
        Tuple tuple = mock(Tuple.class);
        List<String> wordList = new ArrayList<String>();
        String expectedWord1 = "日本";
        String expectedWord2 = "アメリカ";
        wordList.add(expectedWord1);
        wordList.add(expectedWord2);
        wordList.add(expectedWord1);

        // Tupleにこれら2つの単語を流す．
        for (String word:wordList) {
            when(tuple.getStringByField("word")).thenReturn(word);
            wcBolt.execute(tuple);
        }
        ArgumentCaptor<Values> args = ArgumentCaptor.forClass(Values.class);
        verify(collector, times(3)).emit(args.capture());

        // extract actual values
        String actualWord1 = args.getAllValues().get(0).get(0).toString();
        Integer actualCnt1 = (Integer) args.getAllValues().get(0).get(1);
        String actualWord2 = args.getAllValues().get(1).get(0).toString();
        Integer actualCnt2 = (Integer) args.getAllValues().get(1).get(1);
        String actualWord3 = args.getAllValues().get(2).get(0).toString();
        Integer actualCnt3 = (Integer) args.getAllValues().get(2).get(1);

        // assert
        assertThat(actualWord1, is(expectedWord1));
        assertThat(actualCnt1, is(1));
        assertThat(actualWord2, is(expectedWord2));
        assertThat(actualCnt2, is(1));
        assertThat(actualWord3, is(expectedWord1));
        assertThat(actualCnt3, is(2));
    }

}
