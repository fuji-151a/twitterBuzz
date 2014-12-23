package twitter.buzz.storm.bolt;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import net.moraleboost.mecab.Lattice;
import net.moraleboost.mecab.Node;
import net.moraleboost.mecab.impl.StandardTagger;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 形態素解析し名詞を抽出するためのBolt.
 * @author yuya
 *
 */
public class MABolt extends BaseRichBolt {

    /** collector. */
    private OutputCollector collector;

    @Override
    public final void prepare(final Map conf,
                        final TopologyContext context,
                        final OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public final void execute(final Tuple tuple) {
        String text = tuple.getStringByField("text");

        // Taggerを構築。
        // 引数には、MeCabのcreateTagger()関数に与える引数を与える。
        StandardTagger tagger = new StandardTagger("");
        // Lattice（形態素解析に必要な実行時情報が格納されるオブジェクト）を構築
        Lattice lattice = tagger.createLattice();
        // 解析対象文字列をセット
        lattice.setSentence(text);
        // tagger.parse()を呼び出して、文字列を形態素解析する。
        tagger.parse(lattice);
        // 一つずつ形態素をたどりながら、表層形と素性を出力
        Node node = lattice.bosNode();

        while (node != null) {
            String surface = node.surface();
            String feature = node.feature();
            String[] hinshi = feature.split(",");
            if (hinshi[0].equals("名詞")
                    && (hinshi[1].equals("一般") || hinshi[1].equals("固有名詞"))
                    && !hinshi[6].equals("*")
                   ) {
                collector.emit(new Values(surface));
            }
            node = node.next();
        }

        // lattice, taggerを破壊
        lattice.destroy();
        tagger.destroy();
        collector.ack(tuple);
    }

    @Override
    public final void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

}
