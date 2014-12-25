package twitter.buzz.storm.topology;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import twitter.buzz.storm.bolt.MABolt;
import twitter.buzz.storm.bolt.ParseBolt;
import twitter.buzz.storm.bolt.WordCountBolt;

/**
 * Main.
 * @author fuji-151a
 *
 */
public class MainTopology {

    /** active time. */
    private static final int UPTIME = 10000;

    /** ZookeeperHosts and port. */
    private String zkHosts;

    /** Kafka Topic Name. */
    private String topic;

    /** Topology Name. */
    private String topologyName;

    /**
     * Constructor.
     * @param confFile : Configuration File Name
     */
    public MainTopology(final String confFile) {
        Properties prop = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(confFile);
            prop.load(fis);
            if (configFileCheck(prop)) {
                zkHosts = prop.getProperty("zkHost");
                topic = prop.getProperty("topic");
                topologyName = prop.getProperty("topologyName");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * Main Method.
     * @param args args[0]:config file, args[1]: local
     */
    public static void main(final String[] args) {
        if (args.length > 1) {
            System.err.println("command> storm jar application.jar Topology"
                    + "CONF_FILE [local]");
            System.exit(1);
        }
        MainTopology mainTopology = new MainTopology(args[0]);
        StormTopology topology = mainTopology.buildTopology();
        if ("local".equals(args[1])) {
            mainTopology.submitTopologyLocal(topology);
        } else {
            mainTopology.submitTopology(topology);
        }
    }

    /**
     * build Topology.
     * @return create StormTopology
     */
    public final StormTopology buildTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        BrokerHosts hosts = new ZkHosts(zkHosts);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic,
                "/" + topic, UUID.randomUUID().toString());
        KafkaSpout spout = new KafkaSpout(spoutConfig);
        builder.setSpout("kafka_spout", spout, 5);
        builder.setBolt("parse_bolt", new ParseBolt())
            .shuffleGrouping("kafka_spout");
        builder.setBolt("ma_bolt", new MABolt())
            .shuffleGrouping("parse_bolt");
        builder.setBolt("wordcnt_bolt", new WordCountBolt())
            .shuffleGrouping("ma_bolt");
        return builder.createTopology();
    }

    /**
     * Local Mode.
     * @param topology Storm Topology
     */
    private void submitTopologyLocal(final StormTopology topology) {
        Config conf = new Config();
        conf.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, topology);
        Utils.sleep(UPTIME);
        cluster.killTopology(topologyName);
        cluster.shutdown();
    }

    /**
     * Cluster Mode.
     * @param topology Storm Topology
     */
    private void submitTopology(final StormTopology topology) {

    }

    /**
     * Check configuration file.
     * @param prop Properties
     * @return boolean
     */
    private boolean configFileCheck(final Properties prop) {
        if (prop.getProperty("topic") != null
                && prop.getProperty("zkHost") != null
                && prop.getProperty("topologyName") != null
           ) {
            return true;
        }
        return false;
    }
}
