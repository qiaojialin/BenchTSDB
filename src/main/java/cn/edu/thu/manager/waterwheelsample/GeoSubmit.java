package cn.edu.thu.manager.waterwheelsample;

import indexingTopology.bolt.InputStreamReceiverBolt;
import indexingTopology.bolt.InputStreamReceiverBoltServer;
import indexingTopology.bolt.QueryCoordinatorBolt;
import indexingTopology.bolt.QueryCoordinatorWithQueryReceiverServerBolt;
import indexingTopology.common.data.DataSchema;
import indexingTopology.config.TopologyConfig;
import indexingTopology.topology.TopologyGenerator;
import indexingTopology.util.AvailableSocketPool;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;

public class GeoSubmit {

    public static void main(String... args) {

        TopologyConfig config = new TopologyConfig();

        LocalCluster cluster;

        config.dataChunkDir = "./target/tmp";
        config.metadataDir = "./target/tmp";
        config.CHUNK_SIZE = 512 * 1024;
        config.HDFSFlag = false;
        config.previousTime = Integer.MAX_VALUE;
        System.out.println("dataChunkDir is set to " + config.dataChunkDir);
        cluster = new LocalCluster();

        DataSchema schema = new DataSchema();
        schema.addLongField("id");
        schema.addDoubleField("lon");
        schema.addDoubleField("lat");
        schema.addDoubleField("alt");
        schema.addLongField("timestamp");
        schema.setTemporalField("timestamp");
        schema.setPrimaryIndexField("id");

        final long minIndex = 0L;
        final long maxIndex = Long.MAX_VALUE;

        AvailableSocketPool socketPool = new AvailableSocketPool();
        int ingestionPort = socketPool.getAvailablePort();
        int queryPort = socketPool.getAvailablePort();

        TopologyGenerator<Long> topologyGenerator = new TopologyGenerator<>();

        InputStreamReceiverBolt inputStreamReceiverBolt = new InputStreamReceiverBoltServer(schema, ingestionPort, config);
        QueryCoordinatorBolt<Long> coordinator = new QueryCoordinatorWithQueryReceiverServerBolt<>(minIndex, maxIndex, queryPort,
                config, schema);

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, minIndex, maxIndex, false, inputStreamReceiverBolt,
                coordinator, config);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

        cluster.submitTopology("testSimpleTopologyKeyRangeQuery", conf, topology);

        System.out.println("ingestionPort: " + ingestionPort);
        System.out.println("queryPort: " + queryPort);

    }

}
