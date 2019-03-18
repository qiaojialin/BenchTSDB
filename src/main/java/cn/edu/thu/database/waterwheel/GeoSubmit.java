package cn.edu.thu.database.waterwheel;

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
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;

public class GeoSubmit {

    public static void main(String... args) {

        TopologyConfig config = new TopologyConfig();

        boolean local = false;

        config.CHUNK_SIZE = 512 * 1024;
        config.previousTime = Integer.MAX_VALUE;

        if(local) {
            config.dataChunkDir = "./target/tmp";
            config.metadataDir = "./target/tmp";
            config.HDFSFlag = false;
        } else {
            config.HDFSFlag = true;
            config.HDFS_HOST = "hdfs://127.0.0.1:9000/";
            config.dataChunkDir = "hdfs://127.0.0.1:9000/waterwheel";
            config.metadataDir = "hdfs://127.0.0.1:9000/waterwheel";
        }

        System.out.println("dataChunkDir is set to " + config.dataChunkDir);


        DataSchema schema = new DataSchema();

        schema.addLongField("deviceId");
        schema.setPrimaryIndexField("deviceId");

        schema.addLongField("timestamp");
        schema.setTemporalField("timestamp");

        schema.addDoubleField("Latitude");
        schema.addDoubleField("Longitude");
        schema.addDoubleField("Zero");
        schema.addDoubleField("Altitude");

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

        if(local) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("GeoSubmitTopology", conf, topology);
        } else {
            try {
                StormSubmitter.submitTopology("GeoSubmitTopology", conf, topology);
                System.out.println("Topology is successfully submitted to the cluster!");
                System.out.println(config.getCriticalSettings());
            } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
                e.printStackTrace();
            }
        }

        System.out.println("ingestionPort: " + ingestionPort);
        System.out.println("queryPort: " + queryPort);

    }

}
