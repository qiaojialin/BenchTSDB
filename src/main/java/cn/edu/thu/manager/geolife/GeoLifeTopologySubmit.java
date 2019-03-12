package cn.edu.thu.manager.geolife;

import indexingTopology.bolt.InputStreamReceiverBolt;
import indexingTopology.bolt.InputStreamReceiverBoltServer;
import indexingTopology.bolt.QueryCoordinatorBolt;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.common.logics.DataTupleMapper;
import indexingTopology.config.TopologyConfig;
import indexingTopology.topology.TopologyGenerator;
import indexingTopology.util.taxi.City;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;


public class GeoLifeTopologySubmit {

    /**
     * general configuration
     */
    @Option(name = "--help", aliases = {"-h"}, usage = "help")
    private boolean Help = false;

    @Option(name = "--mode", aliases = {"-m"}, usage = "submit|ingest|query")
    private String Mode = "Not Given";

    /**
     * topology configuration
     */
    @Option(name = "--topology-name", aliases = "-t", usage = "topology name")
    private String TopologyName = "T0";

    @Option(name = "--config-file", aliases = {"-f"}, usage = "conf.yaml to override default configs")
    private String confFile = "conf/conf.yaml";

    @Option(name = "--node", aliases = {"-n"}, usage = "number of nodes used in the topology")
    private int NumberOfNodes = 1;

    @Option(name = "--local", usage = "run the topology in local cluster")
    private boolean LocalMode = false;

    /**
     * ingest api configuration
     */
    @Option(name = "--ingest-server-ip", usage = "the ingestion server ip")
    private String IngestServerIp = "localhost";

    @Option(name = "--ingest-rate-limit", aliases = {"-r"}, usage = "max ingestion rate")
    private int MaxIngestRate = Integer.MAX_VALUE;

    /**
     * query api configuration
     */
    @Option(name = "--query-server-ip", usage = "the query server ip")
    private String QueryServerIp = "localhost";

    @Option(name = "--selectivity", usage = "the selectivity on the key domain")
    private double Selectivity = 1;

    @Option(name = "--temporal", usage = "recent time in seconds of interest")
    private int RecentSecondsOfInterest = 5;

    @Option(name = "--queries", usage = "number of queries to perform")
    private int NumberOfQueries = Integer.MAX_VALUE;


    static final double x1 = 40.012928;
    static final double x2 = 40.023983;
    static final double y1 = 116.292677;
    static final double y2 = 116.614865;
    static final int partitions = 128;


    public void submitTopology() throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        DataSchema rawSchema = getRawDataSchema();
        DataSchema schema = getDataSchema();
        City city = new City(x1, x2, y1, y2, partitions);

        Integer lowerBound = 0;
        Integer upperBound = city.getMaxZCode();

        final boolean enableLoadBalance = false;

        TopologyConfig config = new TopologyConfig();


        if (! confFile.equals("none")) {
            config.override(confFile);
            System.out.println("Topology is overridden by " + confFile);
            System.out.println(config.getCriticalSettings());
        } else {
            System.out.println("conf.yaml is not specified, using default instead.");
        }

        InputStreamReceiverBolt dataSource = new InputStreamReceiverBoltServer(rawSchema, 10000, config);

        QueryCoordinatorBolt<Integer> queryCoordinatorBolt = new GeoLifeQueryCoordinatorBolt<>(lowerBound,
                upperBound, 10001, city, config, schema);

        DataTupleMapper dataTupleMapper = new DataTupleMapper(rawSchema, (Serializable & Function<DataTuple, DataTuple>) t -> {
            double lon = (double)schema.getValue("lon", t);
            double lat = (double)schema.getValue("lat", t);
            int zcode = city.getZCodeForALocation(lon, lat);
            t.add(zcode);
            t.add(System.currentTimeMillis());
            return t;
        });

        List<String> bloomFilterColumns = new ArrayList<>();
        bloomFilterColumns.add("id");

        TopologyGenerator<Integer> topologyGenerator = new TopologyGenerator<>();
        topologyGenerator.setNumberOfNodes(NumberOfNodes);

        StormTopology topology = topologyGenerator.generateIndexingTopology(schema, lowerBound, upperBound,
                enableLoadBalance, dataSource, queryCoordinatorBolt, dataTupleMapper, bloomFilterColumns, config);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(NumberOfNodes);

        conf.put(Config.WORKER_CHILDOPTS, "-Xmx1024m");
        conf.put(Config.WORKER_HEAP_MEMORY_MB, 1024);
        conf.put(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS, 1);
        conf.setTopologyStrategy(waterwheel.scheduler.FFDStrategyByCPU.class);

        if (LocalMode) {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology(TopologyName, conf, topology);
        } else {
            StormSubmitter.submitTopology(TopologyName, conf, topology);
            System.out.println("Topology is successfully submitted to the cluster!");
            System.out.println(config.getCriticalSettings());
        }
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        args = new String[]{"-m", "submit", "--local", "-f", "conf/conf.yaml"};

        for(String arg: args) {
            System.out.println("arg:" + arg);
        }

        GeoLifeTopologySubmit geoLifeTopologySubmit = new GeoLifeTopologySubmit();

        CmdLineParser parser = new CmdLineParser(geoLifeTopologySubmit);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            e.printStackTrace();
            parser.printUsage(System.out);
        }

        if (geoLifeTopologySubmit.Help) {
            parser.printUsage(System.out);
            return;
        }

        switch (geoLifeTopologySubmit.Mode) {
            case "submit": geoLifeTopologySubmit.submitTopology(); break;
            default: System.out.println("Invalid command!");
        }
    }


    static private DataSchema getRawDataSchema() {
        DataSchema rawSchema = new DataSchema();
        rawSchema.addVarcharField("id", 32);
        rawSchema.addVarcharField("veh_no", 10);
        rawSchema.addDoubleField("lon");
        rawSchema.addDoubleField("lat");
        rawSchema.addIntField("car_status");
        rawSchema.addDoubleField("speed");
        rawSchema.addVarcharField("position_type", 10);
        rawSchema.addVarcharField("update_time", 32);
        return rawSchema;
    }

    static private DataSchema getDataSchema() {
        DataSchema schema = new DataSchema();
        schema.addVarcharField("id", 32);
        schema.addVarcharField("veh_no", 10);
        schema.addDoubleField("lon");
        schema.addDoubleField("lat");
        schema.addIntField("car_status");
        schema.addDoubleField("speed");
        schema.addVarcharField("position_type", 10);
        schema.addVarcharField("update_time", 32);
        schema.addIntField("zcode");
        schema.addLongField("timestamp");
        schema.setTemporalField("timestamp");
        schema.setPrimaryIndexField("zcode");
        return schema;
    }

}
