package cn.edu.thu.manager.geolife;

import indexingTopology.api.client.QueryResponse;
import indexingTopology.common.aggregator.AggregateField;
import indexingTopology.common.aggregator.Aggregator;
import indexingTopology.common.aggregator.Count;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.common.logics.DataTupleEquivalentPredicateHint;
import indexingTopology.common.logics.DataTuplePredicate;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Random;


public class GeoLifeTopologyQuery {

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

    public void executeQuery() {

        double selectivityOnOneDimension = Math.sqrt(Selectivity);
        DataSchema schema = getDataSchema();
        GeoLifeQueryClient queryClient = new GeoLifeQueryClient(QueryServerIp, 10001);
        Thread queryThread = new Thread(() -> {
            Random random = new Random();

            int executed = 0;
            long totalQueryTime = 0;

            while (true) {

                try {
                    queryClient.connectWithTimeout(10000);
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
                double x = x1 + (x2 - x1) * (1 - selectivityOnOneDimension) * random.nextDouble();
                double y = y1 + (y2 - y1) * (1 - selectivityOnOneDimension) * random.nextDouble();

                final double xLow = x;
                final double xHigh = x + selectivityOnOneDimension * (x2 - x1);
                final double yLow = y;
                final double yHigh = y + selectivityOnOneDimension * (y2 - y1);

//                DataTuplePredicate predicate = t ->
//                                 (double) schema.getValue("lon", t) >= xLow &&
//                                (double) schema.getValue("lon", t) <= xHigh &&
//                                (double) schema.getValue("lat", t) >= yLow &&
//                                (double) schema.getValue("lat", t) <= yHigh ;

                final int id = new Random().nextInt(100000);
                final String idString = "" + id;
//                DataTuplePredicate predicate = t -> schema.getValue("id", t).equals(Integer.toString(new Random().nextInt(100000)));
                DataTuplePredicate predicate = t -> schema.getValue("id", t).equals(idString);



                Aggregator<Integer> aggregator = new Aggregator<>(schema, "id", new AggregateField(new Count(), "*"));
//                Aggregator<Integer> aggregator = null;


//                DataSchema schemaAfterAggregation = aggregator.getOutputDataSchema();
//                DataTupleSorter sorter = (DataTuple o1, DataTuple o2) -> Double.compare((double) schemaAfterAggregation.getValue("count(*)", o1),
//                        (double) schemaAfterAggregation.getValue("count(*)", o2));


                DataTupleEquivalentPredicateHint equivalentPredicateHint = new DataTupleEquivalentPredicateHint("id", idString);

                GeoLifeQueryRequest queryRequest = new GeoLifeQueryRequest<>(xLow, xHigh, yLow, yHigh,
                        System.currentTimeMillis() - RecentSecondsOfInterest * 1000,
                        System.currentTimeMillis(), null, null, aggregator, null,null);
                long start = System.currentTimeMillis();
                try {
                    DateFormat dateFormat = new SimpleDateFormat("MM-dd HH:mm:ss");
                    Calendar cal = Calendar.getInstance();
                    System.out.println("[" + dateFormat.format(cal.getTime()) + "]: A query will be issued.");
                    QueryResponse response = queryClient.query(queryRequest);
                    System.out.println("A query finished.");
                    long end = System.currentTimeMillis();
                    totalQueryTime += end - start;
                    DataSchema outputSchema = response.getSchema();
                    System.out.println(outputSchema.getFieldNames());
                    List<DataTuple> tuples = response.getTuples();
                    for (int i = 0; i < tuples.size(); i++) {
                        System.out.println(tuples.get(i).toValues());
                    }
                    System.out.println(String.format("Query time: %d ms", end - start));

                    if (executed++ >= NumberOfQueries) {
                        System.out.println("Average Query Latency: " + totalQueryTime / (double)executed);
                        break;
                    }

                } catch (SocketTimeoutException e) {
                    Thread.interrupted();
                } catch (IOException e) {
                    if (Thread.currentThread().interrupted()) {
                        Thread.interrupted();
                    }
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                try {
                    queryClient.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            try {
                queryClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        queryThread.start();
    }



    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        args = new String[]{"-m", "query", "--query-server-ip", "localhost"};

        for(String arg: args) {
            System.out.println("arg:" + arg);
        }

        GeoLifeTopologyQuery kingBaseTopologyIngest = new GeoLifeTopologyQuery();

        CmdLineParser parser = new CmdLineParser(kingBaseTopologyIngest);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            e.printStackTrace();
            parser.printUsage(System.out);
        }

        if (kingBaseTopologyIngest.Help) {
            parser.printUsage(System.out);
            return;
        }

        switch (kingBaseTopologyIngest.Mode) {
            case "query": kingBaseTopologyIngest.executeQuery(); break;
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
