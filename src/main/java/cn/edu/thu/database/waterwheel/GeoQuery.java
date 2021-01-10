//package cn.edu.thu.database.waterwheel;
//
//import indexingTopology.api.client.QueryClient;
//import indexingTopology.api.client.QueryRequest;
//import indexingTopology.api.client.QueryResponse;
//import indexingTopology.common.aggregator.*;
//import indexingTopology.common.data.DataSchema;
//
//import java.io.IOException;
//
//public class GeoQuery {
//
//    public static void main(String... args) {
//
//        DataSchema schema = new DataSchema();
//
//        schema.addLongField("deviceId");
//        schema.setPrimaryIndexField("deviceId");
//
//        schema.addLongField("timestamp");
//        schema.setTemporalField("timestamp");
//
//        schema.addDoubleField("Latitude");
//        schema.addDoubleField("Longitude");
//        schema.addDoubleField("Zero");
//        schema.addDoubleField("Altitude");
//
//
//        final QueryClient queryClient = new QueryClient("localhost", 10001);
//
//        try {
//            queryClient.connectWithTimeout(10000);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        Aggregator<Integer> aggregator = new Aggregator<>(schema, "deviceId", new AggregateField(new Count(), "Latitude"));
//
//        try {
//            //a key range query
//            QueryResponse response =  queryClient.query(new QueryRequest<>(0L,0L, Long.MIN_VALUE, Long.MAX_VALUE, aggregator));
//
//            System.out.println(response.getTuples().get(0).get(1));
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//
//    }
//}
