package cn.edu.thu.manager.waterwheelsample;

import indexingTopology.api.client.QueryClient;
import indexingTopology.api.client.QueryRequest;
import indexingTopology.api.client.QueryResponse;
import indexingTopology.common.aggregator.*;
import indexingTopology.common.data.DataSchema;

import java.io.IOException;

public class GeoQuery {

    public static void main(String... args) {

        DataSchema schema = new DataSchema();
        schema.addLongField("id");
        schema.addDoubleField("lon");
        schema.addDoubleField("lat");
        schema.addDoubleField("alt");
        schema.addLongField("timestamp");
        schema.setTemporalField("timestamp");
        schema.setPrimaryIndexField("id");


        final QueryClient queryClient = new QueryClient("localhost", 10001);

        try {
            queryClient.connectWithTimeout(10000);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Aggregator<Integer> aggregator = new Aggregator<>(schema, "id", new AggregateField(new Count(), "lon"));

        try {
            //a key range query
            QueryResponse response =  queryClient.query(new QueryRequest<>(0L,0L, Long.MIN_VALUE, Long.MAX_VALUE, aggregator));

            System.out.println(response.getTuples().get(0).get(1));

        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
