package cn.edu.thu.manager.geolife;

import indexingTopology.api.client.QueryClient;
import indexingTopology.api.client.QueryRequest;
import indexingTopology.api.client.QueryResponse;

import java.io.IOException;
import java.net.SocketTimeoutException;

/**
 * Created by robert on 9/3/17.
 */
public class GeoLifeQueryClient extends QueryClient {
    public GeoLifeQueryClient(String serverHost, int port) {
        super(serverHost, port);
    }

    public QueryResponse query(GeoLifeQueryRequest queryRequest) throws IOException, ClassNotFoundException {
        objectOutputStream.writeUnshared(queryRequest);
        objectOutputStream.reset();
        try {
            boolean eof = false;
            QueryResponse response = null;
            while (!eof) {
                try {
                    QueryResponse remainingQueryResponse = (QueryResponse) objectInputStream.readUnshared();
                    if (response == null) {
                        response = remainingQueryResponse;
                    } else {
                        response.dataTuples.addAll(remainingQueryResponse.dataTuples);
                    }
                    eof = remainingQueryResponse.getEOFFlag();
                } catch (SocketTimeoutException e) {
                }
            }
            return response;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public QueryResponse temporalRangeQuery(Number lowKey, Number highKey, long startTime, long endTime) throws IOException,
            ClassNotFoundException {
        objectOutputStream.writeUnshared(new QueryRequest<>(lowKey, highKey, startTime, endTime));
        objectOutputStream.reset();
        try {
            boolean eof = false;
            QueryResponse response = null;
            while (!eof) {
                try {
                    QueryResponse remainingQueryResponse = (QueryResponse) objectInputStream.readUnshared();
                    if (response == null) {
                        response = remainingQueryResponse;
                    } else {
                        response.dataTuples.addAll(remainingQueryResponse.dataTuples);
                    }
                    eof = remainingQueryResponse.getEOFFlag();
                } catch (SocketTimeoutException e) {
                }
            }
            return response;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public QueryResponse query(QueryRequest query) throws IOException  {
        objectOutputStream.writeUnshared(query);
        objectOutputStream.reset();
        try {
            return (QueryResponse) objectInputStream.readUnshared();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
