package cn.edu.thu.manager;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import com.samsung.sra.datastore.*;
import com.samsung.sra.datastore.aggregates.SimpleCountOperator;
import com.samsung.sra.datastore.ingest.CountBasedWBMH;
import com.samsung.sra.datastore.storage.BackingStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class SummaryStoreM implements IDataBase {

    private static final Logger logger = LoggerFactory.getLogger(SummaryStoreM.class);
    private String storePath;
    private Config config;

    private SummaryStore store = null;

    private Set<Long> allStreams = new HashSet<>();

    public SummaryStoreM(Config config, boolean forQuery) {
        this.config = config;
        this.storePath = config.SUMMARYSTORE_PATH;
        try {
            if(forQuery) {
                store = new SummaryStore(storePath, new SummaryStore.StoreOptions().setKeepReadIndexes(true).setReadOnly(true));
            } else {
                store = new SummaryStore(storePath, new SummaryStore.StoreOptions().setKeepReadIndexes(true));
            }
        } catch (BackingStoreException | IOException | ClassNotFoundException e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public long insertBatch(List<Record> records) {

        List<Long> streams = new ArrayList<>();

        String tag1 = records.get(0).tag1;
        String tag2 = records.get(0).tag2;

        // register metadata
        for (int i = 0; i < config.FIELDS.length; i++) {

            // add stream for write
            long stream = getStreamId(tag1, tag2, i);
            streams.add(stream);
            if(allStreams.contains(stream)) {
                continue;
            }
            allStreams.add(stream);

            // register stream in Summary Store
            Windowing windowing = new GenericWindowing(new ExponentialWindowLengths(2));
            CountBasedWBMH wbmh = new CountBasedWBMH(windowing).setBufferSize(62);
            try {
                store.registerStream(stream, wbmh, new SimpleCountOperator());
            } catch (Exception ignore) {
            }
        }

        long start = System.currentTimeMillis();

        // write streams
        try {
            for (Record record : records) {
                for (int i = 0; i < config.FIELDS.length; i++) {
                    store.append(streams.get(i), record.timestamp, record.fields.get(i));
                }
            }
            // flush
            for (long stream : streams) {
                store.flush(stream);
            }
        } catch (BackingStoreException | StreamException e) {
            logger.error(e.getMessage());
        }

        return System.currentTimeMillis() - start;
    }


    private long getStreamId(String tag1, String tag2, String field) {
        int i;
        for (i = 0; i < config.FIELDS.length; i++) {
            if (field.endsWith(config.FIELDS[i])) {
                break;
            }
        }
        String idStr = tag1 + tag2 + i;

        return Long.parseLong(idStr);
    }

    private long getStreamId(String tag1, String tag2, int i) {
        String idStr = tag1 + tag2 + i;
        return Long.parseLong(idStr);
    }


    @Override
    public void createSchema() {


    }

    @Override
    public long count(String tag1, String tag2, String field, long startTime, long endTime) {

        long series = getStreamId(tag1, tag2, field);

        long start = System.currentTimeMillis();
        try {
            Object stream = store.query(series, startTime, endTime, 0);
            logger.info(stream.toString());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        return System.currentTimeMillis() - start;

    }

    @Override
    public long close() {
        long start = System.currentTimeMillis();

        try {

            for(long stream: allStreams) {
                store.unloadStream(stream);
            }

            store.close();
        } catch (BackingStoreException | IOException | StreamException e) {
            logger.error(e.getMessage());
        }

        return System.currentTimeMillis() - start;
    }

}
