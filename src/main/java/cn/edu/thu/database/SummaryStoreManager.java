package cn.edu.thu.database;

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

public class SummaryStoreManager implements IDataBaseManager {

    private static final Logger logger = LoggerFactory.getLogger(SummaryStoreManager.class);
    private String storePath;
    private Config config;

    private SummaryStore store = null;

    private Set<Long> allStreams = new HashSet<>();

    public SummaryStoreManager(Config config, boolean forQuery) {
        this.config = config;
        this.storePath = config.SUMMARYSTORE_PATH;
        try {
            if (forQuery) {
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

        String tag = records.get(0).tag;

        // register metadata
        for (int i = 0; i < config.FIELDS.length; i++) {

            long stream = getStreamId(tag, i);
            streams.add(stream);
            if (allStreams.contains(stream)) {
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


    /**
     * @param tag   integer connected by "_", for example: 123_123
     * @param field the index of field in config.FIELDS, for example: 4
     * @return 1231234
     */
    private long getStreamId(String tag, String field) {
        StringBuilder builder = new StringBuilder();

        for (String s : tag.split("_")) {
            builder.append(s);
        }

        int i;

        for (i = 0; i < config.FIELDS.length; i++) {
            if (field.endsWith(config.FIELDS[i])) {
                break;
            }
        }

        builder.append(i);

        return Long.parseLong(builder.toString());
    }

    private long getStreamId(String tag, int i) {
        StringBuilder builder = new StringBuilder();

        for (String s : tag.split("_")) {
            builder.append(s);
        }

        builder.append(i);

        return Long.parseLong(builder.toString());
    }


    @Override
    public void createSchema() {


    }

    @Override
    public long count(String tag, String field, long startTime, long endTime) {

        long series = getStreamId(tag, field);

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
    public long flush() {
        long start = System.currentTimeMillis();
        try {
            for (long stream : allStreams) {
                store.unloadStream(stream);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return System.currentTimeMillis() - start;
    }

    @Override
    public long close() {
        long start = System.currentTimeMillis();

        try {
            store.close();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        return System.currentTimeMillis() - start;
    }

}