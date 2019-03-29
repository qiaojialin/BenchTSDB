package utils;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.datasource.parser.BasicParser;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MLabUtilizationParser extends BasicParser {

    public MLabUtilizationParser(Config config, List<String> files) {
        super(config, files);
    }

    private List<Record> convertToRecord(String line) {
        List<Record> records = new ArrayList<>();
        JSONObject jsonObject = JSON.parseObject(line);

        String tag1 = jsonObject.getString("metric");
        String tag2 = jsonObject.getString("hostname");
        String tag3 = jsonObject.getString("experiment");
        String tag = tag1 + "." + tag2 + "," + tag3;

        JSONArray jsonArray = jsonObject.getJSONArray("sample");
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject tv = jsonArray.getJSONObject(i);
            long time = tv.getLongValue("timestamp");
            double value = tv.getDoubleValue("value");
            List<Object> fields = new ArrayList<>();
            fields.add(value);
            records.add(new Record(time, tag, fields));
        }
        return records;
    }



    @Override
    public List<Record> nextBatch() {
        List<Record> records = new ArrayList<>();
        for (String line : cachedLines) {
            records.addAll(convertToRecord(line));
        }
        return records;
    }

    @Override
    public void init() throws Exception {

    }
}
