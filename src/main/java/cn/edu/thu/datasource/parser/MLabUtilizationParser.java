package cn.edu.thu.datasource.parser;

import cn.edu.thu.common.Record;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MLabUtilizationParser implements IParser {


    @Override
    public List<Record> parse(String fileName) {

        List<Record> records = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {

            String line;

            while ((line = reader.readLine()) != null) {
                records.addAll(convertToRecord(line));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return records;
    }

    @Override
    public void close() {

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
            float value = tv.getFloatValue("value");
            List<Object> fields = new ArrayList<>();
            fields.add(value);
            records.add(new Record(time, tag, fields));
        }
        return records;
    }

}
