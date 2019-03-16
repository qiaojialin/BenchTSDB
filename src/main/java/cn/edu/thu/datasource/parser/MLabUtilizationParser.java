package cn.edu.thu.datasource.parser;

import cn.edu.thu.common.Record;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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

    private List<Record> convertToRecord(String line) {
        List<Record> records = new ArrayList<>();
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObject = jsonParser.parse(line).getAsJsonObject();

        String tag1 = jsonObject.get("metric").getAsString();
        String tag2 = jsonObject.get("hostname").getAsString();
        String tag3 = jsonObject.get("experiment").getAsString();
        String tag = tag1 + "." + tag2 + "," + tag3;

        JsonArray jsonArray = jsonObject.get("sample").getAsJsonArray();
        for (int i = 0; i < jsonArray.size(); i++) {
            JsonObject tv = jsonArray.get(i).getAsJsonObject();
            long time = tv.get("timestamp").getAsLong();
            float value = tv.get("value").getAsFloat();
            List<Object> fields = new ArrayList<>();
            fields.add(value);
            records.add(new Record(time, tag, fields));
        }
        return records;
    }

}
