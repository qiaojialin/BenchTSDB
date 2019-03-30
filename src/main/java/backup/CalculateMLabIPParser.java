package backup;

import cn.edu.thu.common.Config;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.FileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CalculateMLabIPParser {

  private Config config;
  private static Logger logger = LoggerFactory.getLogger(CalculateMLabIPParser.class);


  public CalculateMLabIPParser(Config config) {
    this.config = config;
  }

  /**
   * @return record num
   */
  public long parse(String fileName) {

    long total = 0;

    try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {

      String line;

      while ((line = reader.readLine()) != null) {
        if (fileName.contains("dash")) {
          total += convertToRecords(line);
        } else {
          total++;
        }
      }

    } catch (Exception e) {
      logger.warn("parse {} failed, because {}", fileName, e.getMessage());
      e.printStackTrace();
    }

    return total;
  }

  /**
   * parse _dash file
   */
  private long convertToRecords(String line) {
    JsonParser jsonParser = new JsonParser();
    JsonObject jsonObject = jsonParser.parse(line).getAsJsonObject();

    try {
      JsonArray clients = jsonObject.get("client").getAsJsonArray();
      return clients.size();
    } catch (Exception ignore) {
      logger.warn("can not parse: {}", line);
      logger.warn("exception: {}", line);
    }
    return 0L;
  }

}
