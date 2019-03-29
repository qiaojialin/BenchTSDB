package cn.edu.thu.datasource.parser;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BasicParser {

  private static Logger logger = LoggerFactory.getLogger(BasicParser.class);
  private Config config;
  protected List<String> files;
  protected int currentFileIndex = 0;
  protected BufferedReader reader;
  protected List<String> cachedLines;

  public BasicParser(Config config, List<String> files) {
    this.config = config;
    this.files = files;
    try {
      reader = new BufferedReader(new FileReader(files.get(currentFileIndex)));
      init();
    } catch (Exception e) {
      logger.error("meet exception when init file: {}", files.get(currentFileIndex));
      e.printStackTrace();
    }
    cachedLines = new ArrayList<>();
  }

  public boolean hasNextBatch() {

    cachedLines.clear();

    try {
      String line;
      while (true) {

        if(reader == null) {
          return false;
        }

        line = reader.readLine();

        // try to read next file
        if(line == null) {
          if(currentFileIndex < files.size() - 1) {
            currentFileIndex++;
            reader.close();
            reader = new BufferedReader(new FileReader(files.get(currentFileIndex)));
            init();
            continue;
          } else {
            // no more file to read
            reader.close();
            reader = null;
            break;
          }
        }

        // read a line, cache it
        cachedLines.add(line);
        if (cachedLines.size() == config.BATCH_SIZE) {
          break;
        }
      }
    } catch (Exception ignore) {
      logger.error("read file {} failed", files.get(currentFileIndex));
      ignore.printStackTrace();
      return false;
    }

    return !cachedLines.isEmpty();
  }

  /**
   * initialize when start reading a file
   */
  abstract void init() throws Exception;


  /**
   * convert the cachedLines to Record list
   */
  public abstract List<Record> nextBatch();

}
