package cn.edu.thu.reader;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BasicReader {

  private static Logger logger = LoggerFactory.getLogger(BasicReader.class);
  protected Config config;
  protected List<String> files;
  protected BufferedReader reader;
  protected List<String> cachedLines;
  protected int currentFileIndex = 0;

  protected String currentFile;
  protected String currentDeviceId;

  public BasicReader(Config config, List<String> files) {
    this.config = config;
    this.files = files;
    try {
//      reader = new BufferedReader(new FileReader(files.get(currentFileIndex)));
      currentFile = files.get(currentFileIndex);
//      init();
      logger.info("start to read {}-th file {}", currentFileIndex, currentFile);
    } catch (Exception e) {
      logger.error("meet exception when init file: {}", currentFile);
      e.printStackTrace();
    }
    cachedLines = new ArrayList<>();
  }

//  public boolean hasNextBatch() throws Exception {
//    if (currentFileIndex == files.size()) {
//      return false;
//    }
//    currentFile = files.get(currentFileIndex++);
//    logger.info("start to read file {}", currentFile);
//    reader = new BufferedReader(new FileReader(currentFile));
//    init();
//    cachedLines.clear();
//    String line;
//    while ((line = reader.readLine()) != null) {
//      cachedLines.add(line);
//    }
//    return true;
//  }


  private boolean isNew = true;
  public boolean hasNextBatch() throws Exception {
    if(currentFileIndex == files.size()) {
      return false;
    }
    cachedLines.clear();
    if(isNew){
      reader = new BufferedReader(new FileReader(files.get(currentFileIndex)));
      init();
      isNew = false;
    }
    currentFile = files.get(currentFileIndex);
    logger.info("start to read file {}", currentFile);
    String line;
    while (true){
      line = reader.readLine();
      if (line != null){
        cachedLines.add(line);
        if (config.BATCH_SIZE == cachedLines.size()){
          return true;
        }
      }else {
        currentFileIndex++;
        isNew = true;
        return true;
      }
    }

//    return true;
//    return !cachedLines.isEmpty();
  }


  /**
   * convert the cachedLines to Record list
   */
  abstract public List<Record> nextBatch();


  /**
   * initialize when start reading a file
   * maybe skip the first lines
   * maybe init the tagValue(deviceId) from file name
   */
  public abstract void init() throws Exception;

}
