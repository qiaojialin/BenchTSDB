package cn.edu.thu.database;

import cn.edu.thu.common.Config;
import cn.edu.thu.database.fileformat.ORCManager;
import cn.edu.thu.database.fileformat.ParquetManager;
import cn.edu.thu.database.fileformat.TsFileManager;
import cn.edu.thu.database.kairosdb.KairosDBManager;
import cn.edu.thu.database.opentsdb.OpenTSDBManager;
import cn.edu.thu.database.waterwheel.WaterWheelManager;

public class DatabaseFactory {

  public static IDataBaseManager getDatabaseManager(Config config) {
    switch (config.DATABASE) {
      case "NULL":
        return new NullManager();
      case "INFLUXDB":
        return new InfluxDBManager(config);
      case "OPENTSDB":
        return new OpenTSDBManager(config);
      case "KAIROSDB":
        return new KairosDBManager(config);
      case "SUMMARYSTORE":
        return new SummaryStoreManager(config);
      case "WATERWHEEL":
        return new WaterWheelManager(config);
      default:
        throw new RuntimeException(config.DATABASE + " not supported");
    }
  }


  public static IDataBaseManager getFileManager(Config config, String filePath) {
    switch (config.DATABASE) {
      case "NULL":
        return new NullManager();
      case "TSFILE":
        return new TsFileManager(config, filePath);
      case "PARQUET":
        return new ParquetManager(config, filePath);
      case "ORC":
        return new ORCManager(config, filePath);
      default:
        throw new RuntimeException(config.DATABASE + " not supported");
    }
  }

}
