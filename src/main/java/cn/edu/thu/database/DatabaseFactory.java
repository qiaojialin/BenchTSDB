package cn.edu.thu.database;

import static cn.edu.thu.reader.BasicReader.DEVICE_PREFIX;

import cn.edu.thu.common.Config;
import cn.edu.thu.database.fileformat.ORCManager;
import cn.edu.thu.database.fileformat.ParquetManager;
import cn.edu.thu.database.fileformat.TsFileManager;
import cn.edu.thu.database.kairosdb.KairosDBManager;
import cn.edu.thu.database.opentsdb.OpenTSDBManager;
//import cn.edu.thu.database.waterwheel.WaterWheelManager;

public class DatabaseFactory {

  public static IDataBaseManager getDbManager(Config config, String deviceId) {
    switch (config.DATABASE) {
      case "NULL":
        return new NullManager();
//      case "INFLUXDB":
//        return new InfluxDBManager(config);
      case "OPENTSDB":
        return new OpenTSDBManager(config);
      case "KAIROSDB":
        return new KairosDBManager(config);
//      case "SUMMARYSTORE":
//        return new SummaryStoreManager(config);
//      case "WATERWHEEL":
//        return new WaterWheelManager(config);
      case "TSFILE":
        return new TsFileManager(config);
      case "PARQUET":
        return new ParquetManager(config, deviceId);
      case "ORC":
        return new ORCManager(config, deviceId);
      default:
        throw new RuntimeException(config.DATABASE + " not supported");
    }
  }

  public static String getDeviceId(Config config, String fileName) {

    switch (config.DATA_SET) {
      case "NOAA":
        String[] splitStrings = fileName.split(config.DATA_DIR)[1].replaceAll("\\.op", "")
            .split("-");
        return DEVICE_PREFIX + splitStrings[0] + "_" + splitStrings[1];
      case "GEOLIFE":
        return DEVICE_PREFIX + fileName.split(config.DATA_DIR)[1].split("/Trajectory")[0];
      case "TDRIVE":
        return DEVICE_PREFIX + fileName.split(config.DATA_DIR)[1].replaceAll("\\.txt", "");
      case "MLAB_UTILIZATION":
        return "";
      case "REDD":
        return DEVICE_PREFIX + fileName.split(config.DATA_DIR)[1].replaceAll("\\.dat", "")
            .replace("/", "_");
      default:
        throw new RuntimeException(config.DATA_SET + " not supported");
    }
  }
}
