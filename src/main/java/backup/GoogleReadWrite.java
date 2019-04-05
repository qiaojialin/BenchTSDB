package backup;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * output:
 *
 * job ID
 *
 * start time, task index, machine ID, end time, ....
 */
public class GoogleReadWrite {

  private static final Logger logger = LoggerFactory.getLogger(AzureReadWrite.class);
  private static Map<Long, List<String>> jobId_Values = new HashMap<>();
  private static String outPath;

  public static void main(String[] args) throws IOException {

    // data file folder
    String filePath = "/Users/qiaojialin/Desktop/google";

    // output file folder, output file name : 1, 2, ...
    outPath = "/Users/qiaojialin/Desktop/output";

    if (args.length == 2) {
      filePath = args[0];
      outPath = args[1];
    }

    List<String> files = new ArrayList<>();

    getAllFiles(filePath, files);

    for(int i = 0; i < files.size(); i++) {

      String file = files.get(i);

      // flush all series every 10 files
      if(i % 10 == 0) {
        logger.info("start to flush series, currently read file {}", file);
        flush();
      }

      String line;
      logger.info("start to read {}", file);
      try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
        while ((line = reader.readLine()) != null) {
          // time, vmid, min_cpu, max_cpu, avg_cpu
          String[] items = line.split(",");

          if (items.length < 10) {
            continue;
          }

          long jobId = Long.parseLong(items[2]);
          List<String> values = jobId_Values.computeIfAbsent(jobId, k -> new ArrayList<>());

          StringBuilder builder = new StringBuilder(items[0]); // start time
          builder.append(",").append(items[3]); // task index
          builder.append(",").append(items[4]); // machine id
          builder.append(",").append(items[1]); // end time

          for(int j = 5; j < items.length; j++) {
            builder.append(",").append(items[j]);
          }
          values.add(builder.toString());
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    logger.info("start the last flush");
    flush();
    logger.info("all done");
  }


  private static void flush() {
    try {
      for(Entry<Long, List<String>> entry: jobId_Values.entrySet()) {
        long job = entry.getKey();
        List<String> values = entry.getValue();
        if (values.isEmpty()) {
          continue;
        }
        FileWriter writer = new FileWriter(outPath + "/" + job, true);
        for (String line : values) {
          writer.write(line + "\n");
        }
        values.clear();
        writer.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  private static void getAllFiles(String strPath, List<String> files) {
    File f = new File(strPath);
    if (f.isDirectory()) {
      File[] fs = f.listFiles();
      for (File f1 : fs) {
        files.add(f1.getAbsolutePath());
      }
    } else if(f.isFile()) {
      files.add(f.getAbsolutePath());
    }
  }


}
