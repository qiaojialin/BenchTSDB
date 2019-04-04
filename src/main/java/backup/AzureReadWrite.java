package backup;

import java.io.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureReadWrite {

  private static final Logger logger = LoggerFactory.getLogger(AzureReadWrite.class);
  private static Map<String, List<String>> vmId_Values = new HashMap<>();
  private static Set<String> vmIds = new HashSet<>();
  private static List<String> vmIdList;
  private static String outPath;

  public static void main(String[] args) throws IOException {

    // vm file path
    String vmFile = "/Users/qiaojialin/Desktop/azure_data/testSchema.csv";

    // data file folder
    String filePath = "/Users/qiaojialin/Desktop/azure_data/testdata";

    // output file folder, output file name : 1, 2, ...
    outPath = "/Users/qiaojialin/Desktop/azure_data/haha";

    if (args.length == 3) {
      vmFile = args[0];
      filePath = args[1];
      outPath = args[2];
    }


    logger.info("start to read {}", vmFile);
    // get all vmIds

    try (BufferedReader reader = new BufferedReader(new FileReader(vmFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] items = line.split(",");
        vmIds.add(items[0]);
        vmId_Values.put(items[0], new ArrayList<>());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    logger.info("read {} done", vmFile);

    List<String> files = new ArrayList<>();

    for(int i = 1; i <= 125; i++) {
      files.add(filePath + "/vm_cpu_readings-file-" + i + "-of-125.csv");
    }

    vmIdList = new ArrayList<>(vmIds);


    for(int i = 0; i < files.size(); i++) {
      String file = files.get(i);
      File fileF = new File(file);
      if(!fileF.exists()) {
        continue;
      }


      // flush all series every 10 files
      if(i % 10 == 0) {
        logger.info("start to flush series, currently read the {}-th file", i);
        flush();
      }

      String line;
      logger.info("start to read {}", file);
      try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
        while ((line = reader.readLine()) != null) {
          // time, vmid, min_cpu, max_cpu, avg_cpu
          String[] items = line.split(",");

          if (items.length < 5) {
            continue;
          }

          List<String> lines = vmId_Values.get(items[1]);
          if(lines == null) {
            continue;
          }

          try {
            lines.add(items[0] + " " + items[4]);
          } catch (Exception ignore) {
            logger.warn("cannot parse line: {} in {}", line, file);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    flush();
  }


  private static void flush() {
    try {
      for (int j = 0; j < vmIdList.size(); j++) {
        String vmid = vmIdList.get(j);
        List<String> values = vmId_Values.get(vmid);
        if (values.isEmpty()) {
          continue;
        }
        FileWriter writer = new FileWriter(outPath + "/" + j, true);
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
        if (f1.isFile() && f1.getAbsolutePath().contains("readings")) {
          files.add(f1.getAbsolutePath());
        }
      }
    } else if(f.isFile()) {
      files.add(f.getAbsolutePath());
    }
  }

  static class Point implements Comparable {

    private int time;
    private String value;

    public Point(int time, String value) {
      this.time = time;
      this.value = value;
    }

    @Override
    public int compareTo(Object o) {
      Point obj = (Point) o;
      return this.time - obj.time;
    }
  }
}
