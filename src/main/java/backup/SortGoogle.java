package backup;

import cn.edu.thu.common.Utils;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 对一个文件夹内的各个文件内部进行排序，每个文件排好序后写到另一个文件夹
 */
public class SortGoogle {

  private static Logger logger = LoggerFactory.getLogger(ExtendTdrive.class);

  private static String src = "/Users/qiaojialin/Desktop/output";
  private static String outFolder = "/Users/qiaojialin/Desktop/sort";

  public static void main(String[] args) {

    int threadNum = 10;

    if (args.length != 0) {
      src = args[0];
      outFolder = args[1];
      threadNum = Integer.parseInt(args[2]);
    }

    List<String> files = new ArrayList<>();
    Utils.getAllFiles(src, files);

    List<List<String>> thread_files = new ArrayList<>();
    for (int i = 0; i < threadNum; i++) {
      thread_files.add(new ArrayList<>());
    }

    for (int i = 0; i < files.size(); i++) {
      String filePath = files.get(i);
      int thread = i % threadNum;
      thread_files.get(thread).add(filePath);
    }

    ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
    for (int threadId = 0; threadId < threadNum; threadId++) {
      executorService.submit(new Worker(thread_files.get(threadId)));
    }

    executorService.shutdown();

    // wait for all threads done
    boolean allDown = false;
    while (!allDown) {
      if (executorService.isTerminated()) {
        allDown = true;
      }
    }

  }

  static class Worker implements Runnable {

    private Logger logger = LoggerFactory.getLogger(Worker.class);
    private List<String> files;

    public Worker(List<String> files) {
      this.files = files;
    }

    @Override
    public void run() {

      try {

        // extend each file
        for (String file : files) {

          logger.info("start to sort file: {}", file);

          String line;

          List<Line> lines = new ArrayList<>();

          try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            while ((line = reader.readLine()) != null) {
              lines.add(new Line(line));
            }
          } catch (Exception e) {
            e.printStackTrace();
          }

          Collections.sort(lines);

          File file1 = new File(file);

          FileWriter writer = new FileWriter(outFolder + "/" + file1.getName(), true);
          for (Line aline: lines) {
            writer.write(aline.value + "\n");
          }
          writer.close();

        }
        logger.info("I'm done.");
      } catch (Exception e) {
        e.printStackTrace();
      }

    }
  }

  static class Line implements Comparable {

    long task;
    private long time;
    public String value;

    public Line(String line) {
      this.value = line;
      String[] items = line.split(",");
      time = Long.parseLong(items[0]);
      task = Long.parseLong(items[1]);

    }

    @Override
    public int compareTo(Object o) {
      Line obj = (Line) o;

      if(this.task < obj.task) {
        return -1;
      } else if (this.task > obj.task) {
        return 1;
      } else
        return Long.compare(this.time, obj.time);
    }
  }

}



