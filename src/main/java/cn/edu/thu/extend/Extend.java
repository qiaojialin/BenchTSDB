package cn.edu.thu.extend;

import cn.edu.thu.common.Utils;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Extend {

  static String dataset = "";
  static String inputDir = "data/tdrive/1.txt";
  static String outputDir = "";
  static int copyNum = 2;
  static int threadNum = 5;

  /**
   * 在服务器上扩展时，需要的参数为 1、扩展数据集名称 2、数据集目录 3、扩展数据集输出目录 4、扩展的倍数
   *
   * @param args dataSetName, inputDir, outputDir, copyNum
   */
  public static void main(String[] args) {
    if (!parseInput(args)) {
      return;
    }

    List<String> files = new ArrayList<>();
    Utils.getAllFiles(inputDir, files);
    Collections.sort(files);

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
      executorService.submit(constructWork(thread_files, threadId));
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

  static boolean parseInput(String[] args) {
    if (args.length != 4) {
      System.out.println("need to specify dataSetName, inputDir, outputDir, copyNum");
      return false;
    }
    dataset = args[0].toLowerCase();
    inputDir = args[1];
    outputDir = args[2];
    copyNum = Integer.parseInt(args[3]);
    if (!outputDir.endsWith("/")) {
      outputDir += "/";
    }

    if (dataset.equals("tdrive") || dataset.equals("redd")) {
      File file = new File(outputDir);
      file.mkdirs();
      return true;
    } else {
      System.out.println("only support extend tdrive, redd.");
      return false;
    }
  }

  static Runnable constructWork(List<List<String>> thread_files, int threadId) {
    int offset = 0;
    for (int i = 0; i < threadId; i++) {
      offset += thread_files.get(i).size();
    }
    if (dataset.equals("tdrive")) {
      return new HExtendTdrive(thread_files.get(threadId), copyNum, outputDir, offset);
    } else if (dataset.equals("redd")) {
      return new HExtendRedd(thread_files.get(threadId), copyNum, outputDir, offset);
    } else {
      return null;
    }
  }

}
