package backup;

import cn.edu.thu.common.Utils;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MoveSort {

  public static void main(String[] args) {
    String input = "/Users/qiaojialin/Desktop/azure_data/haha";
    String output = "/Users/qiaojialin/Desktop/azure_data/haha/out";

    if(args.length == 2) {
      input = args[0];
      output = args[1];
    }

    List<String> files = new ArrayList<>();
    Utils.getAllFiles(input, files);

    List<FileSize> fileSizes = new ArrayList<>();

    for(String file: files) {
      File fileF = new File(file);
      fileSizes.add(new FileSize(file, fileF.length()));
    }

    Collections.sort(fileSizes);

    int i = 0;

    String currentOutFolder = output + File.separator + i;

    long currentLength = 0;
    long limit = 660 * 1024 * 1024;
//    limit = 100;

    for(FileSize file: fileSizes) {
      File startFile = new File(file.file);
      File tmpFile = new File(currentOutFolder);

      if(!tmpFile.exists()) {
        tmpFile.mkdirs();
        System.out.println("create folder " + tmpFile.getAbsolutePath());
      }

      File outFile = new File(currentOutFolder + File.separator + startFile.getName());
      startFile.renameTo(outFile);
      currentLength += outFile.length();

      System.out.println("move " + startFile.getAbsolutePath() + " to " +
          outFile.getAbsolutePath() + ", current length: " + currentLength + ", limit length: " + limit);

      if(currentLength > limit) {
        i++;
        currentOutFolder = output + File.separator + i;
        System.out.println("exceed limit size " + limit + ", change to new folder: " + currentOutFolder);
        currentLength = 0;
      }

    }
  }

  static class FileSize implements Comparable{
    private String file;
    private long size;

    public FileSize(String file, long size) {
      this.file = file;
      this.size = size;
    }
    @Override
    public int compareTo(Object o) {
      FileSize fileSize = (FileSize) o;
      return (int)(this.size - ((FileSize) o).size);
    }
  }

}
