package cn.edu.thu.extend.record;

public class TDriveRecord implements Comparable<TDriveRecord>{
  long time;
  String[] values;

  public TDriveRecord(long time, String... values) {
    this.time = time;
    this.values = values;
  }

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public String[] getValues() {
    return values;
  }

  public void setValues(String... values) {
    this.values = values;
  }


  @Override
  public int compareTo(TDriveRecord o) {
    return (int) (this.time - o.time);
  }

  public String genRecordStr(String separator){
    String res = ""+time;
    for (String value : values){
      res += separator + value;
    }
    return res;
  }
}
