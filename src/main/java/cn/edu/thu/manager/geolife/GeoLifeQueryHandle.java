package cn.edu.thu.manager.geolife;


import java.io.IOException;

/**
 * Created by robert on 8/3/17.
 */
public interface GeoLifeQueryHandle {
    void handle(GeoLifeQueryRequest clientQueryRequest) throws IOException;
}
