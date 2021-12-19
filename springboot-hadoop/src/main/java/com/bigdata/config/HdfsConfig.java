package com.bigdata.config;

import org.springframework.beans.factory.annotation.Value;

/**
 * @ClassName HdfsConfig
 * @Description:
 * @Author levlin
 * @Date 2021/12/9
 **/
public class HdfsConfig {

    @Value("${hdfs.path}")
    private String path;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
