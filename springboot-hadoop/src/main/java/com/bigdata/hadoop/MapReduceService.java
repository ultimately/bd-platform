package com.bigdata.hadoop;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @ClassName MapReduceService
 * @Description:
 * @Author levlin
 * @Date 2021/12/20
 **/
@Service
public class MapReduceService {
    // 默认reduce输出目录
    private static final String OUTPUT_PATH = "/output";

    /**
     * 单词统计，统计某个单词出现的次数
     * @param jobName
     * @param inputPath
     * @throws Exception
     */
    public void wordCount(String jobName, String inputPath) throws Exception {
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return;
        }
        // 输出目录 = output/当前Job,如果输出路径存在则删除，保证每次都是最新的
        String outputPath = OUTPUT_PATH + "/" + jobName;
        if (HdfsService.existFile(outputPath)) {
            HdfsService.deleteFile(outputPath);
        }
        HdfsService.getWordCountJobsConf(jobName, inputPath, outputPath);
    }

    public void groupSort(String jobName, String inputPath) throws Exception {
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return;
        }
        // 输出目录 = output/当前Job
        String outputPath = OUTPUT_PATH + "/" + jobName;
        if (HdfsService.existFile(outputPath)) {
            HdfsService.deleteFile(outputPath);
        }
        HdfsService.groupSort(jobName, inputPath, outputPath);
    }
}
