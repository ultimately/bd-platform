package com.bigdata.controller;

import com.bigdata.entity.Result;
import com.bigdata.entity.ResultCode;
import com.bigdata.hadoop.MapReduceService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * @ClassName MapReduceAction
 * @Description:
 * @Author levlin
 * @Date 2021/12/19
 **/
@Slf4j
@RestController
@RequestMapping(value = "/hadoop/reduce")
public class MapReduceAction {

    @Autowired
    private MapReduceService mapReduceService;

    /**
     * localhost:8080/hadoop/reduce/wordCount?jobName=wordCount&inputPath=hdfs://localhost:9000/user/hadoop/test/count.txt
     * @param jobName
     * @param inputPath
     * @return
     * @throws Exception
     */
    @PostMapping(value = "wordCount")
    public Result wordCount(@RequestParam("jobName") String jobName, @RequestParam("inputPath") String inputPath)
            throws Exception {
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return new Result(ResultCode.FAILURE, "请求参数为空");
        }
        mapReduceService.wordCount(jobName, inputPath);
        return new Result(ResultCode.SUCCESS, "单词统计成功");
    }

    /**
     * localhost:8080/hadoop/reduce/wordCount?jobName=wordCount&inputPath=hdfs://localhost:9000/user/hadoop/test/group_count.txt
     * @param jobName
     * @param inputPath
     * @return
     * @throws Exception
     */
    @PostMapping(value = "groupSort")
    public Result groupSort(@RequestParam("jobName") String jobName, @RequestParam("inputPath") String inputPath)
            throws Exception {
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return new Result(ResultCode.FAILURE, "请求参数为空");
        }
        mapReduceService.groupSort(jobName, inputPath);
        return new Result(ResultCode.SUCCESS, "分组统计、排序成功");
    }

    @PostMapping(value = "join")
    public Result join(@RequestParam("jobName") String jobName, @RequestParam("inputPath") String inputPath) throws  Exception{

        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return new Result(ResultCode.FAILURE, "请求参数为空");
        }
        mapReduceService.join(jobName, inputPath);
        return new Result(ResultCode.SUCCESS, "表join操作成功");
    }

}
