package com.bigdata.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName Result
 * @Description:
 * @Author levlin
 * @Date 2021/12/9
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Result {

    private ResultCode resultCode;

    private String message;

    private Object data;

    public Result(ResultCode code, String message) {
        this.resultCode = code;
        this.message = message;
    }
}
