package com.dyzcs.malllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by Administrator on 2020/10/15.
 * <p>
 * RestController = Controller + ResponseBody
 */
@RestController
@Slf4j
public class LoggerController {

    @RequestMapping("test1")
    public String test1() {
        System.out.println("111111");
        return "success";
    }

    @RequestMapping("test2")
    public String test2(@RequestParam("aa") String aa) {
        System.out.println(aa);
        return "success";
    }

    @RequestMapping("log")
    public String sendLogToKafka(@RequestParam("logString") String logString) {
        log.info(logString);
        return "success";
    }
}
