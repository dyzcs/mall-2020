package com.dyzcs.malllogger.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * Created by Administrator on 2020/10/15.
 */
@Controller
public class LoggerController {

    @RequestMapping("test")
    public String test() {
        System.out.println("111111");
        return "success";
    }
}
