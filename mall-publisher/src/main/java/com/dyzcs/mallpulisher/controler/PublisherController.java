package com.dyzcs.mallpulisher.controler;

import com.alibaba.fastjson.JSONObject;
import com.dyzcs.mallpulisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2020/10/18.
 */
@RestController
public class PublisherController {
    @Autowired
    private PublisherService publisherService;

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    /**
     * http://localhost:8070/realtime-total?date=2020-10-21
     */
    @RequestMapping("realtime-total")
    public String getRealTimeTotal(@RequestParam("date") String date) {
        // 查询Phoenix数据
        Integer dauTotal = publisherService.getRealTimeTotal(date);
        Double orderAmount = publisherService.getOrderAmount(date);

        // 创建集合用户存放结果Map
        ArrayList<Map<String, Object>> result = new ArrayList<>();
        // 创建Map用于存放日活数
        Map<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        // 创建Map用于存放日活数
        Map<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 233);

        // 创建Map用于存放交易额数据
        Map<String, Object> orderMap = new HashMap<>();
        orderMap.put("id", "order_amount");
        orderMap.put("name", "新增交易额");
        orderMap.put("value", orderAmount);

        // 将map数据放入集合
        result.add(dauMap);
        result.add(newMidMap);
        result.add(orderMap);

        // 返货结果
        return JSONObject.toJSONString(result);
    }

    /**
     * http://localhost:8070/realtime-hours?id=dau&date=2020-12-20
     * <p>
     * http://localhost:8070/realtime-hours?id=order_amount&date=2020-12-20
     */
    @RequestMapping("realtime-hours")
    public String getDauTotalHourMap(@RequestParam("id") String id, @RequestParam("date") String date) {
        // 创建Map用于存放最终结果数据
        Map<String, Map> result = new HashMap<>();

        // 获取昨天的日期
        String yesterday = getYesterdayString(date);
        Map todayHour = null;
        Map yesterdayHour = null;

        if ("dau".equals(id)) {
            // 查询今天的日活数据
            todayHour = publisherService.getDauTotalHourMap(date);
            // 查询昨天的数据
            yesterdayHour = publisherService.getDauTotalHourMap(yesterday);

        } else if ("order_amount".equals(id)) {
            todayHour = publisherService.getOrderAmountHour(date);
            yesterdayHour = publisherService.getOrderAmountHour(yesterday);
        }

        // 将今天以及昨天的分时统计数据放入map集合
        result.put("yesterday", yesterdayHour);
        result.put("today", todayHour);

        // 返回数据
        return JSONObject.toJSONString(result);
    }

    // 获取昨天日期的方法
    private String getYesterdayString(String date) {
        // 获取昨天日期的字符串
        Calendar calendar = Calendar.getInstance();
        String yesterday = null;
        try {
            calendar.setTime(sdf.parse(date));
            calendar.add(Calendar.DAY_OF_MONTH, -1);

            yesterday = sdf.format(calendar.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return yesterday;
    }
}
