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
 * <p>
 * http://localhost:8070/realtime-total?date=2020-10-18
 */
@RestController
public class PublisherController {
    @Autowired
    private PublisherService publisherService;

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    @RequestMapping("realtime-total")
    public String getRealTimeTotal(@RequestParam("date") String date) {
        // 查询Phoenix数据
        Integer dauTotal = publisherService.getRealTimeTotal(date);

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

        // 将map数据放入集合
        result.add(dauMap);
        result.add(newMidMap);

        // 返货结果
        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getDauTotalHourMap(@RequestParam("id") String id, @RequestParam("date") String date) {
        // 创建Map用于存放最终结果数据
        Map<String, Map> result = new HashMap<>();

        // 查询今天的日活数据
        Map todayDauTotal = publisherService.getDauTotalHourMap(date);

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

        // 查询昨天的数据
        Map yesterdayDauTotal = publisherService.getDauTotalHourMap(yesterday);

        // 将今天以及昨天的分时统计数据放入map集合
        result.put("yesterday", yesterdayDauTotal);
        result.put("today", todayDauTotal);

        // 返回数据
        return JSONObject.toJSONString(result);
    }
}
