package com.dyzcs.mallpulisher.controler;

import com.alibaba.fastjson.JSONObject;
import com.dyzcs.mallpulisher.bean.Option;
import com.dyzcs.mallpulisher.bean.Stat;
import com.dyzcs.mallpulisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

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

    @RequestMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date, @RequestParam("startpage") Integer startpage, @RequestParam("size") Integer size, @RequestParam("keyword") String keyword) {
        // 获取ES中的数据
        Map saleDetailMap = publisherService.getSaleDetail(date, startpage, size, keyword);

        // 创建HashMap存放结果数据
        HashMap<String, Object> result = new HashMap<>();

        // 获取数据中的总数以及明细
        Long total = (Long) saleDetailMap.get("total");
        List detail = (List) saleDetailMap.get("detail");

        // 获取年龄信息
        Map ageMap = (Map) saleDetailMap.get("ageMap");

        Long lower20 = 0L;
        Long start20to30 = 0L;

        // 遍历ageMap，将年龄进行分组处理
        for (Object o : ageMap.keySet()) {
            Integer age = (Integer) o;
            Long count = (Long) ageMap.get(o);
            if (age < 20) {
                lower20 += count;
            } else if (age < 30) {
                start20to30 += count;
            }
        }

        double lower20Ratio = Math.round(lower20 * 1000 / total) / 10D;
        double start20to30Ratio = Math.round(start20to30 * 1000 / total) / 10D;
        double up30Ratio = 100D - lower20Ratio - start20to30Ratio;

        Option lower20Opt = new Option("20岁以下", lower20Ratio);
        Option start20to30Opt = new Option("20岁到30岁", start20to30Ratio);
        Option up30Opt = new Option("30岁以上", up30Ratio);

        ArrayList<Option> ageList = new ArrayList<>();
        ageList.add(lower20Opt);
        ageList.add(start20to30Opt);
        ageList.add(up30Opt);

        Stat ageStat = new Stat(ageList, "用户年龄占比");

        // 获取性别信息
        Map genderMap = (Map) saleDetailMap.get("genderMap");
        Long femaleCount = (Long) genderMap.get("F");
        double femaleRatio = Math.round(femaleCount * 1000 / total) / 10D;
        double maleRatio = 100D - femaleRatio;

        Option maleOpt = new Option("男", maleRatio);
        Option femaleOpt = new Option("女", femaleRatio);

        ArrayList<Option> genderList = new ArrayList<>();
        genderList.add(maleOpt);
        genderList.add(femaleOpt);

        Stat genderStat = new Stat(genderList, "用户性别占比");

        ArrayList<Stat> stats = new ArrayList<>();
        stats.add(ageStat);
        stats.add(genderStat);

        // 存放数据
        result.put("total", total);
        result.put("stat", stats);
        result.put("detail", detail);

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
