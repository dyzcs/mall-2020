package com.dyzcs.mallpulisher.service.impl;

import com.dyzcs.mallpulisher.mapper.DauMapper;
import com.dyzcs.mallpulisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2020/10/18.
 */
@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    private DauMapper dauMapper;

    @Override
    public Integer getRealTimeTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHourMap(String date) {
        // 查询phoenix数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        // 创建map，用于存放调整完结构的数据
        HashMap<String, Long> resultMap = new HashMap<>();

        // 遍历list
        for (Map map : list) {
            resultMap.put((String) map.get("LH"), (Long) map.get("CT"));
        }

        // 返回结果
        return resultMap;
    }
}
