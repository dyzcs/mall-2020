package com.dyzcs.mallpulisher.service.impl;

import com.dyzcs.mallpulisher.mapper.DauMapper;
import com.dyzcs.mallpulisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by Administrator on 2020/10/18.
 */
@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    private DauMapper dauMapper;

    @Override
    public String getRealTimeTotal(String date) {
        dauMapper.selectDauTotal(date);
        return null;
    }
}
