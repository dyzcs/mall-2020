package com.dyzcs.mallpulisher.service;

import java.util.Map;

/**
 * Created by Administrator on 2020/10/18.
 */
public interface PublisherService {
    Integer getRealTimeTotal(String date);

    Map getDauTotalHourMap(String date);
}
