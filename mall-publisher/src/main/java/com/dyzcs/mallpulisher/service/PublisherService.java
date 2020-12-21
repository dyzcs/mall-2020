package com.dyzcs.mallpulisher.service;

import java.util.Map;

/**
 * Created by Administrator on 2020/10/18.
 */
public interface PublisherService {
    Integer getRealTimeTotal(String date);

    Map getDauTotalHourMap(String date);

    Double getOrderAmount(String date);

    Map getOrderAmountHour(String date);

    Map getSaleDetail(String date, Integer startpage, Integer size, String keyword);
}
