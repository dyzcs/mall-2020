package com.dyzcs.mallpulisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2020/10/18.
 */
public interface DauMapper {
    Integer selectDauTotal(String date);

    List<Map> selectDauTotalHourMap(String date);
}
