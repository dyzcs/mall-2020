package com.dyzcs.mallpulisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2020/10/21.
 */
public interface OrderMapper {
    // 1.查询当日交易额总数
    Double selectOrderAmountTotal(String date);

    // 2.查询当日交易额时分明细
    List<Map> selectOrderAmountHourMap(String date);
}
