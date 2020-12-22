package com.dyzcs.mallpulisher.service.impl;

import com.dyzcs.constants.MallConstant;
import com.dyzcs.mallpulisher.mapper.DauMapper;
import com.dyzcs.mallpulisher.mapper.OrderMapper;
import com.dyzcs.mallpulisher.service.PublisherService;
import com.dyzcs.mallpulisher.util.MyESUtil;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
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

    @Autowired
    private OrderMapper orderMapper;

    //    @Autowired
    private JestClient jestClient = MyESUtil.getJestCline();


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

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        List<Map> mapList = orderMapper.selectOrderAmountHourMap(date);
        Map orderAmountHourMap = new HashMap<>();
        for (Map map : mapList) {
            orderAmountHourMap.put(map.get("CREATE_HOUR"), map.get("SUM_AMOUNT"));
        }
        return orderAmountHourMap;
    }

    // 构建DSL语句，查询ES获取数据
    @Override
    public Map getSaleDetail(String date, Integer startpage, Integer size, String keyword) {
        // 1.构建DSL语句
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 1.1 添加过滤条件
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        // 1.1.1 添加事件过滤条件，全值匹配
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));

        // 1.1.2 添加商品名称过滤条件，分词匹配
        MatchQueryBuilder sku_name = new MatchQueryBuilder("sku_name", keyword);
        sku_name.operator(Operator.AND);
        boolQueryBuilder.must(sku_name);
        searchSourceBuilder.query(boolQueryBuilder);

        // 1.2 添加聚合组
        // 1.2.1 添加年龄聚合组
        TermsAggregationBuilder ageAggs = AggregationBuilders.terms("countByAge").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);

        // 1.2.2 添加性别聚合组
        TermsAggregationBuilder genderAggs = AggregationBuilders.terms("countByGender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);

        // 1.3 添加分页信息
        searchSourceBuilder.from((startpage - 1) * size);
        searchSourceBuilder.size(size);

        // 2.执行查询
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(MallConstant.MALL_SALE_DETAIL).build();

        SearchResult result = null;
        try {
            result = jestClient.execute(search);
//            System.out.println(result);

        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3.解析返回数据
        assert result != null;
        // 3.1 获取总数
        JsonObject jsonObject = result.getJsonObject();
        Object total = ((JsonObject) ((JsonObject) jsonObject.get("hits")).get("total")).get("value");

        // 3.2 获取数据明细
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        ArrayList<Map> detailList = new ArrayList<>();
        for (SearchResult.Hit<Map, Void> hit : hits) {
            detailList.add(hit.source);
        }

        // 3.3 获取聚合组数据
        MetricAggregation aggregations = result.getAggregations();
        TermsAggregation countByAge = aggregations.getTermsAggregation("countByAge");
        HashMap<String, Long> ageMap = new HashMap<>();
        for (TermsAggregation.Entry entry : countByAge.getBuckets()) {
            String key = entry.getKey();
            Long count = entry.getCount();
            ageMap.put(key, count);
        }

        TermsAggregation countByGender = aggregations.getTermsAggregation("countByGender");
        HashMap<String, Long> genderMap = new HashMap<>();
        for (TermsAggregation.Entry entry : countByGender.getBuckets()) {
            String key = entry.getKey();
            Long count = entry.getCount();
            genderMap.put(key, count);
        }

        // 创建HashMap用于存放解析出来的数据，传递给controller
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("total", total);
        resultMap.put("detail", detailList);
        resultMap.put("ageMap", ageMap);
        resultMap.put("genderMap", genderMap);

        // 返回数据
        return resultMap;
    }
}
