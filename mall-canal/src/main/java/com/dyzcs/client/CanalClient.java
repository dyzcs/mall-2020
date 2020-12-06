package com.dyzcs.client;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.dyzcs.constants.MallConstant;
import com.dyzcs.utils.MyKafkaSender;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Created by Administrator on 2020/10/21.
 */
public class CanalClient {
    public static void main(String[] args) {
        // 获取Canal连接器
        CanalConnector canalConnector =
                CanalConnectors.newSingleConnector(new InetSocketAddress("s183", 11111), "example", "", "");

        // 抓取数据
        while (true) {
            // 连接Canal
            canalConnector.connect();
            // 指定订阅的数据库(mall)
            canalConnector.subscribe("mall.*");
            // 执行抓取数据操作
            Message message = canalConnector.get(1024);

            // 判断当前抓取的数据是否为空，如果为空，休息
            if (message.getEntries().isEmpty()) {
                System.out.println("没有数据，休息5s");
                try {
                    Thread.sleep(5 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                // 解析message
                for (CanalEntry.Entry entry : message.getEntries()) {
                    // 对当前entry类型做判断，只需要数据变化的内容，如果为事务的开启和关闭以及心跳信息则过滤掉
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {
                        // 获取当前数据的表名
                        String tableName = entry.getHeader().getTableName();

                        try {
                            // 将StoreValue反序列化
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                            // 取出行集和事件类型
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                            CanalEntry.EventType eventType = rowChange.getEventType();

                            // 处理数据
                            handler(tableName, rowDatasList, eventType);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    // 处理监控的mysql数据，解析并发送至Kafka
    private static void handler(String tableName, List<CanalEntry.RowData> rowDatasList, CanalEntry.EventType eventType) {
        // 判断是否为订单表 && 取下单数据，即只要新增的数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            sendToKafka(rowDatasList, MallConstant.MALL_ORDER_INFO);
        } else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            sendToKafka(rowDatasList, MallConstant.MALL_ORDER_DETAIL);
        } else if ("user_info".equals(tableName) &&
                (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType))) {
            sendToKafka(rowDatasList, MallConstant.MALL_USER_INFO);
        }
    }

    /**
     * 将数据发送至kafka
     */
    private static void sendToKafka(List<CanalEntry.RowData> rowDatasList, String topic) {
        // 遍历rowDatasList
        for (CanalEntry.RowData rowData : rowDatasList) {
            // 创建JSON对象，存放多个列对象
            JSONObject jsonObject = new JSONObject();
            // 遍历修改后的数据列
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                jsonObject.put(column.getName(), column.getValue());
            }
            // 打印当前行的数据，写入kafka
            System.out.println(jsonObject.toJSONString());
            MyKafkaSender.send(topic, jsonObject.toString());
        }
    }
}
