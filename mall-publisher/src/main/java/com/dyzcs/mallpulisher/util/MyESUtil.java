package com.dyzcs.mallpulisher.util;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;

/**
 * Created by Administrator on 2020/12/21.
 */
public class MyESUtil {
    public static JestClient getJestCline(){
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://localhost:9200")
                .multiThreaded(true)
                .build());
        return  factory.getObject();
    }
}
