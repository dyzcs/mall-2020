package com.dyzcs.uploader;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by Administrator on 2020/10/15.
 */
public class LogUpLoader {
    public static void sendLogStream(String log) {
        try {
            // 不同的日志类型对应不同的 URL
            URL url = new URL("http://s183/log");

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            // 设置请求方式为 post
            conn.setRequestMethod("POST");

            // 时间头用来供 server 进行时钟校对
            conn.setRequestProperty("clientTime", System.currentTimeMillis() + "");

            // 允许上传数据
            conn.setDoOutput(true);

            // 输出流
            OutputStream out = conn.getOutputStream();
            out.write(("logString=" + log).getBytes());
            out.flush();
            out.close();
            int code = conn.getResponseCode();
            System.out.println(code);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
