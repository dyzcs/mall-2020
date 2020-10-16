package com.dyzcs.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * Created by Administrator on 2020/10/16.
 */
object PropertiesUtil {
    def load(propertiesName: String): Properties = {
        val prop = new Properties()
        prop.load(new InputStreamReader(
            Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),
            "UTF-8"))
        prop
    }
}
