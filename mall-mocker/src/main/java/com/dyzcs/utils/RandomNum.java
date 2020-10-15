package com.dyzcs.utils;

import java.util.Random;

/**
 * Created by Administrator on 2020/10/15.
 */
public class RandomNum {
    public static int getRandInt(int fromNum, int toNum) {
        return fromNum + new Random().nextInt(toNum - fromNum + 1);
    }
}
