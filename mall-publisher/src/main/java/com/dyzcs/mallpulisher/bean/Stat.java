package com.dyzcs.mallpulisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Created by Administrator on 2020/12/21.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Stat {
    private List<Option> options;
    private String title;
}
