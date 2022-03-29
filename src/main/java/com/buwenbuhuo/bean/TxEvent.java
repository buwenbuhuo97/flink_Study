package com.buwenbuhuo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author 不温卜火
 * Create 2022-03-29 19:29
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:交易数据工具类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TxEvent {
    private String txId;
    private String payChannel;
    private Long eventTime;
}

