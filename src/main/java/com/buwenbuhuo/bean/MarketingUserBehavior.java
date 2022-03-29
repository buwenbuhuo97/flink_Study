package com.buwenbuhuo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author 不温卜火
 * Create 2022-03-29 18:54
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:渠道统计的方法封装
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MarketingUserBehavior {
    private Long userId;
    private String behavior;
    private String channel;
    private Long timestamp;
}
