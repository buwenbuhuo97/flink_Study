package com.buwenbuhuo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author 不温卜火
 * Create 2022-04-05 22:33
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 恶意登录监控工具类实现
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginEvent {
    private Long userId;
    private String ip;
    private String eventType;
    private Long eventTime;
}
