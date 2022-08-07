package com.example.xxxtwice.domain;

import lombok.Data;

/**
 * @Titile :  LogDTO
 * @Author :  ruthlessHardt
 * @Date :  2022/8/6 18:44
 * @Describtion : 名称，数量，单价，类别，时间
 */
@Data
public class LogDTO {
    /**
     * 商品名 0
     */
    private String productName;
    /**
     * 数量 1
     */
    private Integer purchaseNum;
    /**
     * 单价 2
     */
    private Double productPrice;
    /**
     * 类别 3
     */
    private String productType;
    /**
     * 时间 4
     */
    private String purchaseTime;
}
