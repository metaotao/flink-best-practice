package com.zhiwei.flink.practice.demo;

/**
 * 计算实时热门商品
 */
public class HotItems {
    public static void main(String[] args) {

    }

    /** 用户行为数据结构 **/
    public static class UserBehavior {
        public long userId;         // 用户ID
        public long itemId;         // 商品ID
        public int categoryId;      // 商品类目ID
        public String behavior;     // 用户行为, 包括("pv", "buy", "cart", "fav")
        public long timestamp;      // 行为发生的时间戳，单位秒
    }
}
