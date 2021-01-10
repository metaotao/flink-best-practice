package com.zhiwei.flink.practice.connector.datastream.kafka;

public class UserBehavior {
    public long userId;         // 用户ID
    public long itemId;         // 商品ID
    public int categoryId;      // 商品类目ID
    public String behavior;     // 用户行为, 包括("pv", "buy", "cart", "fav")
    public long timestamp;      // 行为发生的时间戳，单位秒


    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public UserBehavior(long userId, long itemId,int categoryId, String behavior, long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    public static UserBehavior fromString(String userBehavior) {
        String[] split = userBehavior.split(",");
        return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]),
                Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
    }
}
