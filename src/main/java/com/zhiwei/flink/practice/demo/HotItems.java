package com.zhiwei.flink.practice.demo;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 计算实时热门商品
 */
public class HotItems {
    public static void main(String[] args) {

    }

    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {
        private final int topSize;



        public TopNHotItems(int topSize) {
            this.topSize = topSize;
        }
        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {

        }
    }

        /** 用于输出窗口的结果 */
    public static class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

        @Override
        public void apply(
                Tuple key,  // 窗口的主键，即 itemId
                TimeWindow window,  // 窗口
                Iterable<Long> aggregateResult, // 聚合函数的结果，即 count 值
                Collector<ItemViewCount> collector  // 输出类型为 ItemViewCount
        ) {
            Long itemId = ((Tuple1<Long>) key).f0;
            Long count = aggregateResult.iterator().next();
            collector.collect(ItemViewCount.of(itemId, window.getEnd(), count));
        }
    }

    /** COUNT 统计的聚合函数实现，每出现一条记录加一 */
    public static class CountAgg implements AggregateFunction<UserBehavior,Long,Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }
    /** 商品点击量(窗口操作的输出类型) */
    public static class ItemViewCount {
        private long itemId;     // 商品ID
        private long windowEnd;  // 窗口结束时间戳
        private long viewCount;  // 商品的点击量

        public static ItemViewCount of(long itemId, long windowEnd, long viewCount) {
            ItemViewCount result = new ItemViewCount();
            result.itemId = itemId;
            result.windowEnd = windowEnd;
            result.viewCount = viewCount;
            return result;
        }
    }

    /** 用户行为数据结构 **/
    public static class UserBehavior {
        private long userId;         // 用户ID
        private long itemId;         // 商品ID
        private int categoryId;      // 商品类目ID
        private String behavior;     // 用户行为, 包括("pv", "buy", "cart", "fav")
        private long timestamp;      // 行为发生的时间戳，单位秒
    }
}
