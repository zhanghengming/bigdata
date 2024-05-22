package com.flink.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.List;

public class OperatorStateDemo implements SinkFunction<Tuple2<Long,Long>>, CheckpointedFunction {
    ListState<Tuple2<Long, Long>> operatorState;

    private List<Tuple2<Long,Long>> bufferedElements;

    int threshold;

    /**
     * @param context the context for drawing a snapshot of the operator
     * @throws Exception
     * checkpoint时调用
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("....snapshotState");
        this.operatorState.clear();
        // 将缓存器中的元素添加到状态中保存
        for (Tuple2<Long, Long> element : bufferedElements) {
            operatorState.add(element);
        }
    }

    /**
     * @param context the context for initializing the operator
     * @throws Exception
     * 每一个Function在最开始的实例化的时候调用，方法内，实例化状态，在每个task中执行
     * 一旦程序发生异常，就会继续执行初始化
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("....initializeState");
        ListStateDescriptor<Tuple2<Long, Long>> operatorStateDescriptor = new ListStateDescriptor<>(
                "operatorDemo",
                TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                })
        );
        // 做出一个state
        operatorState = context.getOperatorStateStore().getListState(operatorStateDescriptor);
        if (context.isRestored()) {
            // 一旦发生异常，就会变成true，从最新的state中获取数据，将数据放回到缓存当中
            for (Tuple2<Long, Long> element : operatorState.get()) {
                bufferedElements.add(element);
            }
            System.out.println("....context.isRestored():true");
        }
    }

    /**
     * @param value   The input record.
     * @param context Additional context about the input record.
     * @throws Exception
     * 每来一个数据调用一次，把所有的到来的数据都放到缓存器中。目的是为了checkpoint的时候，从缓存器中拿出数据
     */
    @Override
    public void invoke(Tuple2<Long, Long> value, Context context) throws Exception {
        System.out.println("---------invoke..........");
        bufferedElements.add(value);
        // 如果缓冲器的长度满足阈值,就打印到控制台
        if (bufferedElements.size() == threshold) {
            for (Tuple2<Long, Long> element : bufferedElements) {
                System.out.println("...out:" + element);
            }
            //
            bufferedElements.clear();
        }

    }
}
