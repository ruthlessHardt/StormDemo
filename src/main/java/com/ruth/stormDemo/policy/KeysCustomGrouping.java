package com.ruth.stormDemo.policy;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义分组策略
 * @Author ruthlessHardt
 * @Date :  2022/10/10 12:10
 */
public class KeysCustomGrouping implements CustomStreamGrouping {
    //接收目标集合
    private List<Integer> targetTask;

    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> targetTasks) {
        this.targetTask = targetTasks;
    }

    //  TODO  计算：task load=当前的负载/task处理速率（可以是这段时间的平均处理速率）。选择最小的（如果可以，希望加上通信时间）。
    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> subTasks = new ArrayList<>(10);
        for (int i = 0; i <= targetTask.size() / 2; i++) {
            subTasks.add(targetTask.get(i));
        }
        return subTasks;
    }
}
