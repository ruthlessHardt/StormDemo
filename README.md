<div style="text-align: center;font-size: 40px;
font-weight: bolder;margin-top: 60px">
    A Simple Storm Demo
</div> 

### New demand:
    1. Custom Grouping Policy （2022/10/10）coding.....


>  因为数据具有倾斜性，大部分数据符合齐夫卡分布（重尾分布），如果用keygrouping，那么就会造成负载失衡，一些task’就需要处理大量的数据，而一些则只需要处理少部分的数据。为了解决这个问题，提出一个自定义分组方式
>1. 首先根据上游节点发送的key值预测他的频率，但是由于数据流变化较大，频率会有所变化，且之前的高的频率后面会变化，所以要动态进行调整，适应这种变化。（要求准确，耗费内存小，速度快）
>2. 根据频率阈值，排序选出hotkeys，warmkeys，coldkeys。其中hotkeys是频率最高的一组键，warmkeys是频率不那么高的键，当在hotkeys和warmkeys中找不到时，即为coldkeys（coldkeys最好是不进行存储）。然后根据频率来决定这个“key”可以分配的候选task的数量。（最好是用一致哈希）Hotkeys直接shufflegrouping（候选数=组件下总的task数），warmkeys的候选数=频率*当前组件的task总数，coldkey利用keygrouping（候选数=1）。往往频率最高的键只分配几个候选是不够，不频繁的键分配多个又是不划算的，所以为了不频繁改变数据的分配和系统的稳定，我们会给根据阈值设置一定的范围以及一定的时间，只有超过这个范围，才会从原本的分类中踢出。
>3. 选择最佳task，计算：task load=当前的负载/task处理速率（可以是这段时间的平均处理速率）。选择最小的（如果可以，希望加上通信时间）。
   理想负载 = 当前所有task的负载/整个组件的平均处理速率±（可以设置一个范围值）。
   首先判断是否过了一段时间，如果是，且最小的taskload < 理想负载，则调整候选数量-1，如果最小的taskload > 理想负载，则调整候选数量 1。调整键分类
   最开始的设置：所有的key都只有一个可分配的task。
   （重点：决策时间要尽量少，预测尽量准确）  


### Data Source:
    1. kafka (we could get source from kafka)
    2. other (some file for data log)

### What Done In The Demo:
    1.get data sorce
    2.split or other filter operation
    3.use List or Collection for temp,then operate with stream api just for getting result
    4.save result in redis

# This is a simple demo!

<img src="https://s6.jpg.cm/2022/08/07/PdvPkE.png" width="100%">