package com.fastbird.streaming.flink;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Created by yangguo on 2018/8/10.
 */
public class StatefulStreamByFlink {
    private static class ListBuffuer{
        public ListBuffuer(){
            buffer=new ArrayList<KeyValue>();
        }
        public List<KeyValue> getBuffer() {
            return buffer;
        }

        public void setBuffer(List<KeyValue> buffer) {
            this.buffer = buffer;
        }

        public ListBuffuer addItem(KeyValue it){
            buffer.add(it);
            return this;
        }
        private List<KeyValue> buffer;


    }
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment streamingEnv= StreamExecutionEnvironment.createLocalEnvironment(1);
        DataStream<KeyValue> source=streamingEnv.addSource(new RichParallelSourceFunction<KeyValue>() {
            public void run(SourceContext<KeyValue> ctx) throws Exception {
                int count=0;
                int maxIndex=10;
                while(true){
                    Thread.sleep(1000);
                    KeyValue kv=new KeyValue();
                    int idx=count%maxIndex;
                    if(idx==0){
                        kv.setKey(count+"");
                    }else{
                        kv.setKey(idx+"");
                    }
                    kv.setValue(count+"");
                    System.out.println(kv.getKey()+","+kv.getValue());
                    count++;
                    ctx.collect(kv);
                }
            }

            public void cancel() {
            }
        });
        KeyedStream<KeyValue,Tuple> groupSource=source.keyBy("key");
        DataStream<String> outStream= groupSource.process(new KeyedProcessFunction<Tuple, KeyValue, String>() {

            private transient ValueState<ListBuffuer> state=null;
            public void processElement(KeyValue value, Context ctx, Collector<String> out) throws Exception {
                if(state.value()==null){
                    state.update(new ListBuffuer());
                }
                state.update(state.value().addItem(value));
                ctx.timerService().registerProcessingTimeTimer(5*1000);
//                out.collect(generatorOutItem());
            }
            private String generatorOutItem() throws Exception{
                Iterator<KeyValue> kvIT = state.value().buffer.iterator();
                StringBuilder sb = new StringBuilder();
                String key = null;
                KeyValue kv;
                while (kvIT.hasNext()) {
                    kv = kvIT.next();
                    if (key == null) {
                        key = kv.getKey();
                    }
                    sb.append(kv.getKey() + ":" + kv.getValue()).append(",");
                }
                int idx=Integer.parseInt(key)%10;
                String outItem=idx+"->"+sb.toString()+"\n";
                return outItem;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<ListBuffuer> desc=new ValueStateDescriptor<ListBuffuer>("tState",ListBuffuer.class);
                // >>>>>>>>flink 1.6+ api
                StateTtlConfig ttlConfig=StateTtlConfig.newBuilder(Time.seconds(5)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build();
                desc.enableTimeToLive(ttlConfig);
                // <<<<<<<<<
                state=getRuntimeContext().getState(desc);
                super.open(parameters);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                out.collect(generatorOutItem());
            }
        });
        outStream.addSink(new SinkFunction<String>() {
            public void invoke(String value, Context context) throws Exception {
                System.out.println(value);
            }
        });

        System.out.println(streamingEnv.getStreamGraph().getStreamingPlanAsJSON());
        StreamGraph streamGraph=streamingEnv.getStreamGraph();
        JobGraph jobGraph=streamGraph.getJobGraph();
        System.out.println(jobGraph.toString());

        streamingEnv.execute("StatefulStreamingJob");
    }
}
