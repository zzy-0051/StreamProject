package util;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URI;

import static constant.PropertiesConstant.*;

/**
 * @Author : zzy
 * @Date : 2021/10/07
 */

public class CheckpointUtil {

    public static StreamExecutionEnvironment setCheckpointConfig(StreamExecutionEnvironment env, ParameterTool parameterTool) throws Exception {

        if (parameterTool.getBoolean(STREAM_CHECKPOINT_ENABLE, false)
                && CHECKPOINT_MEMORY.equals(parameterTool.get(STREAM_CHECKPOINT_TYPE).toLowerCase())) {
            //1、state in memory 5M
            env.setStateBackend(new HashMapStateBackend());
            env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage(5 * 1024 * 1024 * 100));
            env.enableCheckpointing(parameterTool.getLong(STREAM_CHECKPOINT_INTERVAL, 60000));
        }

        if (parameterTool.getBoolean(STREAM_CHECKPOINT_ENABLE, false)
                && CHECKPOINT_FS.equals(parameterTool.get(STREAM_CHECKPOINT_TYPE).toLowerCase())) {
            env.setStateBackend(new HashMapStateBackend());
            env.getCheckpointConfig().setCheckpointStorage(new URI(parameterTool.get(STREAM_CHECKPOINT_DIR)));
            env.enableCheckpointing(parameterTool.getLong(STREAM_CHECKPOINT_INTERVAL, 60000));
        }

        if (parameterTool.getBoolean(STREAM_CHECKPOINT_ENABLE, false)
                && CHECKPOINT_ROCKSDB.equals(parameterTool.get(STREAM_CHECKPOINT_TYPE).toLowerCase())) {
            env.setStateBackend(new EmbeddedRocksDBStateBackend());
            env.getCheckpointConfig().setCheckpointStorage(parameterTool.get(STREAM_CHECKPOINT_DIR));
        }

        //  set checkpoint_interval
        env.enableCheckpointing(parameterTool.getLong(STREAM_CHECKPOINT_INTERVAL, 60000));

        //  advanced setting
        //  set exactly-once
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        env.getCheckpointConfig().setCheckpointTimeout(60000);

        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);

        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        return env;
    }
}
