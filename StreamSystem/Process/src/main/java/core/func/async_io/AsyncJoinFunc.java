package core.func.async_io;

import com.alibaba.fastjson.JSONObject;

public interface AsyncJoinFunc<T> {
    String getKey(T input);

    void join(T input, JSONObject dimInfo) throws Exception;
}
