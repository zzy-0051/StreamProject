package bean;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

/**
 * @Author : zzy
 * @Date : 2021/09/28
 */

@Data
public class ConfigTableBean {
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK = "clickhouse";

    String sourceTable;

    String operateType;

    String sinkType;

    String sinkTable;

    String sinkColumns;

    String sinkPk;

    String sinkExtend;
}
