package model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author : zzy
 * @Date : 2021/10/07
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class AlertEvent {

    private String type;

    private MetricEvent metricEvent;

    private boolean recover;

    private Long triggerTime;

    private Long recoverTime;

}
