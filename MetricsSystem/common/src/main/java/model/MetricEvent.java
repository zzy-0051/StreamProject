package model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @Author : zzy
 * @Date : 2021/10/07
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class MetricEvent {

    private String name;

    private Long timestamp;

    private Map<String, Object> fields;

    private Map<String, String> tags;

}
