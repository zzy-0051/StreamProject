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
public class AlertRule {
    private Integer id;
    private String name;
    private String measurement;
    private String thresholds;
}
