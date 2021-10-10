package watermark;

import model.OutageMetricEvent;
import org.apache.flink.api.common.eventtime.*;

/**
 * @Author : zzy
 * @Date : 2021/10/08
 */

public class OutageMetricWatermarkStrategy implements WatermarkStrategy<OutageMetricEvent> {
    private long currentTimestamp = Long.MIN_VALUE;

    private final long maxTimeLag = 5000;

    @Override
    public WatermarkGenerator<OutageMetricEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<OutageMetricEvent>() {
            @Override
            public void onEvent(OutageMetricEvent event, long eventTimestamp, WatermarkOutput output) {
                Long timestamp = event.getTimestamp();
                currentTimestamp = Math.max(timestamp, currentTimestamp);
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput output) {
                output.emitWatermark(new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag));
            }
        };
    }
}
