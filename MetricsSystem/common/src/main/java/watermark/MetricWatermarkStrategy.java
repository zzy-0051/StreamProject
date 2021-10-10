package watermark;

import model.MetricEvent;
import org.apache.flink.api.common.eventtime.*;

/**
 * @Author : zzy
 * @Date : 2021/10/07
 */

public class MetricWatermarkStrategy implements WatermarkStrategy<MetricEvent> {
    @Override
    public WatermarkGenerator<MetricEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<MetricEvent>() {
            private long currentTimestamp = Long.MIN_VALUE;
            long maxTimeLag =5000;
            @Override
            public void onEvent(MetricEvent event, long eventTimestamp, WatermarkOutput output) {
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
