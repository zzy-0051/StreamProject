package model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @Author : zzy
 * @Date : 2021/10/07
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OrderLineEvent {

    private Long orderLineId;

    private Long purchaseOrderId;

    private Long orderId;

    private Long consumerId;

    private String consumerName;

    private String shopId;

    private String shopName;

    private int payStatus;

    private Long payAt;

    private int deliveryStatus;

    private int receiveStatus;

    private int reverseStatus;

    private Long shippingAt;

    private Long confirmAt;

    private String consumerNotes;

    private int deviceSource;

    private String name;

    private Long id;

    private List<String> tags;

    private int count;

    private Long payAmount;

}
