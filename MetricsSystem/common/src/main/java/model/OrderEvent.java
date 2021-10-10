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
public class OrderEvent {

    private Long id;

    private Long purchaseOrderId;

    private int deviceSource;

    private Long consumerId;

    private String consumerName;

    private String shopId;

    private String shopName;

    private int payStatus;

    private Long payAt;

    private Long payAmount;

    private int deliveryStatus;

    private int receiveStatus;

    private int reverseStatus;

    private Long shippingAt;

    private Long confirmAt;

    private String consumerNotes;
    
}
