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
public class ShopEvent {

    private Long id;

    private String name;

    private Long ownerId;

    private String ownerName;

    private int status;

    private int type;

    private String phone;

    private String email;

    private String address;

    private String imageUrl;

}
