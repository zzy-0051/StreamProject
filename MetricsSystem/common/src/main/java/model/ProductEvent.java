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
public class ProductEvent {

    private Long id;

    private Long categoryId;

    private String code;

    private Long shopId;

    private String shopName;

    private Long brandId;

    private String brandName;

    private String name;

    private String imageUrl;

    private int status;

    private int type;

    private List<String> tags;

    private Long price;

}
