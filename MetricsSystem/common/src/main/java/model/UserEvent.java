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
public class UserEvent {

    private Long id;

    private String userName;

    private String email;

    private String phoneNumber;

    private String realName;

    private String displayName;

    private String avatarUrl;

    private String password;

    private String address;

    private int deviceSource;

}
