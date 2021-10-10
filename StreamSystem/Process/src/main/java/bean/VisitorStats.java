package bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class VisitorStats {

    private String stt;

    private String edt;

    private String vc;

    private String ch;

    private String ar;

    private String is_new;

    private Long uv_ct = 0L;

    private Long pv_ct = 0L;

    private Long sv_ct = 0L;

    private Long uj_ct = 0L;

    private Long dur_sum = 0L;

    private Long ts;
}
