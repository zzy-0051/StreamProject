package utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author : zzy
 * @Date : 2021/09/28
 */

public class JdbcUtil {
    public static <T> List<T> query(Connection connection, String sql, Class<T> cls, Boolean underScoreToCamel) {

        ArrayList<T> result = new ArrayList<>();

        try {

            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            ResultSet resultSet = preparedStatement.executeQuery();

            ResultSetMetaData metaData = resultSet.getMetaData();

            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {

                T t = cls.newInstance();

                for (int i = 1; i <= columnCount; i++) {

                    String columnName = metaData.getColumnName(i);

                    if (underScoreToCamel) {
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }

                    Object value = resultSet.getObject(i);

                    BeanUtils.setProperty(t, columnName, value);
                }

                result.add(t);

            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }
}
