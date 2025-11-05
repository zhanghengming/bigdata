package com.common;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ：zhm
 * @version ：1.0
 * @since ：2025/11/5 15:18
 */
public class JdbcUtil {
    // 获取数据库连接
    public static Connection getConn() throws SQLException {
        // 注册驱动
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        // 获取连接
        Connection connection = DriverManager.getConnection("jdbc:mysql://192.168.56.101:3306/flink?useSSL=false&serverTimezone=UTC", "root", "root");

        return connection;
    }

    // 关闭数据库连接
    public static void close(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    // 查询数据
    public static <T>List<T> queryList(Connection conn, String sql, Class<T> clazz, boolean... isUnderlineToCamel) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
        List<T> list = new ArrayList<>();
        boolean defaultIsUnderlineToCamel = false;// 默认不转换驼峰命名
        if (isUnderlineToCamel.length > 0) {
            defaultIsUnderlineToCamel = isUnderlineToCamel[0];
        }
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery(sql);
        ResultSetMetaData r = rs.getMetaData();
        while (rs.next()) {
            // 创建对象用于接收查询结果，通过反射创建对象
            T t = clazz.newInstance();
            for (int i = 1; i <= r.getColumnCount(); i++) {
                String columnName = r.getColumnName(i);
                Object columnValue = rs.getObject(i);
                if (defaultIsUnderlineToCamel) {
                    columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                }
                BeanUtils.setProperty(t, columnName, columnValue);

            }
            list.add(t);
        }
        return list;
    }
}
