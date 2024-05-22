package bean;

import java.io.Serializable;

/**
 * 将json中的数据封装成一个对象
 */
public class TableObject implements Serializable {
    private String database;
    private String tableName;
    private String typeInfo;
    private String dataInfo;

    public TableObject(String database, String tableName, String typeInfo, String dataInfo) {
        this.database = database;
        this.tableName = tableName;
        this.typeInfo = typeInfo;
        this.dataInfo = dataInfo;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTypeInfo() {
        return typeInfo;
    }

    public void setTypeInfo(String typeInfo) {
        this.typeInfo = typeInfo;
    }

    public String getDataInfo() {
        return dataInfo;
    }

    public void setDataInfo(String dataInfo) {
        this.dataInfo = dataInfo;
    }

    @Override
    public String toString() {
        return "TableObject{" +
                "database='" + database + '\'' +
                ", tableName='" + tableName + '\'' +
                ", typeInfo='" + typeInfo + '\'' +
                ", dataInfo='" + dataInfo + '\'' +
                '}';
    }
}
