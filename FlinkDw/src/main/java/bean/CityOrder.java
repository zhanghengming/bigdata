package bean;

public class CityOrder {
    public String city;
    public String province;
    public Double totalMoney;
    public Long totalCount;

    public CityOrder() {
    }

    public CityOrder(String city, String province, Double totalMoney, Long totalCount) {
        this.city = city;
        this.province = province;
        this.totalMoney = totalMoney;
        this.totalCount = totalCount;
    }

    @Override
    public String toString() {
        return "CityOrder{" +
                "city='" + city + '\'' +
                ", province='" + province + '\'' +
                ", totalMoney=" + totalMoney +
                ", totalCount=" + totalCount +
                '}';
    }
}
