package bean;

public class TradeOrder {
    public String orderNo;

    public Integer orderId;

    public Double totalMoney;

    public Integer userId;

    public Integer areaId;

    public Integer status;

    public TradeOrder() {
    }

    public TradeOrder(String orderNo, Integer orderId, Double totalMoney, Integer userId, Integer areaId, Integer status) {
        this.orderNo = orderNo;
        this.orderId = orderId;
        this.totalMoney = totalMoney;
        this.userId = userId;
        this.areaId = areaId;
        this.status = status;
    }

    @Override
    public String toString() {
        return "TradeOrder{" +
                "orderNo='" + orderNo + '\'' +
                ", orderId=" + orderId +
                ", totalMoney=" + totalMoney +
                ", userId=" + userId +
                ", areaId=" + areaId +
                ", status=" + status +
                '}';
    }
}
