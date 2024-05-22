package bean;

public class OrderDetail {
    private String orderId;
    private String status;
    private String orderCreateTime;
    private Double price;

    public OrderDetail() {
    }

    public OrderDetail(String orderId, String status, String orderCreateTime, Double price) {
        this.orderId = orderId;
        this.status = status;
        this.orderCreateTime = orderCreateTime;
        this.price = price;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getOrderCreateTime() {
        return orderCreateTime;
    }

    public void setOrderCreateTime(String orderCreateTime) {
        this.orderCreateTime = orderCreateTime;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "OrderDetail{" +
                "orderId='" + orderId + '\'' +
                ", status='" + status + '\'' +
                ", orderCreateTime='" + orderCreateTime + '\'' +
                ", price=" + price +
                '}';
    }
}
