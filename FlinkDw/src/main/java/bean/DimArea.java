package bean;

public class DimArea {
    public Integer arearId;
    public String aname;
    public String cid;
    public String city;
    public String proid;
    public String province;

    public DimArea() {
    }

    public DimArea(Integer arearId, String aname, String cid, String city, String proid, String province) {
        this.arearId = arearId;
        this.aname = aname;
        this.cid = cid;
        this.city = city;
        this.proid = proid;
        this.province = province;
    }
}
