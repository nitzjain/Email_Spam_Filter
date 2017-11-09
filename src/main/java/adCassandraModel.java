import java.io.Serializable;

/**
 * Created by Manpreet Gandhi on 5/12/2016.
 */
public class adCassandraModel implements Serializable {
    private String adID;
    private String click;
    private int hour;
    private int c1;
    private int bannerPos;
    private String siteId;
    private String siteDomain;
    private String siteCategory;
    private String appId;
    private String appDomain;
    private String appCategory;
    private String deviceId;
    private String deviceIp;
    private String deviceModel;
    private int deviceType;
    private int deviceConnType;
    private int C14;
    private int C15;
    private int C16;
    private int C17;
    private int C18;
    private int C19;
    private int C20;
    private int C21;

    public adCassandraModel() {
    }

    public String getAdID() {
        return adID;
    }

    public void setAdID(String adID) {
        this.adID = adID;
    }

    public int getBannerPos() {
        return bannerPos;
    }

    public void setBannerPos(int bannerPos) {
        this.bannerPos = bannerPos;
    }

    public String getSiteId() {
        return siteId;
    }

    public void setSiteId(String siteId) {
        this.siteId = siteId;
    }

    public String getSiteDomain() {
        return siteDomain;
    }

    public void setSiteDomain(String siteDomain) {
        this.siteDomain = siteDomain;
    }

    public String getSiteCategory() {
        return siteCategory;
    }

    public void setSiteCategory(String siteCategory) {
        this.siteCategory = siteCategory;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getAppDomain() {
        return appDomain;
    }

    public void setAppDomain(String appDomain) {
        this.appDomain = appDomain;
    }

    public String getAppCategory() {
        return appCategory;
    }

    public void setAppCategory(String appCategory) {
        this.appCategory = appCategory;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getDeviceIp() {
        return deviceIp;
    }

    public void setDeviceIp(String deviceIp) {
        this.deviceIp = deviceIp;
    }

    public String getDeviceModel() {
        return deviceModel;
    }

    public void setDeviceModel(String deviceModel) {
        this.deviceModel = deviceModel;
    }

    public int getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(int deviceType) {
        this.deviceType = deviceType;
    }

    public int getDeviceConnType() {
        return deviceConnType;
    }

    public void setDeviceConnType(int deviceConnType) {
        this.deviceConnType = deviceConnType;
    }

    public int getC14() {
        return C14;
    }

    public void setC14(int c14) {
        C14 = c14;
    }

    public int getC15() {
        return C15;
    }

    public void setC15(int c15) {
        C15 = c15;
    }

    public int getC16() {
        return C16;
    }

    public void setC16(int c16) {
        C16 = c16;
    }

    public int getC17() {
        return C17;
    }

    public void setC17(int c17) {
        C17 = c17;
    }

    public int getC18() {
        return C18;
    }

    public void setC18(int c18) {
        C18 = c18;
    }

    public int getC19() {
        return C19;
    }

    public void setC19(int c19) {
        C19 = c19;
    }

    public int getC20() {
        return C20;
    }

    public void setC20(int c20) {
        C20 = c20;
    }

    public int getC21() {
        return C21;
    }

    public void setC21(int c21) {
        C21 = c21;
    }

    public int getC1() {
        return c1;
    }

    public void setC1(int c1) {
        this.c1 = c1;
    }

    public int getHour() {
        return hour;
    }

    public void setHour(int hour) {
        this.hour = hour;
    }

    public String getClick() {
        return click;
    }

    public void setClick(String click) {
        this.click = click;
    }
}
