import java.io.Serializable;

/**
 * Created by lyen on 17-6-2.
 */
public class Record implements Serializable {
    private String name;
    private String age;
    private String gender;
    private String location;
    private String money;
    private String phone;

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public Record(String name, String age, String gender, String location, String money, String phone) {
        this.name = name;
        this.age = age;
        this.gender = gender;
        this.location = location;
        this.money = money;
        this.phone = phone;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getMoney() {
        return money;
    }

    public void setMoney(String money) {
        this.money = money;
    }

    public String toString() {
        String str = getPhone() + "\001" + getName() + "\001" + getGender() + "\001" + getAge() + "\001" + getLocation() + "\001" + getMoney();
        return str;
    }
}
