import java.io.Serializable;

/**
 * Created by ssah22 on 9/22/2017.
 */
public class Student implements Serializable{
    int sid;

    public int getSid() {
        return sid;
    }

    public String getSection() {
        return section;
    }

    public String getQid() {
        return qid;
    }

    public String getD_level() {
        return d_level;
    }

    @Override
    public String toString() {
        return "Student{" +
                "sid=" + sid +
                ", section='" + section + '\'' +
                ", qid='" + qid + '\'' +
                ", d_level='" + d_level + '\'' +
                ", combinedKey='" + combinedKey + '\'' +
                '}';
    }

    String section;
    String qid;
    String d_level;
    String combinedKey;

    public Student(int sid, String section, String qid, String d_level) {
        this.sid = sid;
        this.section = section;
        this.qid = qid;
        this.d_level = d_level;
        this.combinedKey = sid+" "+section;
    }
}
