package tmnt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseDemo {
    public static void main(String[] args) {
        try {
            HbaseDao dao = new HbaseDao();
            List<String> cols = new ArrayList<String>();
            cols.add("name");
            cols.add("age");
            cols.add("school");
            dao.createTable("student", cols);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
