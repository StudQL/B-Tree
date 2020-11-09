import java.util.ArrayList;

public class main {
    public static void main(String[] args) throws Exception {

//         Tests section. Long runtime.

        PerformanceTest t;
        ArrayList<int[]> r;
        t = new PerformanceTest("./generated_data.csv", 100, 12);

        t.test_search(0, 1000000, 20000, 5);
        t.test_search(0, 8000, 50, 10);

        t.test_insert(0, 100000, 20000, 5);
        t.test_insert(0, 8000, 50, 10);

        t.test_delete(0, 8000, 500, 1);


    }
}