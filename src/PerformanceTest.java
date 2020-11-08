import org.apache.hadoop.util.hash.Hash;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.*;

public class PerformanceTest {

    SparkBTree sbt;
    BTree bt;
    private String data_path;
    private int tree_order;
    private int num_entries;
    private int num_spark_threads;

    public PerformanceTest(String data_path, int tree_order, int spark_threads) throws FileNotFoundException {
        this.data_path = data_path;
        this.tree_order = tree_order;
        this.num_spark_threads = spark_threads;

        Scanner sc = new Scanner(new FileReader(data_path));
        sc.nextLine();

        HashMap<Integer, String[]> map = new HashMap();
        while (sc.hasNextLine()) {
            String[] record = sc.nextLine().split("\\s*\\;\\s*");
            int key = Integer.parseInt(record[0]);
            String[] value = Arrays.copyOfRange(record, 1, record.length);
            map.put(key, value);
        }

        num_entries = map.size();

        this.sbt = new SparkBTree(map, tree_order, spark_threads);

        this.bt = new BTree(tree_order);
        BTree.Entry[] entries_array = new BTree.Entry[map.size()];

        int counter = 0;
        for (Map.Entry e : map.entrySet()) {
            entries_array[counter] = this.bt.new Entry((Integer) e.getKey(), e.getValue());
            counter++;
        }

        this.bt.single_executioner.insert(entries_array);

    }

    public ArrayList<int[]> test_search(int beg, int end, int step, int samples_per_batch) {

        ArrayList<int[]> results = new ArrayList<int[]>();
        for (int n_keys_to_search = beg; n_keys_to_search <= end; n_keys_to_search += step) {

            System.out.println(n_keys_to_search);

            int single_duration = 0;
            int spark_duration = 0;

            if (n_keys_to_search == 0) {
                continue;
            }

            for (int i = 0; i < samples_per_batch; i++) {

                ArrayList<Integer> list = new ArrayList<Integer>(n_keys_to_search);
                Random random = new Random();

                for (int j = 0; j < n_keys_to_search; j++) {
                    list.add(random.nextInt(num_entries - 1));
                }

                long startTime = System.nanoTime();
                sbt.get(list);
                long endTime = System.nanoTime();
                spark_duration += (int) (endTime - startTime) / 1000000;

//                System.out.println("[Spark " + num_spark_threads + " threads/partitions] Searching " + n_keys_to_search + " keys among " + num_entries + " entries took : " + spark_duration + " ms");

                startTime = System.nanoTime();
                int[] array = list.stream().mapToInt(x -> x).toArray();
                bt.get(array, BTree.executionType.single_thread);
                endTime = System.nanoTime();
                single_duration += (int) (endTime - startTime) / 1000000;

//                System.out.println("[Single] Searching " + n_keys_to_search + " keys among " + num_entries + " entries took : " + single_duration + " ms");
            }

            results.add(new int[]{n_keys_to_search, single_duration / samples_per_batch, -1, spark_duration / samples_per_batch});
        }


        File csvOutputFile = new File("search_benchmark.csv");
        try (PrintWriter pw = new PrintWriter(csvOutputFile)) {
            pw.println("num_keys_to_search,single_thread,multi_thread,spark (" + this.num_spark_threads + " threads/partitions)");
            for (int[] r : results) {
                pw.println(r[0] + "," + r[1] + "," + r[2] + "," + r[3]);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return results;
    }
}
