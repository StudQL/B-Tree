import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class PerformanceTest {

    SparkBTree sbt;
    BTree bt;
    private int tree_order;
    private int num_entries_at_initialization;
    private int num_threads;
    private HashMap<Integer, String[]> map = new HashMap();

    public PerformanceTest(String data_path, int tree_order, int num_threads) throws FileNotFoundException {
        this.tree_order = tree_order;

        if (num_threads <= 0 || num_threads > Runtime.getRuntime().availableProcessors()) {
            this.num_threads = Runtime.getRuntime().availableProcessors();
        } else {
            this.num_threads = num_threads;
        }

        Scanner sc = new Scanner(new FileReader(data_path));
        sc.nextLine();

        while (sc.hasNextLine()) {
            String[] record = sc.nextLine().split("\\s*\\;\\s*");
            int key = Integer.parseInt(record[0]);
            String[] value = Arrays.copyOfRange(record, 1, record.length);
            map.put(key, value);
        }

        num_entries_at_initialization = map.size();

        this.sbt = new SparkBTree(map, tree_order, num_threads);

        this.bt = new BTree(tree_order);
        BTree.Entry[] entries_array = new BTree.Entry[map.size()];

        int counter = 0;
        for (Map.Entry e : map.entrySet()) {
            entries_array[counter] = this.bt.new Entry((Integer) e.getKey(), e.getValue());
            counter++;
        }

        this.bt.single_executioner.insert(entries_array);

    }

    public ArrayList<long[]> test_search(int beg, int end, int step, int samples_per_batch) {
        MultiExecutioner mExecutioner = new MultiExecutioner(bt);

        ArrayList<long[]> results = new ArrayList<long[]>();
        for (int n_keys_to_search = beg; n_keys_to_search <= end; n_keys_to_search += step) {

            System.out.println(n_keys_to_search / (float) end * 100 + "%");

            long single_duration = 0;
            long spark_duration = 0;
            long multi_duration = 0;

            if (n_keys_to_search == 0) {
                continue;
            }
            for (int i = 0; i < samples_per_batch; i++) {
                ArrayList<Integer> list = new ArrayList<Integer>(n_keys_to_search);
                Random random = new Random();

                for (int j = 0; j < n_keys_to_search; j++) {
                    list.add(random.nextInt(num_entries_at_initialization - 1));
                }
                int[] array = list.stream().mapToInt(x -> x).toArray();

                // SPARK
                long startTime = System.nanoTime();
                sbt.get(list);
                long endTime = System.nanoTime();
                spark_duration += (long) (endTime - startTime);

                //Multi-threading
                List<Callable<String>> tasks = new ArrayList<>();
                ArrayList<int[]> array_decomposed = decompose(array, num_threads);
                for (int[] array_part : array_decomposed) {
                    Callable<String> task = mExecutioner.search_task(array_part, 0);
                    tasks.add(task);
                }
                ExecutorService executor_multi = mExecutioner.initialize_executor(num_threads);
                long startTime2 = System.nanoTime();
                List<Future<String>> futures2 = mExecutioner.execute_tasks(executor_multi, tasks);
                long endTime2 = System.nanoTime();
                multi_duration += (int) (endTime2 - startTime2);
                mExecutioner.stop_executor(executor_multi);

                //Single-threading
                long startTime3 = System.nanoTime();
                bt.get(array, BTree.executionType.single_thread);
                long endTime3 = System.nanoTime();
                single_duration += (long) (endTime3 - startTime3);

            }

            results.add(new long[]{n_keys_to_search, single_duration / samples_per_batch, multi_duration / samples_per_batch, spark_duration / samples_per_batch});
        }

        LocalTime time = LocalTime.now();
        write_file(results, "test_results_search" + time.getHour() + "-" + time.getMinute() + "-" + time.getSecond() + ".csv");
        return results;
    }

    public ArrayList<long[]> test_insert(int beg, int end, int step, int samples_per_batch) {
        MultiExecutioner mExecutioner = new MultiExecutioner(bt);
        ArrayList<long[]> results = new ArrayList<long[]>();

        for (int n_entries_to_insert = beg; n_entries_to_insert <= end; n_entries_to_insert += step) {

            System.out.println(n_entries_to_insert / (float) end * 100 + "%");

            long single_duration = 0;
            long multi_duration = 0;

            if (n_entries_to_insert == 0) {
                continue;
            }
            for (int i = 0; i < samples_per_batch; i++) {
                ArrayList<Integer> list = new ArrayList<Integer>(n_entries_to_insert);
                Random random = new Random();
                List<Callable<String>> tasks = new ArrayList<>();

                //Multi-threading

                int[] array = random.ints(n_entries_to_insert, 0, num_entries_at_initialization * 3).toArray();
                BTree.Entry[] entries_array = new BTree.Entry[n_entries_to_insert];
                for (int e = 0; e < n_entries_to_insert; e++) {
                    entries_array[e] = bt.new Entry(array[i], array[i]);
                }
                ExecutorService executor_multi = mExecutioner.initialize_executor(num_threads);
                for (BTree.Entry entry : entries_array) {
                    Callable<String> task = mExecutioner.insert_task(entry, 0);
                    tasks.add(task);
                }
                long startTime2 = System.nanoTime();
                List<Future<String>> futures2 = mExecutioner.execute_tasks(executor_multi, tasks);
                long endTime2 = System.nanoTime();
                mExecutioner.stop_executor(executor_multi);
                multi_duration += (endTime2 - startTime2);

                //Single-threading
                long startTime3 = System.nanoTime();
                bt.insert(entries_array, BTree.executionType.single_thread);
                long endTime3 = System.nanoTime();
                single_duration += (endTime3 - startTime3);

            }

            results.add(new long[]{n_entries_to_insert, single_duration / samples_per_batch, multi_duration / samples_per_batch, -1});
        }

        LocalTime time = LocalTime.now();
        write_file(results, "test_results_insert" + time.getHour() + "-" + time.getMinute() + "-" + time.getSecond() + ".csv");
        return results;
    }

    public ArrayList<long[]> test_delete(int beg, int end, int step, int samples_per_batch) throws Exception {


        ArrayList<long[]> results = new ArrayList<long[]>();
        for (int n_keys_to_delete = beg; n_keys_to_delete <= end; n_keys_to_delete += step) {

            System.out.println(n_keys_to_delete / (float) end * 100 + "%");

            long single_duration = 0;
            long multi_duration = 0;

            if (n_keys_to_delete == 0) {
                continue;
            }
            for (int i = 0; i < samples_per_batch; i++) {
                BTree btbis = new BTree(tree_order);
                BTree.Entry[] entries_array = new BTree.Entry[map.size()];

                int counter = 0;
                for (Map.Entry e : map.entrySet()) {
                    entries_array[counter] = btbis.new Entry((Integer) e.getKey(), e.getValue());
                    counter++;
                }

                btbis.single_executioner.insert(entries_array);

                MultiExecutioner mExecutioner = new MultiExecutioner(btbis);
                ArrayList<Integer> list = new ArrayList<Integer>(n_keys_to_delete);
                Random random = new Random();

                //Single-threading
                int[] array = random.ints(n_keys_to_delete, 0, num_entries_at_initialization / 2).toArray();

                long startTime3 = System.nanoTime();
                try {
                    btbis.delete(array, BTree.executionType.single_thread);
                } catch (Exception e) {
                }


                long endTime3 = System.nanoTime();
                single_duration += (endTime3 - startTime3);

                //Multi-threading
                array = random.ints(n_keys_to_delete, num_entries_at_initialization / 2, num_entries_at_initialization).toArray();
                List<Callable<String>> tasks = new ArrayList<>();
                ArrayList<int[]> array_decomposed = decompose(array, num_threads);
                for (int[] array_part : array_decomposed) {
                    Callable<String> task = mExecutioner.delete_task(array_part, 0);
                    tasks.add(task);
                }
                ExecutorService executor_multi = mExecutioner.initialize_executor(num_threads);
                long startTime2 = System.nanoTime();
                mExecutioner.execute_tasks(executor_multi, tasks);
                long endTime2 = System.nanoTime();
                mExecutioner.stop_executor(executor_multi);
                multi_duration += (endTime2 - startTime2);


            }

            results.add(new long[]{n_keys_to_delete, single_duration / samples_per_batch, multi_duration / samples_per_batch, -1});
        }

        LocalTime time = LocalTime.now();
        write_file(results, "test_results_delete" + time.getHour() + "-" + time.getMinute() + "-" + time.getSecond() + ".csv");
        return results;
    }


    public ArrayList<int[]> decompose(int[] array, int num_threads) {
        int length_array = 0;
        int last_array_length = 0;
        ArrayList<int[]> array_decomposed = new ArrayList<int[]>();
        int pointer = 0;

        if (array.length % num_threads == 0) {
            length_array = array.length / num_threads;
            last_array_length = array.length / num_threads;
        } else {
            length_array = array.length / num_threads;
            last_array_length = array.length % num_threads;
        }
        for (int i = 0; i < num_threads; i++) {
            if (i == num_threads - 1) {
                int[] array_part = Arrays.copyOfRange(array, pointer, pointer + last_array_length - 1);
                array_decomposed.add(array_part);
            } else {
                int[] array_part = Arrays.copyOfRange(array, pointer, pointer + length_array - 1);
                pointer += length_array;
                array_decomposed.add(array_part);
            }
        }
        return array_decomposed;
    }

    public void write_file(ArrayList<long[]> results, String filename) {
        File csvOutputFile = new File(filename);
        try (PrintWriter pw = new PrintWriter(csvOutputFile)) {
            pw.println("num_keys;single_thread;multi_thread (" + this.num_threads + " threads);spark (" + this.num_threads + " threads/partitions)");
            for (long[] r : results) {
                pw.println(r[0] + ";" + r[1] + ";" + r[2] + ";" + r[3]);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
