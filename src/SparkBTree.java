import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;

public class SparkBTree<T> implements Serializable {
    private final int min_key;
    private final int max_key;
    private final int num_partitions;
    private final int num_entries_per_partition;

    protected JavaRDD data;

    public SparkBTree(HashMap<Integer, T> input_data, int tree_order, int spark_threads) {

        System.setProperty("hadoop.home.dir", "C:/hadoop-2.7.4/");
        num_partitions = spark_threads;
        SparkConf conf = new SparkConf().setAppName("BTree in local Spark").setMaster("local[" + num_partitions + "]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        min_key = Collections.min(input_data.keySet());
        max_key = Collections.max(input_data.keySet());
        num_entries_per_partition = (int) Math.ceil((max_key - min_key) / (float) num_partitions);

        List<Tuple2<Integer, T>> input_pairs = new ArrayList<>();

        for (Map.Entry e : input_data.entrySet()) {
            input_pairs.add(new Tuple2<Integer, T>((int) e.getKey(), (T) e.getValue()));
        }

        JavaPairRDD<Integer, T> input_rdd = sc.parallelizePairs(input_pairs);
        input_rdd = input_rdd.partitionBy(new CustomPartitioner());

        data = input_rdd.mapPartitions((iter) -> {
            BTree bt = new BTree(tree_order);
            while (iter.hasNext()) {
                Tuple2<Integer, T> next = iter.next();
                int key = next._1;
                T value = next._2;
                BTree.Entry[] entry = new BTree.Entry[1];
                entry[0] = bt.new Entry(key, value);
                bt.insert(entry, BTree.executionType.single_thread);
            }
            List<BTree> result = new ArrayList<BTree>();
            result.add(bt);
            return result.iterator();
        }, true);
        System.out.println("Number of threads/partitions : " + data.getNumPartitions());
    }

    public JavaRDD<BTree.Entry> get(List<Integer> keys) {
        class queryFilter implements Function2<Integer, Iterator<BTree>, Iterator<BTree.Entry>> {

            List<Integer> keys;

            public queryFilter(List<Integer> keys) {
                this.keys = keys;
            }

            @Override
            public Iterator<BTree.Entry> call(Integer integer, Iterator<BTree> bTreeIterator) throws Exception {
                if (bTreeIterator.hasNext()) {
                    List<Integer> keysToSearch = new ArrayList<>();
                    for (int i = 0; i < keys.size(); i++) {
                        Integer current = keys.get(i);
                        if (getKeyPartition(current) == integer) {
                            keysToSearch.add(current);
                        }
                    }
                    BTree bt = bTreeIterator.next();
                    if (!keysToSearch.isEmpty()) {
                        List<BTree.Entry> a = Arrays.asList(bt.get(keysToSearch.stream().mapToInt(x -> x).toArray(), BTree.executionType.single_thread));
                        return a.iterator();
                    }
                }
                return new ArrayList<BTree.Entry>().iterator();
            }
        }

        JavaRDD<BTree.Entry> output = data.mapPartitionsWithIndex(new queryFilter(keys), false);
        return output;
    }

    public void insert(HashMap<Integer, T> input_data) {
        class queryFilter implements Function2<Integer, Iterator<BTree>, Iterator<BTree.Entry>> {

            HashMap<Integer, T> input_data;

            public queryFilter(HashMap<Integer, T> input_data) {
                this.input_data = input_data;
            }

            @Override
            public Iterator<BTree.Entry> call(Integer integer, Iterator<BTree> bTreeIterator) {
                if (bTreeIterator.hasNext()) {
                    ArrayList<BTree.Entry<T>> entries = new ArrayList<>();
                    BTree bt = bTreeIterator.next();
                    for (Map.Entry<Integer, T> e : input_data.entrySet()) {
                        if (getKeyPartition(e.getKey()) == integer) {
                            entries.add(bt.new Entry<>(e.getKey(), e.getValue()));
                        }
                    }
                    if (!entries.isEmpty()) {
                        bt.insert(entries.toArray(new BTree.Entry[entries.size()]), BTree.executionType.single_thread);
                    }
                }
                return null;
            }
        }

        data.mapPartitionsWithIndex(new queryFilter(input_data), false);
    }


    private int getKeyPartition(int key) {
        int partition_number = (key - min_key) / num_entries_per_partition;
        if (partition_number < 0) {
            return 0;
        } else {
            return Math.min(partition_number, num_partitions - 1);
        }
    }

    private class CustomPartitioner extends Partitioner {

        public CustomPartitioner() {
            super();
        }

        @Override
        public int numPartitions() {
            return num_partitions;
        }

        @Override
        public int getPartition(Object key) {
            return getKeyPartition((Integer) key);
        }
    }
}
