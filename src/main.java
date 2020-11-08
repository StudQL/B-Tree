import guru.nidi.graphviz.attribute.Color;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.Graph;
import guru.nidi.graphviz.model.MutableGraph;
import guru.nidi.graphviz.model.MutableNode;
import guru.nidi.graphviz.model.Node;

import javax.swing.*;
import java.io.*;
import java.sql.Time;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static guru.nidi.graphviz.model.Factory.*;

public class main {
    public static void main(String[] args) throws Exception {

    //BTree bTree = random_tree(60,9);
        System.setProperty("java.awt.headless", "false");

        // Multi-threading
        BTree bTree = test_multi(5);

        /*String rel_path = graphviz_display(bTree.rootNode, null, null, "1. Build tree");

        JFrame f = new JFrame("1. Build tree");
        ImageIcon i = new ImageIcon(rel_path);
        f.setSize(i.getIconWidth(), i.getIconHeight() + 50);
        f.add(new JPanel().add(new JLabel(i)));
        f.setLocationRelativeTo(null);
        f.show();
        */

        //Single-threading
        /*
        BTree bTree = test_insertion(5);
        String rel_path = graphviz_display(bTree.rootNode, null, null, "1. Build tree");

        JFrame f = new JFrame("1. Build tree");
        ImageIcon i = new ImageIcon(rel_path);
        f.setSize(i.getIconWidth(), i.getIconHeight() + 50);
        f.add(new JPanel().add(new JLabel(i)));
        f.setLocationRelativeTo(null);
        f.show();


        int ops_counter = 1;

        while (true) {
            System.out.println("Insert or delete a node (example : 'delete 15', 'insert 45):\n");
            Scanner sc = new Scanner(System.in);
            String user_command = sc.nextLine();
            String[] user_command_array = user_command.strip().split("\\s+");
            boolean valid_user_command = true;

            if (user_command_array.length == 2) {
                int key = Integer.parseInt(user_command_array[1]);
                if (user_command_array[0].equals("delete")) {
                    bTree.single_executioner.delete(new int[]{key});
                    rel_path = graphviz_display(bTree.rootNode, null, null, ops_counter + ". " + user_command.strip());
                    ops_counter++;
                } else if (user_command_array[0].equals("insert")) {
                    bTree.single_executioner.insert(new BTree.Entry[]{bTree.new Entry(key, key)});
                    rel_path = graphviz_display(bTree.rootNode, null, null, ops_counter + ". " + user_command.strip());
                    ops_counter++;
                } else {
                    System.out.println("error");
                    valid_user_command = false;
                }

                if(valid_user_command){
                    f = new JFrame(ops_counter + ". " + user_command.strip());
                    i = new ImageIcon(rel_path);
                    f.setSize(i.getIconWidth(), i.getIconHeight() + 50);
                    f.add(new JPanel().add(new JLabel(i)));
                    f.setLocationRelativeTo(null);
                    f.show();
                }
            }
        }
        */
    }

    public static BTree test_insertion(int tree_order) throws Exception {

        BufferedReader br = new BufferedReader(new FileReader("B-Tree/data.txt"));
        String line[] = br.readLine().split("\\s+");
        System.out.println(Arrays.toString(line));

        BTree bT = new BTree(tree_order);
        SingleExecutioner sExecutioner = new SingleExecutioner(bT);

        // create an array of entries
        BTree.Entry entries[] = new BTree.Entry[line.length];
        for (int i = 0; i < line.length; i++) {
            int key = Integer.parseInt(line[i]);
            entries[i] = bT.new Entry(key, key);
        }

        sExecutioner.insert(entries);
        return bT;
    }

    public static BTree test_multi(int tree_order) throws Exception {

        BTree bT = new BTree(tree_order);
        MultiExecutioner mExecutioner = new MultiExecutioner(bT);
        // create an array of entries
        BufferedReader br = new BufferedReader(new FileReader("B-Tree/data.txt"));
        String line[] = br.readLine().split("\\s+");
        BTree.Entry entries[] = new BTree.Entry[line.length];
        Random random = new Random();
        int[] array = random.ints(10000, 0, 10000).toArray();
        for (int i = 0; i < line.length; i++) {
            int key = Integer.parseInt(line[i]);
            entries[i] = bT.new Entry(key, key);
        }
        BTree.Entry[] entries_array = new BTree.Entry[10000];
        for (int i = 0; i < 10000; i++) {
            entries_array[i] = bT.new Entry(array[i], array[i]);
        }
        // Initialize Executor
        ExecutorService executor_multi = mExecutioner.initialize_executor();
        ExecutorService executor_single = mExecutioner.initialize_executor_single();
        // Insert Tasks - creation of the Tree
        List<Callable<String>> tasks = new ArrayList<>();
        int id = 0;
        for (BTree.Entry entry : entries) {
            //System.out.println(entry);
            try {
                Callable<String> task = mExecutioner.insert_task(entry, id);
                tasks.add(task);
            } catch (Exception e) {
                e.printStackTrace();
            }
            id++;
        }
        for (BTree.Entry entry : entries_array) {
            try {
                Callable<String> task = mExecutioner.insert_task(entry, id);
                tasks.add(task);
            } catch (Exception e) {
                e.printStackTrace();
            }
            id++;
        }
        List<Future<String>> futures = mExecutioner.execute_tasks(executor_single, tasks);
        // Test_data
        long[][] data = test_search(bT, mExecutioner, executor_multi, 4, false);
        write_file(data, "multi", "B-Tree/test_data/multi_search.csv");
        mExecutioner.stop_executor(executor_multi);
        mExecutioner.stop_executor(executor_single);
        return bT;
    }
    public static long[][] test_insert(BTree bTree, MultiExecutioner mExecutioner, ExecutorService executor,  int nb_epochs, boolean single){
        long[][] data = new long[1001][2];
        int idx = 1;
        data[0][0]= 0;
        for(int j = 8; j <= 80000; j+=80) {
            long tmp_data = 0;
            for (int i = 0; i < nb_epochs; i++) {
                if (single) {
                    List<Callable<String>> tasks2 = new ArrayList<>();
                    BTree.Entry[] entries_array = new BTree.Entry[j];
                    Random random = new Random();
                    int[] array = random.ints(j, 0,j).toArray();
                    for (int e = 0; e < j; e++) {
                        entries_array[e] = bTree.new Entry(array[i], array[i]);
                    }
                    // System.out.println(search2.length);
                    for (BTree.Entry entry :entries_array ) {
                        try {
                            Callable<String> task = mExecutioner.insert_task(entry,0);
                            tasks2.add(task);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    long start_search = System.currentTimeMillis();
                    List<Future<String>> futures2 = mExecutioner.execute_tasks(executor, tasks2);
                    long end_search = System.currentTimeMillis();
                    tmp_data += end_search - start_search;
                } else {
                    List<Callable<String>> tasks2 = new ArrayList<>();
                    BTree.Entry[] entries_array = new BTree.Entry[j];
                    Random random = new Random();
                    int[] array = random.ints(j, 0,j).toArray();
                    for (int e = 0; e < j; e++) {
                        entries_array[e] = bTree.new Entry(array[i], array[i]);
                    }
                    // System.out.println(search2.length);
                    for (BTree.Entry entry :entries_array ) {
                        try {
                            Callable<String> task = mExecutioner.insert_task(entry,0);
                            tasks2.add(task);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    long start_search = System.currentTimeMillis();
                    List<Future<String>> futures2 = mExecutioner.execute_tasks(executor, tasks2);
                    long end_search = System.currentTimeMillis();
                    tmp_data += end_search - start_search;
                }
            }
            data[idx][0] = data[idx - 1][0] + tmp_data / nb_epochs;
            data[idx][1] = j;
            idx++;
            System.out.println(idx);
        }
        return data;
    }

    public static long[][] test_delete(BTree bTree, MultiExecutioner mExecutioner, ExecutorService executor,  int nb_epochs, boolean single){
        long[][] data = new long[1001][2];
        int idx = 1;
        data[0][0]= 0;
        for(int j = 8; j <= 80000; j+=80) {
            long tmp_data = 0;
            for (int i = 0; i < nb_epochs; i++) {
                if (single) {
                    // Delete Tasks
                    List<Callable<String>> tasks3 = new ArrayList<>();
                    int id3 = 0;
                    int[] keys = new int[]{92, 75, 60};
                    for(int key : keys)
                    {

                        try {
                            Callable<String> task = mExecutioner.delete_task(new int[]{key}, id3);
                            tasks3.add(task);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        id3++;
                    }
                    List<Future<String>> futures3 = mExecutioner.execute_tasks(executor, tasks3);
                    for(Future<String> future : futures3){
                        try {
                            System.out.println(future.get());
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        }
                    }
                    long start_search = System.currentTimeMillis();
                    List<Future<String>> futures2 = mExecutioner.execute_tasks(executor, tasks3);
                    long end_search = System.currentTimeMillis();
                    tmp_data += end_search - start_search;
                } else {
                    List<Callable<String>> tasks2 = new ArrayList<>();
                    int[] search = new int[]{92, 75, 60, 45, 50, 27, 4, 78};
                    int[] search2 = new int[j];
                    for (int k = 0; k < j / 8; k++) {
                        System.arraycopy(search, 0, search2, 8 * k, 8);
                    }
                    int pointer = 0;
                    for (int h = 0; h < 8; h++) {
                        try {
                            int coeff = search2.length / 8;
                            Callable<String> task = mExecutioner.search_task(Arrays.copyOfRange(search2, pointer, pointer + coeff - 1), h);
                            pointer += coeff;
                            System.out.println(Arrays.copyOfRange(search2, pointer, pointer + coeff - 1).length);
                            tasks2.add(task);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    long start_search = System.currentTimeMillis();
                    List<Future<String>> futures2 = mExecutioner.execute_tasks(executor, tasks2);
                    long end_search = System.currentTimeMillis();
                    tmp_data += end_search - start_search;
                }
            }
            data[idx][0] = data[idx - 1][0] + tmp_data / nb_epochs;
            data[idx][1] = j;
            idx++;
            //System.out.println(idx);
        }
        return data;
    }

    public static long[][] test_search(BTree bTree, MultiExecutioner mExecutioner, ExecutorService executor,  int nb_epochs, boolean single){
        long[][] data = new long[1001][2];
        int idx = 1;
        data[0][0]= 0;
        for(int j = 8; j <= 80000; j+=80) {
            long tmp_data = 0;
            for (int i = 0; i < nb_epochs; i++) {
                if(single) {
                    List<Callable<String>> tasks2 = new ArrayList<>();
                    int[] search = new int[]{92, 75, 60, 45, 50, 27, 4, 78};
                    int[] search2 = new int[j];
                    for (int k = 0; k < j / 8; k++) {
                        System.arraycopy(search, 0, search2, 8 * k, 8);
                    }
                   // System.out.println(search2.length);
                    try {
                        Callable<String> task = mExecutioner.search_task(search2, 0);
                        tasks2.add(task);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    long start_search = System.currentTimeMillis();
                    List<Future<String>> futures2 = mExecutioner.execute_tasks(executor, tasks2);
                    long end_search = System.currentTimeMillis();
                    tmp_data += end_search - start_search;
                }
                else {
                    List<Callable<String>> tasks2 = new ArrayList<>();
                    int[] search = new int[]{92, 75, 60, 45, 50, 27, 4, 78};
                    int[] search2 = new int[j];
                    for (int k = 0; k < j / 8; k++) {
                        System.arraycopy(search, 0, search2, 8 * k, 8);
                    }
                    int pointer = 0;
                    for(int h = 0; h < 8; h++){
                        try{
                            int coeff = search2.length/8;
                            Callable<String> task = mExecutioner.search_task(Arrays.copyOfRange(search2, pointer, pointer+coeff-1), h);
                            pointer += coeff;
                            tasks2.add(task);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    long start_search = System.currentTimeMillis();
                    List<Future<String>> futures2 = mExecutioner.execute_tasks(executor, tasks2);
                    long end_search = System.currentTimeMillis();
                    tmp_data += end_search - start_search;
                }
            }
            data[idx][0] = data[idx-1][0] + tmp_data/nb_epochs;
            data[idx][1] = j;
            idx++;
            //System.out.println(idx);
        }
        return data;
    }

    public static void write_file(long[][] data, String executionType, String filename){
        try( BufferedWriter bw = new BufferedWriter(new FileWriter(filename))) {
            bw.write(executionType +";Number of inserts");
            for (int i = 0; i < data.length; i++) {
                for (int j = 0; j < 2; j++) {
                    bw.write(data[i][j] + ((j == data[i].length-1) ? "" : ";"));
                }
                bw.newLine();
            }
            bw.flush();
        } catch (IOException e) {}

    }

    public static BTree random_tree(int n_nodes, int tree_order) {
        BTree bT = new BTree(tree_order);
        Random random = new Random();
        int[] array = random.ints(n_nodes, 0, n_nodes).toArray();
        BTree.Entry[] entries_array = new BTree.Entry[n_nodes];
        for (int i = 0; i < n_nodes; i++) {
            entries_array[i] = bT.new Entry(array[i], array[i]);
        }
        bT.single_executioner.insert(entries_array);
        return bT;
    }

    public static String graphviz_display(BTree.Node current, MutableNode graphviz_current, List<MutableNode> nodes, String tree_name) throws IOException {
        if (nodes == null)
            nodes = new ArrayList<>();
        graphviz_current = mutNode(current.getEntriesString());
        nodes.add(graphviz_current);

        for (int i = 0; i < current.used_slots + 1; i++) {
            System.out.println(current.childrenNode[i].getEntriesString());
            MutableNode child = mutNode(current.childrenNode[i].getEntriesString());
            graphviz_current.addLink(child);
            if (!current.childrenNode[i].isLeaf) {
                graphviz_display(current.childrenNode[i], child, nodes, tree_name);
            }
            nodes.add(child);
        }

        Graph g = graph(tree_name).directed().with(nodes);
        String rel_path = "example/" + tree_name + ".png";
        Graphviz.fromGraph(g).scale(1.5).render(Format.PNG).toFile(new File(rel_path));
        return rel_path;
    }
}
