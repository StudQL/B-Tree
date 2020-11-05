import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

public class BTree {

    final int M;
    final int min_keys;
    final int max_keys;
    final int min_children;

    enum executionType {single_thread, multi_thread, spark};
    Node rootNode;
    AbstractExecutioner single_executioner = new SingleExecutioner(this);
    AbstractExecutioner multi_executioner = new MultiExecutioner(this);
    AbstractExecutioner spark_executioner = new SparkExecutioner(this);

    public BTree(int M) {
        this.M = M;
        this.min_children = (int) Math.ceil(M / 2.0);
        this.max_keys = M - 1;
        this.min_keys = this.min_children - 1;
        this.rootNode = new Node(true, true);
    }

    class Node {
        Entry[] entries;
        Node leftSibling = null;
        Node rightSibling = null;
        Node parentNode = null;
        Node[] childrenNode = null;
        boolean isRoot;
        boolean isLeaf;
        int used_slots = 0; // UPDATE A CHAQUE INSERTION ET DELETION D'UN ELEMENT

        Node(boolean isRoot, boolean isLeaf) {
            this.entries = new Entry[max_keys];
            this.isRoot = isRoot;
            this.isLeaf = isLeaf;
        }

        String getEntriesString(){
            String[] s_array = new String[used_slots];
            for (int i = 0; i< used_slots; i++){
                s_array[i] = Integer.toString(entries[i].key);
            }
            return String.join(" | ", s_array);
        }

        int getKeyIndex(int key) {
            if(this.used_slots >= min_keys){
                int beg = 0, end = max_keys - 1;
                while (beg <= end) {
                    int e = beg + (end - beg) / 2;
                    if (this.entries[e].key == key)
                        return e;
                    if (this.entries[e].key < key)
                        beg = e + 1;
                    else
                        end = e - 1;
                }
            }else{
                for (int i = 0; i < max_keys; i++){
                    if(this.entries[i].key == key){
                        return i;
                    }
                }
            }
            return -1;
        }

        void addEntryAtIndex(Entry entry, int index) {
            if(this.entries[index] != null && this.entries[index].key == entry.key){
                this.entries[index].value = entry.value;
                return;
            }

            for (int j = this.used_slots - 1; index <= j; j--) {
                this.entries[j + 1] = this.entries[j];
            }
            this.entries[index] = entry;
            this.used_slots++;
        }

        void addEntry(Entry entry) throws Exception {
            if (this.used_slots == max_keys) {
                throw new Exception("Array already full");
            }
            int i = 0;
            for (; i < this.used_slots; i++) {
                if (this.entries[i].key >= entry.key) {
                    break;
                }
            }
            addEntryAtIndex(entry, i);
        }

        void addEntry(int key, Object value) throws Exception {
            Entry entry = new Entry(key, value);
            addEntry(entry);
        }

        int getChildNodePosition(Node n){
            for(int i = 0; i < M; i++){
                if (childrenNode[i].equals(n)){
                    return i;
                }
            }
            return -1;
        }

        @Override
        public String toString() {
            return Arrays.toString(entries);
        }


    }

    class Entry {
        int key;
        Object value;

        Entry(int key, Object value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return "Entry{" +
                    "key=" + key +
                    ", value=" + value +
                    '}';
        }

//        Node getLeftChild(){
//            if (selfNode.isLeaf){
//                return null;
//            }
//            int index_in_node = selfNode.getKeyIndex(key);
//            return selfNode.childrenNode[index_in_node];
//        }
//
//        Node getRightChild(){
//            if (selfNode.isLeaf){
//                return null;
//            }
//            int index_in_node = selfNode.getKeyIndex(key);
//            return selfNode.childrenNode[index_in_node+1];
//        }
//

    }


    public void insert(Entry[] entries, executionType execution) {
        this.get_executioner(execution).insert(entries);
    }

    public Entry[] get(int[] keys, executionType execution) {
        return this.get_executioner(execution).get(keys);
    }

    public void delete(int[] keys, executionType execution) throws Exception {
        this.get_executioner(execution).delete(keys);
    }

    private AbstractExecutioner get_executioner(executionType execution) {
        switch (execution) {
            case single_thread:
                return single_executioner;
            case multi_thread:
                return multi_executioner;
            case spark:
                return spark_executioner;
            default:
                return null;
        }
    }

}
