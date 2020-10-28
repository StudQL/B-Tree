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
        this.min_children = (int) Math.ceil(M / 2);
        this.max_keys = M - 1;
        this.min_keys = this.min_children - 1;
        this.rootNode = new Node(true, true);
    }

    class Node {
        Entry[] entries;
        Node leftSibling = null;
        Node rightSibling = null;
        Node parentNode = null;
        Node[] childrenNode = new Node[M];
        boolean isRoot;
        boolean isLeaf;
        int used_slots = 0; // UPDATE A CHAQUE INSERTION ET DELETION D'UN ELEMENT

        Node(boolean isRoot, boolean isLeaf) {
            this.entries = new Entry[max_keys];
            this.isRoot = isRoot;
            this.isLeaf = isLeaf;
        }

        int getKeyIndex(int key) {
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
            return -1;
        }

        int getKeyIndex(Entry entry) {
            return getKeyIndex(entry.key);
        }

        void addEntryAtIndex(Entry entry, int index) {
            for (int j = this.used_slots - 1; index <= j; j--) {
                this.entries[j + 1] = this.entries[j];
            }
            this.entries[index] = entry;
            entry.selfNode = this;
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

        void removeEntryAtIndex(int index){
            this.entries[index] = null;
            this.used_slots--;
            for (int j = index + 1; j <= this.used_slots; j++) {
                this.entries[j - 1] = this.entries[j];
                if (j == this.used_slots) {
                    this.entries[j] = null;
                }
            }
        }

        void removeEntry(int key) throws Exception {
            int i = getKeyIndex(key);
            if (i == -1) {
                throw new Exception("Key not found");
            }
            removeEntryAtIndex(i);
        }

        void removeEntry(Entry entry) throws Exception {
            removeEntry(entry.key);
        }

        Entry pollLastEntry() throws Exception {
            for (int i = max_keys - 1; i >= min_keys; i--) {
                if (this.entries[i] != null) {
                    Entry e = this.entries[i];
                    this.entries[i] = null;
                    return e;
                }
            }
            return null;
        }

        Entry pollFirstEntry() throws Exception {
            Entry e = this.entries[0];
            removeEntry(e);
            return e;
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
        Node selfNode = null;

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

        Node getLeftChild(){
            if (selfNode.isLeaf){
                return null;
            }
            int index_in_node = selfNode.getKeyIndex(key);
            return selfNode.childrenNode[index_in_node];
        }

        Node getRightChild(){
            if (selfNode.isLeaf){
                return null;
            }
            int index_in_node = selfNode.getKeyIndex(key);
            return selfNode.childrenNode[index_in_node+1];
        }

        Entry getInorderPredecessor(){
            Node current = getLeftChild();
            while (!current.isLeaf){
                current = current.entries[current.used_slots-1].getRightChild();
            }
            return current.entries[current.used_slots-1];
        }

        Entry getInorderSuccessor(){
            Node current = getRightChild();
            while (!current.isLeaf){
                current = current.entries[0].getLeftChild();
            }
            return current.entries[0];
        }
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
