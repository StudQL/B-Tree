import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BTree implements Serializable {

    final int M;
    final int min_keys;
    final int max_keys;
    final int min_children;
    Node rootNode;

    ;
    AbstractExecutioner single_executioner = new SingleExecutioner(this);
    AbstractExecutioner multi_executioner = new MultiExecutioner(this);

    public BTree(int M) {
        this.M = M;
        this.min_children = (int) Math.ceil(M / 2.0);
        this.max_keys = M - 1;
        this.min_keys = this.min_children - 1;
        this.rootNode = new Node(true, true);
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
            default:
                return null;
        }
    }

    @Override
    public String toString() {
        return "BTree{" +
                "rootNode=" + rootNode +
                '}';
    }

    enum executionType {single_thread, multi_thread, spark}

    class Node implements Serializable {
        Entry[] entries;
        Node leftSibling = null;
        Node rightSibling = null;
        Node parentNode = null;
        Node[] childrenNode = null;
        boolean isRoot;
        boolean isLeaf;
        int used_slots = 0; // UPDATE A CHAQUE INSERTION ET DELETION D'UN ELEMENT
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        Node(boolean isRoot, boolean isLeaf) {
            this.entries = new Entry[max_keys];
            this.isRoot = isRoot;
            this.isLeaf = isLeaf;
        }

        String getEntriesString() {
            String[] s_array = new String[used_slots];
            for (int i = 0; i < used_slots; i++) {
                s_array[i] = Integer.toString(entries[i].key);
            }
            return String.join(" | ", s_array);
        }

        int getKeyIndex(int key) {
            if (this.used_slots >= min_keys) {
                int low = 0;
                int high = max_keys - 1;
                while (low <= high) {
                    int mid = low + ((high - low) / 2);
                    if (this.entries[mid] == null ||this.entries[mid].key > key) {
                        high = mid - 1;
                    } else if (this.entries[mid].key < key)
                        low = mid + 1;
                    else
                        return mid;
                }
            } else {
                for (int i = 0; i < min_keys; i++) {
                    if (this.entries[i] != null && this.entries[i].key == key) {
                        return i;
                    }
                }
            }
            return -1;
        }

        void addEntryAtIndex(Entry entry, int index) {
            if (this.entries[index] != null && this.entries[index].key == entry.key) {
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

        int getChildNodePosition(Node n) {
            for (int i = 0; i < M; i++) {
                if (childrenNode[i].equals(n)) {
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

    class Entry<T>implements Serializable {
        int key;
        T value;

        Entry(int key, T value) {
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

    }
}
