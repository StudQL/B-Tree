 abstract class AbstractExecutioner{

    protected BTree bT;

     AbstractExecutioner(BTree bTree) {
        this.bT = bTree;
    }

     abstract BTree.Entry[] get(int[] keys);

     abstract void insert(BTree.Entry[] entries);

     abstract void delete(int[] keys) throws Exception;

}
