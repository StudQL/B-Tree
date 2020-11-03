 class SingleExecutioner extends AbstractExecutioner {

    SingleExecutioner(BTree bTree) {
        super(bTree);
    }

    @Override
    BTree.Entry[] get(int[] keys) {
        BTree.Entry output[] = new BTree.Entry[keys.length];

        for (int i = 0; i < keys.length; i++)
            output[i] = single_get(keys[i]);

        return output;
    }

    BTree.Entry single_get(int key) {

        BTree.Node root = bT.rootNode;
        while (!root.isLeaf) {
            if (key > root.entries[root.used_slots - 1].key) {
                root = root.childrenNode[root.used_slots];
                continue;
            } else {
                for (int i = 0; i < root.used_slots; i++) {
                    if (root.entries[i].key == key)
                        return root.entries[i];

                    else if (root.entries[i].key > key) {
                        root = root.childrenNode[i];
                        break;
                    }
                }
            }
        }

        if (root.getKeyIndex(key) != -1)
            return root.entries[root.getKeyIndex(key)];
        else
            return null;
    }

    @Override
    void insert(BTree.Entry[] entries) {
        for (BTree.Entry entry : entries) {
            try {
                single_insert(entry);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
     void single_insert(BTree.Entry entry) throws Exception {

        if (entry == null) throw new IllegalArgumentException("entry argument is null");
        //Root node without entry 
        if((bT.rootNode.isRoot)&&(bT.rootNode.used_slots==0)){
            bT.rootNode.entries[0] = entry;
            bT.rootNode.used_slots++;
            return;
        }
        BTree.Node temp = bT.rootNode;
        while(!temp.isLeaf)
        {                      
			if(entry.key > temp.entries[temp.used_slots-1].key){
                temp = temp.childrenNode[temp.used_slots];
				continue;
            }            
			for(int i=0; i<temp.used_slots; i++)
				if(entry.key < temp.entries[i].key){
                    temp = temp.childrenNode[i];
					break;
				}
        }

        if(temp.used_slots == bT.max_keys)
            temp = splitNode(temp, entry);
        temp.addEntry(entry);
        
}


    private BTree.Node splitRootNode(BTree.Node node, BTree.Entry entry){
        
        int mid = bT.max_keys/2;
        BTree.Node leftNode = bT.new Node(false, true);
        BTree.Node rightNode = bT.new Node(false, true);
        System.arraycopy(node.entries, 0, leftNode.entries, 0, mid);
        leftNode.used_slots = mid;
        System.arraycopy(node.entries, mid+1, rightNode.entries, 0, mid-1);
        rightNode.used_slots = mid-1;

        BTree.Entry tmp = node.entries[mid];
        node.entries = new BTree.Entry[bT.max_keys]; 
        node.entries[0] = tmp;            
        node.used_slots = 1;
        
        node.isLeaf = false;
        node.childrenNode = new BTree.Node[bT.M];
        node.childrenNode[0] = leftNode;
        node.childrenNode[1] = rightNode;
        leftNode.parentNode = node;
        rightNode.parentNode = node;
        leftNode.rightSibling = rightNode;
        rightNode.leftSibling = leftNode;

        if(entry.key>node.entries[0].key)
            return rightNode;
            
        return leftNode;    
}

    private BTree.Node splitParentNode(BTree.Node node, BTree.Entry entry){
        
        int mid = bT.max_keys/2;
        BTree.Node leftNode = bT.new Node(false, false);
        BTree.Node rightNode = bT.new Node(false, false);
        System.arraycopy(node.entries, 0, leftNode.entries, 0, mid);
        leftNode.used_slots = mid;
        System.arraycopy(node.entries, mid+1, rightNode.entries, 0, mid-1);
        rightNode.used_slots = mid-1;

        BTree.Entry tmp = node.entries[mid];
        node.entries = new BTree.Entry[bT.max_keys]; 
        node.entries[0] = tmp;            
        node.used_slots = 1;
                    
        leftNode.childrenNode = new BTree.Node[bT.M];
        rightNode.childrenNode = new BTree.Node[bT.M];

        for(int i=0; i<mid; i++){
            leftNode.childrenNode[i] = node.childrenNode[i];
            leftNode.childrenNode[i].parentNode = leftNode;
            rightNode.childrenNode[i] = node.childrenNode[i + mid + 1];
            rightNode.childrenNode[i].parentNode = rightNode;
        }
            leftNode.childrenNode[mid] = node.childrenNode[mid];
            leftNode.childrenNode[mid].parentNode = leftNode;
	    
	    leftNode.childrenNode[mid].rightSibling = null;
            rightNode.childrenNode[0].leftSibling = null;

            node.childrenNode = new BTree.Node[bT.M];
            node.childrenNode[0] = leftNode;
            node.childrenNode[1] = rightNode;
            
            leftNode.parentNode = node;
            rightNode.parentNode = node;
            leftNode.rightSibling = rightNode;
            rightNode.leftSibling = leftNode;

            if(entry.key>node.entries[0].key)
                return rightNode;
            
            return leftNode;
    }


     public BTree.Node splitNode(BTree.Node node, BTree.Entry entry){
        
        int mid = bT.max_keys/2;
        BTree.Node parent = node.parentNode;

        // root node
        if(node.isRoot) return splitRootNode(node, entry);
            
        // parent node full of entries so we have to split it before
        if(parent.used_slots == bT.max_keys)
            parent = splitParentNode(parent, node.entries[mid]);
    
        //split of a leaf node with parent not full
        BTree.Node rightNode = bT.new Node(false, true);
        System.arraycopy(node.entries, mid+1, rightNode.entries, 0, mid-1);
        rightNode.used_slots = mid-1;
        node.used_slots = mid;
        int index = parent.used_slots;

        BTree.Entry midEntry = node.entries[mid];

        for(int i=mid; i<2*mid; i++)
            node.entries[i] = null;

        for(int i=parent.used_slots-1;i>-1;i--)
        {
            if(midEntry.key<parent.entries[i].key){
                parent.entries[i+1] = parent.entries[i];
                parent.childrenNode[i+2] = parent.childrenNode[i+1];
                index = i;
            }
            else 
                break;
        }
        parent.entries[index] = midEntry;
	parent.childrenNode[index+1] = rightNode;
	parent.used_slots++;
	rightNode.parentNode = parent;
        rightNode.leftSibling = node;
	rightNode.rightSibling = node.rightSibling;
        node.rightSibling = rightNode;
	     
	if(parent.childrenNode[index+2] != null)
            parent.childrenNode[index+2].leftSibling = rightNode;
	     
        if(entry.key>midEntry.key){
			return rightNode;
		}
		return node;
    }
        
         

    @Override
     void delete(int[] keys) throws Exception {
        BTree.Entry[] entries = this.get(keys);
        for (BTree.Entry e : entries){
            single_delete(e);
        }
    }

    private void single_delete(BTree.Entry e) throws Exception {
        BTree.Node e_node = e.selfNode;
        if(e_node.isLeaf){
            e_node.removeEntry(e);
            if(e_node.used_slots < bT.min_keys){
                if(e_node.leftSibling.used_slots > bT.min_keys){

                }
                else if (e_node.rightSibling.used_slots > bT.min_keys){

                }
                else{

                }
            }
        }
    }
}
