package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;

    private final LRUCache<PageId,Page> lruCache;


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.lruCache = new LRUCache<>(this.numPages);
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        if (pid!=null){
            if (this.lruCache.get(pid) != null){
                return this.lruCache.get(pid);
            }else{
                // need to be add to buffer pool
                DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
                Page page = file.readPage(pid);
                lruCache.put(pid,page);
                return page;
            }
        }
        // todo
        return null;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        updateBufferPool(file.insertTuple(tid,t),tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        updateBufferPool(file.deleteTuple(tid,t),tid);
    }

    private void updateBufferPool(List<Page> pages,TransactionId tid){
        for (Page page : pages) {
            page.markDirty(true,tid);
            lruCache.put(page.getId(),page);
        }

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (LRUNode<PageId, Page> value : this.lruCache.data.values()) {
            flushPage(value.key);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        this.lruCache.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = lruCache.get(pid);
        if (page == null) { return; }
        TransactionId tid = page.isDirty();
        // dirty
        if (tid != null){
            Page beforeImage = page.getBeforeImage();
            Database.getLogFile().logWrite(tid,beforeImage,page);
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
        }

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    }


    private static class LRUCache<K,V> {

        Map<K,LRUNode<K,V>> data;
        Integer capacity;
        LRUNode head;
        LRUNode tail;

        public LRUCache(int capacity) {
            this.data = new ConcurrentHashMap<>();
            this.capacity = capacity;
            this.head = new LRUNode();
            this.tail = new LRUNode();
            this.head.next=tail;
            this.tail.pre = head;
        }

        public synchronized int size(){
            return this.data.size();
        }


        public synchronized V get(K key) {
            boolean ok = data.containsKey(key);
            if(ok){
                // move to head;
                LRUNode<K,V> node = data.get(key);
                this.moveToHead(node);
                return node.value;
            }else{
                return null;
            }
        }

        public synchronized void put(K key, V value) {
            boolean ok = data.containsKey(key);
            if (ok){
                LRUNode node = data.get(key);
                node.value = value;
                this.moveToHead(node);
            }else{
                int size = data.size();
                if (size>=capacity){
                    LRUNode removeNode = deleteLastNode();
                    data.remove(removeNode.key);
                }
                LRUNode newNode = new LRUNode(key, value);
                data.put(key,newNode);
                this.addToHead(newNode);
            }
        }

        public synchronized void remove(K key){
            V v = get(key);
            LRUNode node = (LRUNode) v;
            Page page = (Page) node.value;
            try {
                Database.getBufferPool().flushPage(page.getId());
            }catch (IOException e){
                e.printStackTrace();
            }
            deleteNode((LRUNode)v);
        }
        public synchronized void deleteNode(LRUNode node){
            node.pre.next = node.next;
            node.next.pre = node.pre;
        }
        public synchronized void moveToHead(LRUNode node){
            deleteNode(node);
            addToHead(node);
        }
        public synchronized void addToHead(LRUNode node){
            node.pre=head;
            node.next=head.next;
            head.next.pre=node;
            head.next=node;
        }
        public synchronized LRUNode deleteLastNode(){
            LRUNode lastNode = this.tail.pre;
            lastNode.pre.next = this.tail;
            this.tail.pre = lastNode.pre;
            return lastNode;
        }
    }
    private static class LRUNode<K,V>{
        K key;
        V value;
        LRUNode pre;
        LRUNode next;
        public LRUNode() {}
        public LRUNode(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }
}




