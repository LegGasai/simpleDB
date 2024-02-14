package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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

    private final LockManager lockManager;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.lruCache = new LRUCache<>(this.numPages);
        this.lockManager = new LockManager();
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
            try {
                if (!lockManager.grantLock(tid,pid,perm,0)){
                    throw new TransactionAbortedException();
                }
            }catch (InterruptedException e){
                System.out.println("getPage()获取锁异常:"+e);
            }
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
        lockManager.releaseLock(pid,tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid,p);
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
        if (commit){
            try {
                flushPagesAndSetImg(tid);
            }catch (IOException e){
                e.printStackTrace();
            }

        }else{
            try {
                rollback(tid);
            }catch (DbException e){
                e.printStackTrace();
            }
        }
        // release all lock hold by tid
        lockManager.releaseByTid(tid);
    }



    public synchronized void rollback(TransactionId tid) throws DbException{
        List<Page> pages = getPagesByTid(tid);
        for (Page page : pages) {
            PageId pageId = page.getId();
            Page originPage = Database.getCatalog().getDatabaseFile(pageId.getTableId()).readPage(pageId);
            // recovery page
            lruCache.put(pageId,originPage);
        }
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
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
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
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        updateBufferPool(file.deleteTuple(tid,t),tid);
    }


    private void updateBufferPool(List<Page> pages,TransactionId tid) throws DbException{
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
            Database.getLogFile().logWrite(tid,page.getBeforeImage(),page);
            Database.getLogFile().force();
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        List<Page> pages = getPagesByTid(tid);
        for (Page page : pages) {
            Database.getLogFile().logWrite(tid,page.getBeforeImage(),page);
            Database.getLogFile().force();
            page.markDirty(false,null);
            Database.getCatalog().getDatabaseFile(page.getId().getTableId()).writePage(page);
        }
    }

    private synchronized void flushPagesAndSetImg(TransactionId tid) throws IOException{
        List<Page> pages = getPagesByTid(tid);
        for (Page page : pages) {
            Database.getLogFile().logWrite(tid,page.getBeforeImage(),page);
            Database.getLogFile().force();
            page.markDirty(false,null);
            Database.getCatalog().getDatabaseFile(page.getId().getTableId()).writePage(page);
            page.setBeforeImage();
        }
    }

    private synchronized List<Page> getPagesByTid(TransactionId tid){
        List<Page> pages = new ArrayList<>();
        for (Map.Entry<PageId, LRUNode<PageId, Page>> entry : this.lruCache.data.entrySet()) {
            Page page = entry.getValue().value;
            if (page.isDirty()!=null && page.isDirty().equals(tid)){
                pages.add(page);
            }
        }
        return pages;
    }


    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    }

    // PageId -> Page
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

        public synchronized void put(K key, V value) throws DbException{
            boolean ok = data.containsKey(key);
            if (ok){
                LRUNode node = data.get(key);
                node.value = value;
                this.moveToHead(node);
            }else{
                int size = data.size();
                if (size>=capacity){
                    // evict the last recently used page
                    LRUNode removeNode = deleteLastNode();
                    if (removeNode == null){
                        throw new DbException("缓冲区全为脏页，没有剩余空间！");
                    }
                    data.remove(removeNode.key);
                }
                LRUNode newNode = new LRUNode(key, value);
                data.put(key,newNode);
                this.addToHead(newNode);
            }
        }

        public synchronized void remove(K key){
            LRUNode<K, V> node = data.get(key);
            if (node == null){
                return;
            }
            data.remove(key);
            deleteNode(node);
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
            LRUNode lastNode = null;
            LRUNode curNode = this.tail.pre;
            while (curNode!=null&&curNode!=this.head){
                if (((Page)curNode.value).isDirty() != null){
                    curNode = curNode.pre;
                }else{
                    lastNode = curNode;
                    break;
                }
            }
            if (lastNode == null) {
                return lastNode;
            }
            deleteNode(lastNode);
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



//package simpledb.storage;
//
//import simpledb.common.Database;
//import simpledb.common.Permissions;
//import simpledb.common.DbException;
//import simpledb.common.DeadlockException;
//
//import simpledb.transaction.LockManager;
//import simpledb.transaction.TransactionAbortedException;
//import simpledb.transaction.TransactionId;
//
//import java.io.*;
//
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.locks.ReentrantLock;
//import java.util.concurrent.locks.ReentrantReadWriteLock;
//
///**
// * BufferPool manages the reading and writing of pages into memory from
// * disk. Access methods call into it to retrieve pages, and it fetches
// * pages from the appropriate location.
// * <p>
// * The BufferPool is also responsible for locking;  when a transaction fetches
// * a page, BufferPool checks that the transaction has the appropriate
// * locks to read/write the page.
// *
// * @Threadsafe, all fields are final
// */
//public class BufferPool {
//    /**
//     * Bytes per page, including header.
//     */
//    int i = 0;
//    private static final int DEFAULT_PAGE_SIZE = 4096;
//
//    private static int pageSize = DEFAULT_PAGE_SIZE;
//
//    /**
//     * Default number of pages passed to the constructor. This is used by
//     * other classes. BufferPool should use the numPages argument to the
//     * constructor instead.
//     */
//    public static final int DEFAULT_PAGES = 50;
//    private int numPages;
//    private LRUCache<PageId, Page> lruCache;
//    private LockManager lockManager;
//
//    /**
//     * Creates a BufferPool that caches up to numPages pages.
//     *
//     * @param numPages maximum number of pages in this buffer pool.
//     */
//    public BufferPool(int numPages) {
//        // some code goes here
//        this.numPages = numPages;
//        this.lruCache = new LRUCache<>(numPages);
//        this.lockManager = new LockManager();
//
//
//    }
//
//    public static int getPageSize() {
//        return pageSize;
//    }
//
//    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
//    public static void setPageSize(int pageSize) {
//        BufferPool.pageSize = pageSize;
//    }
//
//    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
//    public static void resetPageSize() {
//        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
//    }
//
//
//    /**
//     * Retrieve the specified page with the associated permissions.
//     * Will acquire a lock and may block if that lock is held by another
//     * transaction.
//     * <p>
//     * The retrieved page should be looked up in the buffer pool.  If it
//     * is present, it should be returned.  If it is not present, it should
//     * be added to the buffer pool and returned.  If there is insufficient
//     * space in the buffer pool, a page should be evicted and the new page
//     * should be added in its place.
//     *
//     * @param tid  the ID of the transaction requesting the page
//     * @param pid  the ID of the requested page
//     * @param perm the requested permissions on the page
//     */
//    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
//            throws TransactionAbortedException, DbException {
//        // some code goes here
//        boolean lockAcquired = false;
//        long start = System.currentTimeMillis();
//        long timeout = new Random().nextInt(2000);
//        while (!lockAcquired) {
//            long now = System.currentTimeMillis();
//            if (now - start > timeout) {
//                throw new TransactionAbortedException();
//            }
//            try {
//                lockAcquired = lockManager.grantLock(tid, pid, perm,0);
//            }catch (Exception e){}
//
//        }
//
//        if (lruCache.get(pid) == null) {
//
//            DbFile databaseFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
//            Page page = databaseFile.readPage(pid);
//            if (lruCache.getSize() >= numPages) {
//                evictPage();
//            }
//            lruCache.put(pid, page);
//            return page;
//
//        } else {
//            return lruCache.get(pid);
//        }
//
//    }
//
//    /**
//     * Releases the lock on a page.
//     * Calling this is very risky, and may result in wrong behavior. Think hard
//     * about who needs to call this and why, and why they can run the risk of
//     * calling it.
//     *
//     * @param tid the ID of the transaction requesting the unlock
//     * @param pid the ID of the page to unlock
//     */
//    public void unsafeReleasePage(TransactionId tid, PageId pid) {
//        // some code goes here
//        // not necessary for lab1|lab2
//        lockManager.releaseLock(pid, tid);
//    }
//
//    /**
//     * Release all locks associated with a given transaction.
//     *
//     * @param tid the ID of the transaction requesting the unlock
//     */
//    public void transactionComplete(TransactionId tid) {
//        // some code goes here
//        // not necessary for lab1|lab2
//        transactionComplete(tid, true);
//    }
//
//    /**
//     * Return true if the specified transaction has a lock on the specified page
//     */
//    public boolean holdsLock(TransactionId tid, PageId p) {
//        // some code goes here
//        // not necessary for lab1|lab2
//        return lockManager.holdsLock(tid, p);
//    }
//
//    /**
//     * Commit or abort a given transaction; release all locks associated to
//     * the transaction.
//     *
//     * @param tid    the ID of the transaction requesting the unlock
//     * @param commit a flag indicating whether we should commit or abort
//     */
//    public void transactionComplete(TransactionId tid, boolean commit) {
//        // some code goes here
//        // not necessary for lab1|lab2
//        if (commit) {
//            try {
//                flushPages2(tid);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        } else {
//            rollback(tid);
//        }
//        lockManager.releaseByTid(tid);
//
//    }
//
//    /**
//     * Add a tuple to the specified table on behalf of transaction tid.  Will
//     * acquire a write lock on the page the tuple is added to and any other
//     * pages that are updated (Lock acquisition is not needed for lab2).
//     * May block if the lock(s) cannot be acquired.
//     * <p>
//     * Marks any pages that were dirtied by the operation as dirty by calling
//     * their markDirty bit, and adds versions of any pages that have
//     * been dirtied to the cache (replacing any existing versions of those pages) so
//     * that future requests see up-to-date pages.
//     *
//     * @param tid     the transaction adding the tuple
//     * @param tableId the table to add the tuple to
//     * @param t       the tuple to add
//     */
//
//    public void insertTuple(TransactionId tid, int tableId, Tuple t)
//            throws DbException, IOException, TransactionAbortedException {
//        // some code goes here
//        // not necessary for lab1
//        // System.out.println(tid.getId());
//        DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
//        //System.out.println(tid.getId());
//        List<Page> pages = databaseFile.insertTuple(tid, t);
//        //System.out.println(tid.getId());
//        for (Page page : pages) {    //用脏页替换buffer中现有的页
//            page.markDirty(true, tid);
//            lruCache.put(page.getId(), page);
//        }
//
//    }
//
//    /**
//     * Remove the specified tuple from the buffer pool.
//     * Will acquire a write lock on the page the tuple is removed from and any
//     * other pages that are updated. May block if the lock(s) cannot be acquired.
//     * <p>
//     * Marks any pages that were dirtied by the operation as dirty by calling
//     * their markDirty bit, and adds versions of any pages that have
//     * been dirtied to the cache (replacing any existing versions of those pages) so
//     * that future requests see up-to-date pages.
//     *
//     * @param tid the transaction deleting the tuple.
//     * @param t   the tuple to delete
//     */
//    public void deleteTuple(TransactionId tid, Tuple t)
//            throws DbException, IOException, TransactionAbortedException {
//        // some code goes here
//        // not necessary for lab1
//        PageId pageId = t.getRecordId().getPageId();
//        int tableId = pageId.getTableId();
//        DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
//        List<Page> pages = databaseFile.deleteTuple(tid, t);
//        for (int i = 0; i < pages.size(); i++) {
//            pages.get(i).markDirty(true, tid);
//        }
//    }
//
//    /**
//     * Flush all dirty pages to disk.
//     * NB: Be careful using this routine -- it writes dirty data to disk so will
//     * break simpledb if running in NO STEAL mode.
//     */
//    public synchronized void flushAllPages() throws IOException {
//        // some code goes here
//        // not necessary for lab1
//        LRUCache<PageId, Page>.DLinkedNode head = lruCache.getHead();
//        LRUCache<PageId, Page>.DLinkedNode tail = lruCache.getTail();
//        while (head != tail) {
//            Page value = head.value;
//            if (value != null && value.isDirty() != null) {
//
//                DbFile databaseFile = Database.getCatalog().getDatabaseFile(value.getId().getTableId());
//                try {
//                    System.out.printf("flushpage:[%d]:[%d]\n",value.getId().getTableId(),value.getId().getPageNumber());
//                    Database.getLogFile().logWrite(value.isDirty(), value.getBeforeImage(), value);
//                    Database.getLogFile().force();
//                    //这里不能将脏页标记为不脏，如果这样做则当事务提交的时候，flushpage2函数找不到脏页，无法将更新写入磁盘
//                    //也无法setbeforeimage 详情见LogTest的78行
//                    // value.markDirty(false, null);
//                    databaseFile.writePage(value);
//
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//
//            }
//            head = head.next;
//        }
//
//    }
//
//    /**
//     * Remove the specific page id from the buffer pool.
//     * Needed by the recovery manager to ensure that the
//     * buffer pool doesn't keep a rolled back page in its
//     * cache.
//     * <p>
//     * Also used by B+ tree files to ensure that deleted pages
//     * are removed from the cache so they can be reused safely
//     */
//    public synchronized void discardPage(PageId pid) {
//        // some code goes here
//        // not necessary for lab1
//
//        LRUCache<PageId, Page>.DLinkedNode head = lruCache.getHead();
//        LRUCache<PageId, Page>.DLinkedNode tail = lruCache.getTail();
//        while (head != tail) {
//            PageId key = head.key;
//            if (key != null && key.equals(pid)) {
//                lruCache.remove(head);
//                return;
//            }
//            head = head.next;
//        }
//    }
//
//    /**
//     * Flushes a certain page to disk
//     *
//     * @param pid an ID indicating the page to flush
//     */
//
//    private synchronized void flushPage(PageId pid) throws IOException {
//        // some code goes here
//        // not necessary for lab1
//        Page discard = lruCache.get(pid);
//        DbFile databaseFile = Database.getCatalog().getDatabaseFile(discard.getId().getTableId());
//        try {
//            TransactionId dirtier = discard.isDirty();
//            if (dirtier != null) {
//                Database.getLogFile().logWrite(dirtier, discard.getBeforeImage(), discard);
//                Database.getLogFile().force();
//                discard.markDirty(false, null);
//                databaseFile.writePage(discard);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    /**
//     * Write all pages of the specified transaction to disk.
//     */
//    public synchronized void flushPages(TransactionId tid) throws IOException {
//        // some code goes here
//        // not necessary for lab1|lab2
//        LRUCache<PageId, Page>.DLinkedNode head = lruCache.getHead();
//        LRUCache<PageId, Page>.DLinkedNode tail = lruCache.getTail();
//        while (head != tail) {
//            Page value = head.value;
//            if (value != null && value.isDirty() != null && value.isDirty().equals(tid)) {
//                DbFile databaseFile = Database.getCatalog().getDatabaseFile(value.getId().getTableId());
//                try {
//                    Database.getLogFile().logWrite(value.isDirty(), value.getBeforeImage(), value);
//                    Database.getLogFile().force();
//                    value.markDirty(false, null);
//                    databaseFile.writePage(value);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//            head = head.next;
//        }
//    }
//
//
//    public synchronized void flushPages2(TransactionId tid) throws IOException {
//        // some code goes here
//        // not necessary for lab1|lab2
//        // System.out.println(tid.getId());
//        LRUCache<PageId, Page>.DLinkedNode head = lruCache.getHead();
//        LRUCache<PageId, Page>.DLinkedNode tail = lruCache.getTail();
//        while (head != tail) {
//            Page value = head.value;
//            if (value != null && value.isDirty() != null && value.isDirty().equals(tid)) {
//                DbFile databaseFile = Database.getCatalog().getDatabaseFile(value.getId().getTableId());
//                try {
//                    Database.getLogFile().logWrite(value.isDirty(), value.getBeforeImage(), value);
//                    Database.getLogFile().force();
//                    value.markDirty(false, null);
//                    databaseFile.writePage(value);
//                    value.setBeforeImage();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//            head = head.next;
//        }
//    }
//
//    private void findNotDirty() throws DbException {
//        LRUCache<PageId, Page>.DLinkedNode head = lruCache.getHead();
//        LRUCache<PageId, Page>.DLinkedNode tail = lruCache.getTail();
//        tail = tail.prev;
//        while (head != tail) {
//            Page value = tail.value;
//            if (value != null && value.isDirty() == null) {
//                lruCache.remove(tail);
//                return;
//            }
//            tail = tail.prev;
//        }
//        throw new DbException("no dirty page");
//    }
//
//    private synchronized void rollback(TransactionId transactionId) {
//        LRUCache<PageId, Page>.DLinkedNode head = lruCache.getHead();
//        LRUCache<PageId, Page>.DLinkedNode tail = lruCache.getTail();
//        while (head != tail) {
//            Page value = head.value;
//            LRUCache<PageId, Page>.DLinkedNode tmp = head.next;
//            if (value != null && value.isDirty() != null && value.isDirty().equals(transactionId)) {
//                //删掉脏页
//                lruCache.remove(head);
//                try {
//                    //重新读原来的页
//                    Page page = Database.getBufferPool().getPage(transactionId, value.getId(), Permissions.READ_ONLY);
//                    page.markDirty(false,null);
//                } catch (TransactionAbortedException e) {
//                    e.printStackTrace();
//                } catch (DbException e) {
//                    e.printStackTrace();
//                }
//            }
//            head = tmp;
//        }
//    }
//
//    /**
//     * Discards a page from the buffer pool.
//     * Flushes the page to disk to ensure dirty pages are updated on disk.
//     */
//    private synchronized void evictPage() throws DbException {
//        // some code goes here
//        // not necessary for lab1
//        //如果为脏页则不能替换
//        Page value = lruCache.getTail().prev.value;
//        //  如果是脏页
//        if (value != null && value.isDirty() != null) {
//            findNotDirty();
//        } else {
//            //不是脏页没改过，不需要写磁盘
//            lruCache.discard();
//        }
//
//    }
//
//    public LockManager getLockManager() {
//        return lockManager;
//    }
//
//    public LRUCache<PageId, Page> getLruCache() {
//        return lruCache;
//    }
//}
//
//
//class LRUCache<K,V> {
//    class DLinkedNode {
//        K key;
//        V value;
//        DLinkedNode prev;
//        DLinkedNode next;
//        public DLinkedNode() {}
//        public DLinkedNode(K _key, V _value) {key = _key; value = _value;}
//
//    }
//
//    private Map<K, DLinkedNode> cache = new ConcurrentHashMap<K, DLinkedNode>();
//    private int size;
//    private int capacity;
//    private DLinkedNode head, tail;
//
//    public LRUCache(int capacity) {
//        this.size = 0;
//        this.capacity = capacity;
//        // 使用伪头部和伪尾部节点
//        head = new DLinkedNode();
//        tail = new DLinkedNode();
//        head.next = tail;
//        head.prev = tail;
//        tail.prev = head;
//        tail.next = head;
//    }
//
//
//    public int getSize() {
//        return size;
//    }
//
//    public DLinkedNode getHead() {
//        return head;
//    }
//
//    public DLinkedNode getTail() {
//        return tail;
//    }
//
//    public Map<K, DLinkedNode> getCache() {
//        return cache;
//    }
//    //必须要加锁，不然多线程链表指针会成环无法结束循环。在这卡一天
//    public synchronized V get(K key) {
//        DLinkedNode node = cache.get(key);
//        if (node == null) {
//            return null;
//        }
//        // 如果 key 存在，先通过哈希表定位，再移到头部
//        moveToHead(node);
//        return node.value;
//    }
//    public synchronized void remove(DLinkedNode node){
//        node.prev.next = node.next;
//        node.next.prev = node.prev;
//        cache.remove(node.key);
//        size--;
//    }
//
//    public synchronized void discard(){
//        // 如果超出容量，删除双向链表的尾部节点
//        DLinkedNode tail = removeTail();
//        // 删除哈希表中对应的项
//        cache.remove(tail.key);
//        size--;
//
//    }
//    public synchronized void put(K key, V value) {
//        DLinkedNode node = cache.get(key);
//        if (node == null) {
//            // 如果 key 不存在，创建一个新的节点
//            DLinkedNode newNode = new DLinkedNode(key, value);
//            // 添加进哈希表
//            cache.put(key, newNode);
//            // 添加至双向链表的头部
//            addToHead(newNode);
//            ++size;
//        }
//        else {
//            // 如果 key 存在，先通过哈希表定位，再修改 value，并移到头部
//            node.value = value;
//            moveToHead(node);
//        }
//    }
//
//    private void addToHead(DLinkedNode node) {
//        node.prev = head;
//        node.next = head.next;
//        head.next.prev = node;
//        head.next = node;
//    }
//
//    private void removeNode(DLinkedNode node) {
//        node.prev.next = node.next;
//        node.next.prev = node.prev;
//    }
//
//
//    private void moveToHead(DLinkedNode node) {
//        removeNode(node);
//        addToHead(node);
//    }
//
//    private DLinkedNode removeTail() {
//        DLinkedNode res = tail.prev;
//        removeNode(res);
//        return res;
//    }
//
//}