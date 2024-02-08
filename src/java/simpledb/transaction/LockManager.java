package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.execution.Predicate;
import simpledb.storage.HeapPageId;
import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: Jiang Yichen
 * @Date: 2024-02-08-10:34
 * @Description:
 */
public class LockManager {
    /**
     * PageId -> set of pageLocks
     */
    private final ConcurrentHashMap<PageId, HashSet<PageLock>> pid2Locks;
    /**
     * TransactionId -> set of pageLocks,i.e All locks hold by one transaction
     */
    private final ConcurrentHashMap<TransactionId,HashSet<PageLock>> tid2Locks;

    public LockManager() {
        pid2Locks = new ConcurrentHashMap<>();
        tid2Locks = new ConcurrentHashMap<>();
    }

    /**
     * Release lock on the specific page
     * @param pageId pid of page
     */
    public synchronized void releaseLock(PageId pageId,TransactionId tid){
        PageLock lock = getLockByPidAndTid(tid, pageId);
        if (lock!=null){
            HashSet<PageLock> pageLocks = pid2Locks.get(pageId);
            HashSet<PageLock> tidLocks = tid2Locks.get(tid);
            pageLocks.remove(lock);
            tidLocks.remove(lock);
            if (pageLocks.isEmpty()){
                pid2Locks.remove(pageId);
            }
            if (tidLocks.isEmpty()){
                tid2Locks.remove(tid);
            }
            this.notifyAll();
        }
    }

    /**
     * Release all lock hold by the transaction
     * @param tid tid of transaction
     */
    public synchronized void releaseByTid(TransactionId tid){
        if (tid2Locks.containsKey(tid)){
            HashSet<PageLock> tidLocks = tid2Locks.get(tid);
            for (PageLock lock : tidLocks) {
                PageId pageId = lock.getPageId();
                if (pid2Locks.containsKey(pageId)){
                    HashSet<PageLock> pageLocks = pid2Locks.get(pageId);
                    pageLocks.remove(lock);
                    if (pageLocks.isEmpty()){
                        pid2Locks.remove(pageId);
                    }
                }
            }
            this.notifyAll();
            tid2Locks.remove(tid);
        }
    }

    public synchronized boolean grantLock(TransactionId tid, PageId pageId, Permissions perm, int retry) throws InterruptedException{
        if (retry == 3){
            return false;
        }
        //holds the lock originally
        PageLock lock = getLockByPidAndTid(tid, pageId);
        if (lock!=null){
            if (perm == Permissions.READ_ONLY){
                return true;
            }else if (perm == Permissions.READ_WRITE){
                if (lock.getType().equals(LockType.XLOCK)){
                    return true;
                }else if (lock.getType().equals(LockType.SLOCK)){
                    // try to update lock
                    HashSet<PageLock> locksByPid = pid2Locks.get(pageId);
                    if (locksByPid!=null&&locksByPid.size()==1){
                        // update to XLock
                        lock.updateToXLock();
                        return true;
                    }
                    wait(50);
                    return grantLock(tid,pageId,perm,retry+1);
                }
            }
        }
        //try to acquire a lock
        else{
            if (canLockPage(pageId,perm)){
                PageLock pageLock = new PageLock(perm == Permissions.READ_ONLY ? LockType.SLOCK : LockType.XLOCK, pageId, tid);
                HashSet<PageLock> pageLockSet = pid2Locks.getOrDefault(pageId, new HashSet<>());
                HashSet<PageLock> tidLockSet = tid2Locks.getOrDefault(tid, new HashSet<>());
                pageLockSet.add(pageLock);
                tidLockSet.add(pageLock);
                pid2Locks.put(pageId,pageLockSet);
                tid2Locks.put(tid,tidLockSet);
                return true;
            }else{
                wait(50);
                return grantLock(tid,pageId,perm,retry+1);
            }
        }
        return false;
    }

    private synchronized PageLock getLockByPidAndTid(TransactionId tid,PageId pageId){
        if (this.tid2Locks.containsKey(tid)){
            HashSet<PageLock> locks = this.tid2Locks.get(tid);
            for (PageLock pageLock : locks) {
                if (pageLock.getPageId().equals(pageId)){
                    return pageLock;
                }
            }
        }
        return null;
    }

    private synchronized boolean canLockPage(PageId pageId,Permissions perm){
        if (this.pid2Locks.containsKey(pageId)){
            HashSet<PageLock> pageLocks = this.pid2Locks.get(pageId);
            if (!pageLocks.isEmpty() && perm == Permissions.READ_WRITE){
                return false;
            }
            for (PageLock pageLock : pageLocks) {
                if (pageLock.type.equals(LockType.XLOCK)){
                    return false;
                }
            }
            if (perm == Permissions.READ_ONLY){
                return true;
            }
        }
        return true;
    }
    public synchronized boolean holdsLock(TransactionId tid,PageId pageId){
        return getLockByPidAndTid(tid,pageId) != null;
    }

    /** Constants used for return codes in Field.compare */
    public enum LockType implements Serializable {
        XLOCK,SLOCK;


        @Override
        public String toString() {
            if (this == XLOCK)
                return "XLOCK";
            if (this == SLOCK)
                return "SLOCK";
            throw new IllegalStateException("impossible to reach here");
        }


    }

    private class PageLock{
        private LockType type;
        private PageId pageId;
        private TransactionId tid;

        public PageLock(LockType type, PageId pageId, TransactionId tid) {
            this.type = type;
            this.pageId = pageId;
            this.tid = tid;
        }

        public void updateToXLock() {
            this.type = LockType.XLOCK;
        }

        public LockType getType() {
            return type;
        }

        public PageId getPageId() {
            return pageId;
        }

        public TransactionId getTid() {
            return tid;
        }

        @Override
        public int hashCode() {
            return Objects.hash(tid, pageId);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj){
                return true;
            }
            if (!(obj instanceof PageLock)){
                return false;
            }
            PageLock p1 = (PageLock) obj;
            return this.pageId.equals(p1.pageId) && this.tid.equals(p1.tid);
        }
    }


}
