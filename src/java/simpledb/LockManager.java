package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

public class LockManager {
    public static Logger log = Logger.getGlobal();

    private final HashMap<PageId, ReadWriteLock> lockPool;
    private final HashMap<TransactionId, HashSet<PageLock>> tidToPages;

    private class PageLock {
        Permissions p;
        PageId pid;

        PageLock(Permissions p, PageId pid) {
            this.p = p;
            this.pid = pid;
        }

        @Override
        public int hashCode() {
            return pid.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof PageLock)) return false;
            PageLock other = (PageLock) obj;
            return pid.equals(other.pid);
        }
    }

    public LockManager() {
        lockPool = new HashMap<>();
        tidToPages = new HashMap<>();
    }

    private ReadWriteLock getLock(TransactionId tid, PageId pid) {
//        log.info(tid + " get page " + pid);
        if (!lockPool.containsKey(pid)) {
            log.info("new lock " + tid + " " + pid);
            ReadWriteLock lock = new ReentrantReadWriteLock();
            synchronized (lockPool) {
                if (!lockPool.containsKey(pid)) {
                    log.info(tid + " put lock " + pid);
                    lockPool.put(pid, lock);
                }
            }
        }
        HashSet<PageLock> lockList;
        if (!tidToPages.containsKey(tid)) {
            lockList = new HashSet<>();
            tidToPages.put(tid, lockList);
        }
        return lockPool.get(pid);
    }

    public synchronized void lock(TransactionId tid, PageId pid, Permissions perm) {
        if (perm == Permissions.READ_ONLY) readLock(tid, pid);
        if (perm == Permissions.READ_WRITE) writeLock(tid, pid);
    }

    public synchronized void unlock(TransactionId tid, PageId pid) {
        for (PageLock plk : tidToPages.get(tid)) {
            if (plk.pid.equals(pid)) {
                if (plk.p == Permissions.READ_ONLY) {
                    readUnlock(tid, pid);
                } else {
                    writeUnlock(tid, pid);
                }
                break;
            }
        }
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        return tidToPages.get(tid).contains(new PageLock(null, pid));
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid, Permissions perm) {
        for (PageLock plk : tidToPages.get(tid)) {
            if (plk.pid.equals(pid) && plk.p == perm) {
                return true;
            }
        }
        return false;
    }

    public synchronized void updateToWriteLock(TransactionId tid, PageId pid) {
        for (PageLock plk : tidToPages.get(tid)) {
            if (plk.pid.equals(pid)) {
                if (plk.p == Permissions.READ_ONLY) {
                    readUnlock(tid, pid);
                    writeLock(tid, pid);
                }
                break;
            }
        }
    }

    private void readLock(TransactionId tid, PageId pid) {
        ReadWriteLock lock = getLock(tid, pid);
        HashSet<PageLock> lockList = tidToPages.get(tid);
        PageLock plk = new PageLock(Permissions.READ_ONLY, pid);
        if (lockList.contains(plk)) return;
        lock.readLock().lock();
        lockList.add(plk);
        log.info(tid + " read lock success " + pid);
    }

    private void writeLock(TransactionId tid, PageId pid) {
        ReadWriteLock lock = getLock(tid, pid);
        HashSet<PageLock> lockList = tidToPages.get(tid);
        PageLock plk = new PageLock(Permissions.READ_WRITE, pid);
        if (lockList.contains(plk)) return;
        lock.writeLock().lock();
        lockList.add(plk);
        log.info(tid + " write lock success " + pid);
    }

    public void readUnlock(TransactionId tid, PageId pid) {
        ReadWriteLock lock = getLock(tid, pid);
        lock.readLock().unlock();
        PageLock plk = new PageLock(Permissions.READ_ONLY, pid);
        tidToPages.get(tid).remove(plk);
        log.info(tid + " read unlock success " + pid);
    }

    private void writeUnlock(TransactionId tid, PageId pid) {
        ReadWriteLock lock = getLock(tid, pid);
        lock.writeLock().unlock();
        PageLock plk = new PageLock(Permissions.READ_WRITE, pid);
        tidToPages.get(tid).remove(plk);
        log.info(tid + " write unlock success " + pid);
    }

    public void releaseAll(TransactionId tid) {
        HashSet<PageLock> pageLocks = tidToPages.remove(tid);
        for (PageLock l : pageLocks) {
            if (l.p == Permissions.READ_ONLY) {
                lockPool.get(l.pid).readLock().unlock();
            } else {
                lockPool.get(l.pid).writeLock().unlock();
            }
        }
    }

    public Collection<PageId> lockedPages(TransactionId tid) {
        HashSet<PageLock> pageLocks = tidToPages.get(tid);
        List<PageId> pages = new LinkedList<>();
        for (PageLock plk : pageLocks) {
            pages.add(plk.pid);
        }
        return pages;
    }
}
