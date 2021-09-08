package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DeadlockException;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

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
    private static int maxNumPages;
    private LockManager lockManager = new LockManager();

    class LockManager {
        class LockItem {
            List<TransactionId> readTids;
            TransactionId writeTid;
            LockItem() {
                readTids = new ArrayList<>();
                writeTid = null;
            }
        }

        private Map<PageId, LockItem> lPgLocks = new HashMap<>();
        private Map<TransactionId, Set<TransactionId>> waitList = new HashMap<>();
        private Random rand;

        LockManager() {
            this.rand = new Random();
        }

        synchronized void lock(TransactionId tid, PageId pid, Permissions perm) throws DeadlockException {
            if (!lPgLocks.containsKey(pid)) {
                lPgLocks.put(pid, new LockItem());
            }
            LockItem item = lPgLocks.get(pid);

            if (Permissions.READ_ONLY.equals(perm)) {
                if (item.readTids.contains(tid)) {
                    return;
                }

                while (item.writeTid != null && !item.writeTid.equals(tid)) {
                    try {
                        if (waitList.containsKey(item.writeTid) && waitList.get(item.writeTid).contains(tid)) {
                            throw new DeadlockException();
                        }
                        waitList.put(tid, new HashSet<>());
                        waitList.get(tid).add(item.writeTid);
                        Thread.sleep(rand.nextInt(4));
                    } catch (InterruptedException e) {
                        return;
                    }
                }
                waitList.remove(tid);
                item.readTids.add(tid);
            } else {
                //已经有写权限
                if (tid.equals(item.writeTid)) {
                    return;
                }
                while (item.writeTid != null
                        || (item.readTids.size() > 0 && !(item.readTids.size() == 1 && item.readTids.get(0).equals(tid))) ) {
                    try {
                        waitList.put(tid, new HashSet<>());
                        if (item.writeTid != null) {
                            if (waitList.containsKey(item.writeTid) && waitList.get(item.writeTid).contains(tid)) {
                                waitList.remove(tid);
                                throw new DeadlockException();
                            }
                            waitList.get(tid).add(item.writeTid);
                        }
                        for (int i = 0; i < item.readTids.size(); ++i) {
                            TransactionId t = item.readTids.get(i);
                            if (!t.equals(tid)) {
                                if (waitList.containsKey(t) && waitList.get(t).contains(tid)) {
                                    waitList.remove(tid);
                                    throw new DeadlockException();
                                }
                                waitList.get(tid).add(t);
                            }
                        }
                        Thread.sleep(rand.nextInt(4));
                    } catch (InterruptedException e) {
                        return;
                    }
                }

                waitList.remove(tid);
                item.writeTid = tid;
                item.readTids.remove(tid);
            }
        }

        synchronized void unlock(TransactionId tid) {
            lPgLocks.values().forEach(item -> {
                if (tid.equals(item.writeTid)) {
                    item.writeTid = null;
                }
                item.readTids.remove(tid);
            });
        }

        synchronized void unsafeReleasePage(TransactionId tid, PageId pid) {
            LockItem item = lPgLocks.get(pid);
            if (item == null) {
                return;
            }
            if (tid.equals(item.writeTid)) {
                item.writeTid = null;
            }
            item.readTids.remove(tid);
        }

        boolean holdsLock(TransactionId tid, PageId pid) {
            LockItem item = lPgLocks.get(pid);
            if (item == null) {
                return false;
            }

            if (tid.equals(item.writeTid)) {
                return true;
            }

            return item.readTids.contains(tid);
        }

        Set<PageId> getDirtyPages(TransactionId tid) {
            return lPgLocks.entrySet().stream()
                    .filter(o -> tid.equals(o.getValue().writeTid))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
        }
    }

    class PageData {
        Page page;
        Date date;
        PageData(Page page, Date date) {
            this.page = page;
            this.date = date;
        }
    }
    private Map<PageId, PageData> pages = new HashMap<>();
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // wildpea
        maxNumPages = numPages;
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
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // wildpea
        try {
            lockManager.lock(tid, pid, perm);
        } catch (DeadlockException e) {
            transactionComplete(tid, false);
            throw new TransactionAbortedException();
        }

        if (pages.containsKey(pid)) {
            PageData pageData = pages.get(pid);
            pageData.date = new Date();
            return pageData.page;
        }

        DbFile df = Database.getCatalog().getDatabaseFile(pid.getTableId());

        Page page = df.readPage(pid);
        if (pages.size() >= maxNumPages) {
            evictPage();
        }
        pages.put(pid, new PageData(page, new Date()));

        return page;
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
        // wildpea
        // not necessary for lab1|lab2
        lockManager.unsafeReleasePage(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // wildpea
        // not necessary for lab1|lab2
        lockManager.unlock(tid);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // wildpea
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // wildpea
        // not necessary for lab1|lab2
        if (commit) {
            try {
                flushPages(tid);
            } catch (Exception e) {
                System.out.println("oops");
            }
        } else {
            Set<PageId> lPages = lockManager.getDirtyPages(tid);
            for (PageId pid : lPages) {
                if (tid.equals(pages.get(pid).page.isDirty())) {
                    discardPage(pid);
                }
            }
        }
        transactionComplete(tid);
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
        // wildpea
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> upPgs = file.insertTuple(tid, t);
        for (Page page:upPgs) {
            page.markDirty(true, tid);
            if (!pages.containsKey(page.getId())) {
                pages.put(page.getId(), new PageData(page, new Date()));
                try {
                    lockManager.lock(tid, page.getId(), Permissions.READ_WRITE);
                } catch (DeadlockException e) {
                    throw new TransactionAbortedException();
                }
            }
        }
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
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // wildpea
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> upPgs = file.deleteTuple(tid, t);
        for (Page page: upPgs) {
            page.markDirty(true, tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // wildpea
        // not necessary for lab1
        for (Map.Entry<PageId, PageData> item: pages.entrySet()) {
            Page page = item.getValue().page;
            if (page.isDirty() != null) {
                DbFile file = Database.getCatalog().getDatabaseFile(item.getKey().getTableId());
                file.writePage(page);
            }
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
        // wildpea
        // not necessary for lab1
        pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // wildpea
        // not necessary for lab1
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pages.get(pid).page);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // wildpea
        // not necessary for lab1|lab2
        Set<PageId> lPages = lockManager.getDirtyPages(tid);
        for (PageId pid: lPages) {
            if (tid.equals(pages.get(pid).page.isDirty())) {
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // wildpea
        // not necessary for lab1
        if (pages.size() == 0) {
            return;
        }
        Optional<PageData> minPage = pages.values().stream().filter(v -> v.page.isDirty() == null).min(Comparator.comparing(v -> v.date));
        if (!minPage.isPresent()) {
            throw new DbException("no page can evict");
        }

        PageId pid = minPage.get().page.getId();
        if (pages.get(pid).page.isDirty() != null) {
            try {
                flushPage(pid);
            } catch (IOException e) {
                throw new DbException(e.getMessage());
            }
        }
        pages.remove(pid);
    }

}
