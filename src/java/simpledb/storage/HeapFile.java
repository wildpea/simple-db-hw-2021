package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;
    private Map<PageId, HeapPage> pages = new HashMap<>();
    private int lastPgNo = -1;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // wildpea
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // wildpea
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    @Override
    public int getId() {
        // wildpea
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    @Override
    public TupleDesc getTupleDesc() {
        // wildpea
        return td;
    }

    private void loadPage() {
        try (FileInputStream is = new FileInputStream(f)) {
            byte[] c = new byte[BufferPool.getPageSize()];
            while (is.read(c) != -1) {
                lastPgNo++;
                HeapPageId hpId = new HeapPageId(getId(), lastPgNo);
                HeapPage page = new HeapPage(hpId, c);
                pages.put(hpId, page);
            }
        } catch (Exception e) {
            System.out.println("error");
        }
    }

    // see DbFile.java for javadocs
    @Override
    public Page readPage(PageId pid) {
        // wildpea
        if (pages.size() == 0) {
            loadPage();
        }

        if (pages.containsKey(pid)) {
            return pages.get(pid);
        }
        throw new IllegalArgumentException("no such pgid");
    }

    // see DbFile.java for javadocs
    @Override
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // wildpea
        if (pages.size() == 0) {
            loadPage();
        }
        return pages.size();
    }

    // see DbFile.java for javadocs
    @Override
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    @Override
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        // wildpea
        ;
//        Page page = Database.getBufferPool().getPage(tid, , perm);


        class TIterator implements DbFileIterator {
            final private Permissions perm = Permissions.READ_ONLY;
            private TransactionId tid;
            Iterator<PageId> curPgIter;
            private HeapPage curPage;
            private Iterator<Tuple> iter;
            private boolean opened = false;

            TIterator(TransactionId tid) {
                this.tid = tid;
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                if (pages.size() == 0) {
                    loadPage();
                    if (pages.size() == 0) {
                        throw new DbException("no data");
                    }
                }
                rewind();
                opened = true;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!opened) {
                    return false;
                }
                if (iter == null) {
                    if (!curPgIter.hasNext()) {
                        return false;
                    }

                    curPage = (HeapPage) Database.getBufferPool().getPage(tid, (HeapPageId)curPgIter.next(), perm);
                    iter = curPage.iterator();
                }

                if (iter.hasNext()) {
                    return true;
                }

                while (curPgIter.hasNext()) {
                    curPage = (HeapPage) Database.getBufferPool().getPage(tid, (HeapPageId)curPgIter.next(), perm);
                    iter = curPage.iterator();
                    if (iter.hasNext()) {
                        return true;
                    }
                }

                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!opened || iter == null) {
                    throw new NoSuchElementException("not opened");
                }
                if (iter.hasNext()) {
                    return iter.next();
                }

                while (curPgIter.hasNext()) {
                    curPage = (HeapPage) Database.getBufferPool().getPage(tid, (HeapPageId)curPgIter.next(), perm);
                    iter = curPage.iterator();

                    if (iter.hasNext()) {
                        return iter.next();
                    }
                }

                return null;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                curPgIter = pages.keySet().iterator();
                if (curPgIter.hasNext()) {
                    curPage = (HeapPage) Database.getBufferPool().getPage(tid, (HeapPageId)curPgIter.next(), perm);
                    iter = curPage.iterator();
                } else {
                    throw new DbException("no item");
                }
            }

            @Override
            public void close() {
                opened = false;
            }
        }

        return new TIterator(tid);
    }

}

