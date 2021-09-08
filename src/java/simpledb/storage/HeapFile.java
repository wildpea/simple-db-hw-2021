package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.awt.image.DataBuffer;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

import static sun.swing.MenuItemLayoutHelper.max;

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

    // see DbFile.java for javadocs
    @Override
    public Page readPage(PageId pid) {
        // wildpea
        if (pid.getTableId() != getId() || pid.getPageNumber() >= numPages()) {
            throw new IllegalArgumentException("not current tableId");
        }

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(f, "r")) {
            randomAccessFile.seek(getFileOffset(pid.getPageNumber()));
            byte[] b = new byte[BufferPool.getPageSize()];
            if (randomAccessFile.read(b) == -1) {
                throw new IllegalArgumentException("not current tableId");
            }
            return new HeapPage((HeapPageId) pid, b);
        } catch (Exception e) {
            throw new IllegalArgumentException("no such pgid");
        }
    }

    private long getFileOffset(int pageNo) {
        return (long) BufferPool.getPageSize() * pageNo;
    }

    // see DbFile.java for javadocs
    @Override
    public void writePage(Page page) throws IOException {
        // wildpea
        // not necessary for lab1
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(f, "rw")) {
            int pgNo = page.getId().getPageNumber();
            randomAccessFile.seek(getFileOffset(pgNo));
            randomAccessFile.write(page.getPageData());
        } catch (Exception e) {
            throw new IOException("write page error");
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // wildpea
        return (int) (f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    @Override
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // wildpea
        // not necessary for lab1
        ArrayList<Page> pgs = new ArrayList<>();
        HeapPage page;
        try {
            page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), Math.max(0, numPages() - 1)), Permissions.READ_WRITE);
            page.insertTuple(t);
        } catch (Exception e) {
            int pgId = numPages();
            synchronized (this) {
                if (numPages() == pgId) {
                    page = new HeapPage(new HeapPageId(getId(), pgId), HeapPage.createEmptyPageData());
                    writePage(page);
                }
            }
            page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), Math.max(0, numPages() - 1)), Permissions.READ_WRITE);
            page.insertTuple(t);
        }
        pgs.add(page);
        return pgs;
    }

    // see DbFile.java for javadocs
    @Override
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // wildpea
        // not necessary for lab1
        ArrayList<Page> pgs = new ArrayList<>();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        pgs.add(page);
        return pgs;
    }

    // see DbFile.java for javadocs
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        // wildpea
        class TIterator implements DbFileIterator {
            final private Permissions perm = Permissions.READ_ONLY;
            private TransactionId tid;
            private HeapPage curPage;
            private Iterator<Tuple> iter;
            private boolean opened = false;
            private int curPgNo = 0;

            TIterator(TransactionId tid) {
                this.tid = tid;
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                opened = true;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                try {
                    if (!opened) {
                        return false;
                    }
                    if (iter == null) {
                        if (curPgNo >= numPages()) {
                            return false;
                        }

                        curPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), curPgNo), perm);
                        iter = curPage.iterator();
                    }

                    if (iter.hasNext()) {
                        return true;
                    }

                    while (++curPgNo < numPages()) {
                        curPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), curPgNo), perm);
                        iter = curPage.iterator();
                        if (iter.hasNext()) {
                            return true;
                        }
                    }

                    return false;
                } catch (DbException e) {
                    return false;
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!opened || iter == null) {
                    throw new NoSuchElementException("not opened");
                }
                return iter.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                curPgNo = 0;
                curPage = null;
                iter = null;
            }

            @Override
            public void close() {
                opened = false;
            }
        }

        return new TIterator(tid);
    }

}

