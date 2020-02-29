package simpledb;

import java.io.*;
import java.util.*;
import java.lang.Math;
//import java.util.logging.Logger;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
//    Logger log = Logger.getGlobal();

    private File backfile;
    private TupleDesc desc;
    private RandomAccessFile rawFile;
    private int pageSize;
    private int id;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        backfile = f;
        desc = td;
        id = backfile.getAbsoluteFile().hashCode();
        try {
            rawFile = new RandomAccessFile(f, "rw");
        } catch (IOException e) {
            e.printStackTrace();
        }
        pageSize = BufferPool.getPageSize();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return backfile;
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
    public int getId() {
        // some code goes here
        return id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return desc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int n = numPages();
        int pgno = pid.getPageNumber();
        if (pgno >= n || pgno < 0) return null;
        byte[] data = new byte[pageSize];
        try {
//            log.info("n: " + n + " pgno:" + pgno);
            rawFile.seek(pgno * pageSize);
            rawFile.read(data);
            return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), data);
        } catch (IOException e) {
//            log.info("!!!" + e.toString());
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pgno = page.getId().getPageNumber();
        rawFile.seek(pgno * pageSize);
        rawFile.write(page.getPageData());
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) backfile.length() / pageSize;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> res = new ArrayList<>(1);
        for (int i = numPages() - 1; i >= 0; i--) {
            // look for a empty slot
            HeapPageId hpid = new HeapPageId(id, i);
            HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_ONLY);
            if (p.getNumEmptySlots() > 0) {
                Database.getBufferPool().updateToWriteLock(tid, p.pid);
                p.insertTuple(t);
                res.add(p);
                return res;
            }
            if (Database.getBufferPool().holdsReadLock(tid, hpid)) {
                Database.getBufferPool().releaseReadLock(tid, hpid);
            }
        }
        // no more empty page
        // extend file
        HeapPageId hpid = new HeapPageId(id, numPages());
        rawFile.setLength(backfile.length() + pageSize);
        HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_WRITE);
        newPage.insertTuple(t);
        res.add(newPage);
        return res;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        if (t.getRecordId().getPageId().getTableId() != id) throw new DbException("mismatch table id");
        Page p = Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        HeapPage hp = (HeapPage)p;
        hp.deleteTuple(t);
        ArrayList<Page> res = new ArrayList<>(1);
        res.add(p);
        return res;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapPageIterator(tid);
    }

    private class HeapPageIterator implements DbFileIterator {
        private int pidx, n;
        private Iterator<Tuple> it;
        private TransactionId tid;

        public HeapPageIterator(TransactionId tid) {
            pidx = 0;
            n = numPages();
            it = null;
            this.tid = tid;
        }

        private void readPage()
                throws DbException, TransactionAbortedException {
            while (pidx < n && (it == null || !it.hasNext())) {
                Page p = Database.getBufferPool().getPage(tid, new HeapPageId(id, pidx), Permissions.READ_ONLY);
                if (! (p instanceof HeapPage)) throw new DbException("HeapFile requires HeapPage");
                HeapPage hp = (HeapPage) p;
                it = hp.iterator();
                pidx++;
//                log.info("" + pidx + "" + it.hasNext());
            }
        }

        public void open()
                throws DbException, TransactionAbortedException {
            readPage();
        }

        public boolean hasNext()
                throws DbException, TransactionAbortedException {
            return it != null && it.hasNext();
        }

        public Tuple next()
                throws DbException, TransactionAbortedException, NoSuchElementException {
            if (it == null || !it.hasNext()) throw new NoSuchElementException();
            Tuple t = it.next();
            if (!it.hasNext()) {
                readPage();
            }
//            log.info(t.toString());
            return t;
        }

        public void rewind() throws DbException, TransactionAbortedException {
            pidx = 0;
            it = null;
            readPage();
        }

        public void close() {
            pidx = 0;
            it = null;
        }
    }
}

