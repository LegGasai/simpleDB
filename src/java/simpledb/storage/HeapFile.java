package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
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

    private final File file;
    private final TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
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
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pageNo = pid.getPageNumber();
        int pageSize = BufferPool.getPageSize();
        int offset = pageNo * pageSize;
        Page page;
        RandomAccessFile randomAccessFile = null;
        try{
            randomAccessFile = new RandomAccessFile(this.file,"r");
            byte [] data = new byte[pageSize];
            randomAccessFile.seek(offset);
            randomAccessFile.read(data);
            page = new HeapPage((HeapPageId) pid,data);
        }catch (IOException e){
            throw new RuntimeException(e);
        }finally {
            if (randomAccessFile!=null){
                try {
                    randomAccessFile.close();
                }catch (IOException e){
                    throw new RuntimeException(e);
                }
            }
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageSize = BufferPool.getPageSize();
        int pageNumber = page.getId().getPageNumber();
        int offset = pageSize * pageNumber;
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            randomAccessFile.seek(offset);
            randomAccessFile.write(page.getPageData());
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.ceil(file.length()*1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pages = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), i), Permissions.READ_WRITE);
            if (page.getNumEmptySlots() == 0){
                Database.getBufferPool().unsafeReleasePage(tid, page.pid);
                continue;
            }
            page.insertTuple(t);
            pages.add(page);
            return pages;
        }
        // need new pages;
        BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(file,true));
        byte[] emptyPageData = HeapPage.createEmptyPageData();
        bw.write(emptyPageData);
        bw.close();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), this.numPages()-1), Permissions.READ_WRITE);
        page.markDirty(true,tid);
        page.insertTuple(t);
        pages.add(page);
        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pages = new ArrayList<>();
        RecordId recordId = t.getRecordId();
        PageId pid = recordId.getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        page.markDirty(true,tid);
        pages.add(page);
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this,tid);
    }

    public static final class HeapFileIterator implements DbFileIterator{

        private final HeapFile file;
        private final TransactionId tid;
        private int pageNo;
        private Iterator<Tuple> iterator;

        public HeapFileIterator(HeapFile file, TransactionId tid) {
            this.file = file;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.pageNo = 0;
            this.iterator = getPageIterator(pageNo);
        }

        private Iterator<Tuple> getPageIterator(int pageNo) throws TransactionAbortedException,DbException{
            if (pageNo < 0 || pageNo >= file.numPages()){
                throw new DbException(String.format("[getPageIterator()]:HeapFile[%d] does not contain page[%d]\n",file.getId(), pageNo));
            }
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(file.getId(), pageNo), Permissions.READ_ONLY);
            return page.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            // current page is over,try next page
            while (iterator !=null && !iterator.hasNext()){
                if (pageNo < file.numPages()-1){
                    pageNo++;
                    iterator = getPageIterator(pageNo);
                }else{
                    iterator = null;
                }
            }
            if (iterator == null) { return false; }
            return iterator.hasNext();

        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (iterator == null || !iterator.hasNext()) { throw new NoSuchElementException(); }
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            iterator = null;
        }
    }
}

