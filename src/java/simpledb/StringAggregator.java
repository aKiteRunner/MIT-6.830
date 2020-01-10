package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield, afield;
    private Type gbfieldtype;
    private Op what;
    private int cnt;
    private HashMap<Field, Integer> cntGroup;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) throw new IllegalArgumentException("only supports COUNT");
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.cnt = 0;
        cntGroup = null;
        if (gbfield != Aggregator.NO_GROUPING) {
            cntGroup = new HashMap<>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field f = tup.getField(afield);
        String val = ((StringField)f).getValue();
        if (gbfield == Aggregator.NO_GROUPING) {
            if (what == Op.COUNT) cnt++;
        } else {
            Field key = tup.getField(gbfield);
            if (what == Op.COUNT) {
                cntGroup.put(key, cntGroup.getOrDefault(key, 0) + 1);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        if (gbfield == Aggregator.NO_GROUPING) {
            return new NoGroupIterator();
        } else {
            return new GroupIterator();
        }
    }


    private class GroupIterator implements OpIterator {
        private Iterator<Map.Entry<Field, Integer>> it;
        private TupleDesc td;

        public GroupIterator() {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            it = cntGroup.entrySet().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Map.Entry<Field, Integer> entry = it.next();
            Tuple t = new Tuple(td);
            t.setField(0, entry.getKey());
            t.setField(1, new IntField(entry.getValue()));
            return t;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return td;
        }

        @Override
        public void close() {
            it = null;
        }
    }

    private class NoGroupIterator implements OpIterator {
        private Iterator<Tuple> it;
        private TupleDesc td;
        private List<Tuple> data;

        public NoGroupIterator() {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
            Tuple t = new Tuple(td);
            t.setField(0, new IntField(cnt));
            data = Collections.singletonList(t);
        }

        @Override
        public void open() {
            it = data.listIterator();
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public Tuple next() throws NoSuchElementException {
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return td;
        }

        @Override
        public void close() {
            it = null;
        }
    }

}
