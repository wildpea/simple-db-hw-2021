package simpledb.storage;

import simpledb.common.Type;
import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    protected List<TDItem> items = new ArrayList<>();

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        @Override
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // wildpea
        return items.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // wildpea
        if (typeAr.length != fieldAr.length) {
            throw new RuntimeException("type and field not match");
        }
        int len = fieldAr.length;
        for (int i = 0; i < len; ++i) {
            items.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // wildpea
        for (Type type : typeAr) {
            items.add(new TDItem(type, ""));
        }
    }

    public TupleDesc(TupleDesc td) {
        // wildpea
        int len = td.numFields();
        for (int i = 0; i < len; ++i) {
            items.add(new TDItem(td.getFieldType(i), td.getFieldName(i)));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // wildpea
        return items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // wildpea
        if (items.size() > i) {
            return items.get(i).fieldName;
        }
        throw new NoSuchElementException("no such element");
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // wildpea
        if (items.size() > i) {
            return items.get(i).fieldType;
        }
        throw new NoSuchElementException("no such element");
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // wildpea
        if (name == null) {
            throw new NoSuchElementException();
        }
        for (int i = 0; i < items.size(); ++i) {
            if (items.get(i).fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // wildpea
        return items.size() * Type.INT_TYPE.getLen();
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        //wildpea
        int len1 = td1.numFields();
        int len = len1 + td2.numFields();
        Type[] tp = new Type[len];
        String[] fd = new String[len];
        for (int i = 0; i <len; ++i) {
            if (i < len1) {
                tp[i] = td1.getFieldType(i);
                fd[i] = td1.getFieldName(i);
            } else {
                tp[i] = td2.getFieldType(i - len1);
                fd[i] = td2.getFieldName(i - len1);
            }
        }

        return new TupleDesc(tp, fd);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    @Override
    public boolean equals(Object o) {

        if (o == null) {
            return false;
        }
        // wildpea
        if (!o.getClass().equals(TupleDesc.class)) {
            return false;
        }

        TupleDesc tb = (TupleDesc)o;
        if (tb.numFields() != items.size()) {
            return false;
        }

        for (int i = 0; i < items.size(); i++) {
            if (!tb.getFieldName(i).equals(getFieldName(i))
                    || !tb.getFieldType(i).equals(getFieldType(i))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    @Override
    public String toString() {
        // wildpea
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < items.size(); ++i) {
            TDItem item = items.get(i);
            sb.append(item.fieldType)
                    .append("[")
                    .append(i)
                    .append("](")
                    .append(item.fieldName)
                    .append("),");
        }
        String rst = sb.toString();
        return rst.length() > 0 ? rst.substring(0, rst.length() - 1) : rst;
    }
}
