package io.mycat.memory.unsafe.utils.sort;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

public class UnsafeStreamSorter {

    PriorityQueue<UnsafeSorterIterator> priorityQueue;
    private int numRecords = 0;

    /**
     * 当前迭代器
     */
    private UnsafeSorterIterator sorterIterator;

    UnsafeStreamSorter(
            final RecordComparator recordComparator,
            final PrefixComparator prefixComparator
    ) {

        final Comparator<UnsafeSorterIterator> comparator = new Comparator<UnsafeSorterIterator>() {
            @Override
            public int compare(UnsafeSorterIterator left, UnsafeSorterIterator right) {
                final int prefixComparisonResult = prefixComparator.compare(left.getKeyPrefix(), right.getKeyPrefix());
                if (prefixComparisonResult == 0) {
                    return recordComparator.compare(
                            left.getBaseObject(), left.getBaseOffset(),
                            right.getBaseObject(), right.getBaseOffset());
                } else {
                    return prefixComparisonResult;
                }
            }
        };

        priorityQueue = new PriorityQueue<UnsafeSorterIterator>(comparator);
    }

    public void addSorterIterator(UnsafeSorterIterator sorterIterator) throws IOException {
        /**
         * 添加迭代器到priorityQueue中
         */
        if(sorterIterator.hasNext()){
            sorterIterator.loadNext();
        }
        priorityQueue.offer(sorterIterator);

        numRecords += sorterIterator.getNumRecords();
    }

    public UnsafeSorterIterator getSortedIterator() throws IOException {
        return new UnsafeSorterIterator() {
            @Override
            public boolean hasNext() {
                return !priorityQueue.isEmpty() || (sorterIterator != null && sorterIterator.hasNext());
            }

            @Override
            public void loadNext() throws IOException {
                if (sorterIterator != null) {
                    if (sorterIterator.hasNext()) {
                        sorterIterator.loadNext();
                        /**
                         *添加一个完整迭代器集合给优先级队列，
                         *优先级队列为根据比较器自动调整想要的数据大小
                         * 每次都将spillReader添加到队列中进行新的调整
                         * 最后得到最小的元素，为出优先级队列做准备
                         */
                        priorityQueue.add(sorterIterator);
                    }
                }

                /**
                 * 出队列，当前spillreader最小的元素出优先级队列
                 */
                sorterIterator = priorityQueue.remove();
            }

            @Override
            public Object getBaseObject() {
                return sorterIterator.getBaseObject();
            }

            @Override
            public long getBaseOffset() {
                return sorterIterator.getBaseOffset();
            }

            @Override
            public int getRecordLength() {
                return sorterIterator.getRecordLength();
            }

            @Override
            public long getKeyPrefix() {
                return sorterIterator.getKeyPrefix();
            }

            @Override
            public int getNumRecords() {
                return numRecords;
            }
        };
    }
}
