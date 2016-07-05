package edu.jhu.fcriscu1.taskframework.datastructure;
import rx.Observable;

import rx.subjects.PublishSubject;
import rx.subjects.Subject;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;

import java.util.concurrent.TimeUnit;

/**
 * Design for implementing a BlockingQueue within the RxJava framework
 * ref: http://www.nurkiewicz.com/2015/07/consuming-javautilconcurrentblockingque.html
 */
public class ObservableQueue<T> implements BlockingQueue<T>, Closeable {

    private final Subject<T, T> subject = PublishSubject.<T>create().toSerialized();

    public Observable<T> observe() {
        return subject;
    }

    @Override
    public boolean add(T t) {
        return offer(t);
    }

    @Override
    public boolean offer(T t) {
        subject.onNext(t);
        return true;
    }

    @Override
    public void close() throws IOException {
        subject.onCompleted();
    }
    @Override
    public T remove() {
        try {
            return noSuchElement();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public T poll() {
        return null;
    }

    @Override
    public T element() {
        try {
            return noSuchElement();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private T noSuchElement() throws Exception{
        throw new NoSuchElementException();
    }

    @Override
    public T peek() {
        return null;
    }

    @Override
    public void put(T t) throws InterruptedException {
        offer(t);
    }

    @Override
    public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
        return offer(t);
    }

    @Override
    public T take() throws InterruptedException {
        throw new UnsupportedOperationException("Use observe() instead");
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        return null;
    }

    @Override
    public int remainingCapacity() {
        return 0;
    }

    @Override
    public boolean remove(Object o) {
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        c.forEach(this::offer);
        return true;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    @Override
    public void clear() {
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public boolean contains(Object o) {
        return false;
    }

    @Override
    public Iterator<T> iterator() {
        return Collections.emptyIterator();
    }

    @Override
    public Object[] toArray() {
        return new Object[0];
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return a;
    }

    @Override
    public int drainTo(Collection<? super T> c) {
        return 0;
    }

    @Override
    public int drainTo(Collection<? super T> c, int maxElements) {
        return 0;
    }

}
