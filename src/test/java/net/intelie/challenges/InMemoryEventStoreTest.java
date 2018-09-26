package net.intelie.challenges;

import org.junit.Test;
import org.junit.After;
import org.junit.Before;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

public class InMemoryEventStoreTest {

    Event t0 = new Event("type", 0);
    Event t1 = new Event("type", 1);
    Event t2 = new Event("type", 2);

    InMemoryEventStore store = new InMemoryEventStore();

    @Before
    public void setUp() throws Exception {
        store.insert(t0);
        store.insert(t1);
        store.insert(t2);
    }

    @Test
    public void queryIteratorMoveNext() {

        EventIterator iterator = store.query("type", 0, 0);

        assertFalse(iterator.moveNext());
    }

    @Test
    public void queryIteratorMoveNextOne() {

        EventIterator iterator = store.query("type", 0, 1);

        assertTrue(iterator.moveNext());
        assertEquals(t0, iterator.current());
        assertFalse(iterator.moveNext());
    }

    @Test
    public void queryIteratorMoveNextTwo() {

        EventIterator iterator = store.query("type", 0, 2);

        assertTrue(iterator.moveNext());
        assertEquals(t1, iterator.current());
        assertTrue(iterator.moveNext());
        assertEquals(t0, iterator.current());
        assertFalse(iterator.moveNext());
    }

    @Test
    public void queryIteratorMoveNextThree() {

        EventIterator iterator = store.query("type", 0, 3);

        assertTrue(iterator.moveNext());
        assertEquals(t2, iterator.current());
        assertTrue(iterator.moveNext());
        assertEquals(t1, iterator.current());
        assertTrue(iterator.moveNext());
        assertEquals(t0, iterator.current());
        assertFalse(iterator.moveNext());
    }

    @Test(expected = IllegalStateException.class)
    public void queryIteratorWhenMoveNextReturningFalse() {

        EventIterator iterator = store.query("type", 0, 0);

        assertFalse(iterator.moveNext());

        iterator.current();
    }

    @Test(expected = IllegalStateException.class)
    public void queryIteratorWhenMoveNextNeverCalled() {

        EventIterator iterator = store.query("type", 0, 3);

        iterator.current();
    }

    @Test
    public void queryIteratorRemove() {

        EventIterator iterator = store.query("type", 0, 3);

        while (iterator.moveNext()) {
            iterator.remove();
        }

        iterator = store.query("type", 0, 3);

        assertFalse(iterator.moveNext());
    }

    @After
    public void tearDown() throws Exception {
        store.removeAll("type");
    }
}
