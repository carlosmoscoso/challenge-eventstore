package net.intelie.challenges;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Comparator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Simple in-memory implementation of {@link EventStore}.
 * Note new events are added to the head of the list for
 * optimized search times. This is mostly on the assumption
 * that access will happen based on a recent event offset.
 *
 * <p>When many threads expect to access a given collection
 * frequently and concurrently, the single-threaded premise
 * that the world is stopped while you look at or change it
 * does not hold true anymore and a new model is necessary.
 * In this new model, a program must recognize there will be
 * other participants, and the world will keep changing. So
 * the best you can get is a immutable snapshot of the data
 * while others threads are still updating the system.
 *
 * <p>Concurrent collections ({@link ConcurrentHashMap} and
 * {@link ConcurrentSkipListMap}) are perfect for this case
 * where you have any number of reads as well as a tunable
 * number of writes expected to share a common collection.
 * They are normally preferable to similar "synchronized"
 * classes since there is no concept of locking the entire
 * table in a way that prevents other threads from changing
 * it, which makes it easier to scale in a multithreaded
 * context.
 *
 * @author Carlos Moscoso
 */
public class InMemoryEventStore implements EventStore {

    private ConcurrentMap<String, ConcurrentNavigableMap<Long, List<Event>>> state;

    /**
     * Creates a new event store meant for dev/testing purposes.
     */
    public InMemoryEventStore() {
        this(8, 0.9f, 1);
    }

    /**
     * Creates a new event store with an initial table size based on the given
     * number of elements (initialCapacity), table density (loadFactor), and
     * number of concurrently updating threads (concurrencyLevel).
     *
     * @param initialCapacity the initial capacity.
     * @param loadFactor the load factor.
     * @param concurrencyLevel the estimated number of concurrently updating threads.
     */
    public InMemoryEventStore(int initialCapacity, float loadFactor, int concurrencyLevel) {
        state = new ConcurrentHashMap<>(initialCapacity, loadFactor, concurrencyLevel);
    }

    @Override
    public void insert(Event event) {
        state.computeIfAbsent(event.type(), k -> new ConcurrentSkipListMap<>(EMPTY))
                .merge(event.timestamp(), singletonList(event), CONCAT);
    }

    @Override
    public void removeAll(String type) {
        state.remove(type);
    }

    @Override
    public EventIterator query(String type, long startTime, long endTime) {
        return new EventIterator() {
            Iterator<Event> iterator = FLATTEN.apply(state.getOrDefault(type, new ConcurrentSkipListMap<>(EMPTY))
                    .subMap(endTime, false, startTime, true)
                    .values()
                    .stream())
                    .iterator();

            Event current;

            @Override
            public boolean moveNext() {
                current = (iterator != null && iterator.hasNext()) ?
                        iterator.next() : null;

                return current != null;
            }

            @Override
            public Event current() {

                if (current == null) {
                    throw new IllegalStateException();
                }

                return current;
            }

            @Override
            public void remove() {
                state.computeIfPresent(current().type(), (type, events) ->
                        events.remove(current().timestamp()) != null && events.isEmpty() ?
                                null : events);
            }

            @Override
            public void close() {
                iterator = null;
            }
        };
    }

    private static final BinaryOperator<List<Event>> CONCAT = (events1, events2) -> {
        List<Event> events = new ArrayList<>();
        events.addAll(events1);
        events.addAll(events2);
        return events;
    };

    private static final Function<Stream<List<Event>>, List<Event>> FLATTEN = events ->
            events.reduce(emptyList(), CONCAT);

    private static final ConcurrentNavigableMap<Long, List<Event>> EMPTY =
            new ConcurrentSkipListMap<>(Comparator.reverseOrder());
}
