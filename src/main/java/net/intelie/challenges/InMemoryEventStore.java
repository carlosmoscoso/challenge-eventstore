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

public class InMemoryEventStore implements EventStore {

    private ConcurrentMap<String, ConcurrentNavigableMap<Long, List<Event>>> state;

    public InMemoryEventStore() {
        state = new ConcurrentHashMap<>();
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
                return current;
            }

            @Override
            public void remove() {
                state.computeIfPresent(current.type(), (type, events) ->
                        events.remove(current.timestamp()) != null && events.isEmpty() ?
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
