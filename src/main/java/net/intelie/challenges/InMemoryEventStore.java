package net.intelie.challenges;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class InMemoryEventStore implements  EventStore {

    public final ConcurrentMap<String, ConcurrentNavigableMap<Long, List<Event>>> state;

    public InMemoryEventStore() {
        state = new ConcurrentHashMap<>();
    }

    @Override
    public void insert(Event event) {
        state.putIfAbsent(event.type(), new ConcurrentSkipListMap<>());
        state.get(event.type()).merge(event.timestamp(), singletonList(event), CONCAT);
    }

    @Override
    public void removeAll(String type) {
        state.remove(type);
    }

    @Override
    public EventIterator query(String type, long startTime, long endTime) {
        return null;
    }

    public List<Event> queryAsList(String type, long startTime, long endTime) {

        Stream<List<Event>> eventsAsStream = state.get(type)
                .subMap(startTime, endTime)
                .values()
                .stream();

        return FLATTEN.apply(eventsAsStream);
    }

    private static final BinaryOperator<List<Event>> CONCAT = (events1, events2) -> {
        List<Event> events = new ArrayList<>();
        events.addAll(events1);
        events.addAll(events2);
        return events;
    };

    private static final Function<Stream<List<Event>>, List<Event>> FLATTEN = events ->
            events.reduce(emptyList(), CONCAT);
}
