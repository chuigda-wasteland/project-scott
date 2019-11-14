package tech.icey.basic;

public class Pair<T1, T2> {
    public Pair(T1 first, T2 second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        var thatPair = (Pair<T1, T2>)o;
        return thatPair.first.equals(this.first) && thatPair.second.equals(this.second);
    }

    public T1 getFirst() { return this.first; }

    public T2 getSecond() { return this.second; }

    private T1 first;

    private T2 second;
}
