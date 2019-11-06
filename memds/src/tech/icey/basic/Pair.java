package tech.icey.basic;

public class Pair<T1, T2> {
    public Pair(T1 first, T2 second) {
        this.first = first;
        this.second = second;
    }

    public T1 getFirst() { return this.first; }

    public T2 getSecond() { return this.second; }

    private T1 first;

    private T2 second;
}
