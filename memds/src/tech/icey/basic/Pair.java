package tech.icey.basic;

public class Pair<T1, T2> {
    public Pair(T1 first, T2 second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (o.getClass() == this.getClass()) {
            var thatPair = (Pair<T1, T2>) o;
            return thatPair.first.equals(this.first) && thatPair.second.equals(this.second);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "Pair<" + first.toString() + ", " + second.toString() + ">";
    }

    public T1 getFirst() { return this.first; }

    public T2 getSecond() { return this.second; }

    private T1 first;

    private T2 second;
}
