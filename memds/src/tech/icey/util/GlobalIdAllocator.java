package tech.icey.util;

public class GlobalIdAllocator {
    private static GlobalIdAllocator instance = new GlobalIdAllocator();

    private int id = 0;

    public int nextId() {
        this.id++;
        return this.id;
    }

    public static GlobalIdAllocator getInstance() {
        return instance;
    }
}
