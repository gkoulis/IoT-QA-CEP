package gr.hua.dit.iotvm.iotcore.demonstration;

import java.util.Optional;

public interface StorageAdapter<T> {

    void insert(T value);

    Optional<T> getTimely();

    void delete(T value);
}
