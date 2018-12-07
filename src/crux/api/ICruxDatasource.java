package crux.api;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents the database as of a specific business and
 * transaction time.
 */
public interface ICruxDatasource {
    /**
     * Returns the document map for an entity.
     *
     * @param eid an object that can be coerced into an entity id.
     * @return    the entity document map.
     */
    public Map entity(Object eid);

    /**
     * Returns the entity tx for an entity.
     *
     * @param eid an object that can be coerced into an entity id.
     * @return    the entity transaction details.
     */
    public Map entityTx(Object eid);

    /**
     * Returns a new snapshot allowing for lazy query results in a
     * try-with-resources block using {@link #q(Closeable snapshot,
     * Map query)}.
     *
     * @return an implementation specific snapshot
     */
    public Closeable newSnapshot();

    /**
     * Queries the db.
     *
     * @param query the query in map, vector or string form.
     * @return      a set of result tuples.
     */
    public Set<List> q(Object query);

    /**
     * Queries the db lazily.
     *
     * @param snapshot a snapshot from {@link #newSnapshot()]}.
     * @param query    the query in map, vector or string form.
     * @return         a lazy sequence of result tuples.
     */
    public Iterable<List> q(Closeable snapshot, Object query);
}