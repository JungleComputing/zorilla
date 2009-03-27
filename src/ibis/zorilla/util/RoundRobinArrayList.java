package ibis.zorilla.util;

import java.util.ArrayList;

public final class RoundRobinArrayList extends ArrayList {

    private static final long serialVersionUID = 1L;
    
    int next = 0;

    public RoundRobinArrayList() {
        super();
    }

    public Object next() {
        Object result;

        if (next >= size()) {
            next = 0;
        }

        result = get(next);
        next++;

        return result;
    }

}
