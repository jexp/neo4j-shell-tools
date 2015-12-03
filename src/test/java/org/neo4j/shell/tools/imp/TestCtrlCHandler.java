package org.neo4j.shell.tools.imp;

import org.neo4j.helpers.Cancelable;
import org.neo4j.shell.CtrlCHandler;

/**
* @author mh
* @since 05.10.15
*/
public class TestCtrlCHandler implements CtrlCHandler {
    @Override
    public Cancelable install(Runnable runnable) {
        return new Cancelable() {
            @Override
            public void cancel() {

            }
        };
}
}
