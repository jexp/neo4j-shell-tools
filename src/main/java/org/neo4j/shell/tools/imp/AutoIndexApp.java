package org.neo4j.shell.tools.imp;

import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.shell.*;
import org.neo4j.shell.kernel.apps.TransactionProvidingApp;

/**
 * Created by mh on 04.07.13.
 */
public class AutoIndexApp extends TransactionProvidingApp {

    {
        addOptionDefinition( "t", new OptionDefinition( OptionValueType.MUST,
                "The type of index, either Node or Relationship" ) );
        addOptionDefinition( "r", new OptionDefinition( OptionValueType.NONE,
                "Removes removes given properties from auto-indexing." ) );
    }

    enum Type { Node, Relationship }
    @Override
    protected Continuation exec(AppCommandParser parser, Session session, Output out) throws Exception {
        Type type = Type.valueOf(parser.option("t", "Node"));
        boolean remove = parser.options().containsKey("r");

        IndexManager index = getServer().getDb().index();
        AutoIndexer<? extends PropertyContainer> autoIndexer = type==Type.Node ? index.getNodeAutoIndexer() : index.getRelationshipAutoIndexer();
        autoIndexer.setEnabled(true);

        out.println((remove ? "Disabling" : "Enabling") + " auto-indexing of " + type + " properties: " + parser.arguments());
        for (String argument : parser.arguments()) {
            if (remove) {
                autoIndexer.stopAutoIndexingProperty(argument);
            }
            else {
                autoIndexer.startAutoIndexingProperty(argument);
            }
        }
        return Continuation.INPUT_COMPLETE;
    }

    @Override
    public String getName() {
        return "auto-index";
    }
}
