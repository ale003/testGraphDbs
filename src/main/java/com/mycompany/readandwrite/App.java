package com.mycompany.readandwrite;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.oupls.sail.GraphSail;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;

/**
 * Hello world!
 *
 */
public class App {

    public static enum DBS {

        TITAN,
        NEO4J,
        ORIENT
    }

    public static void main(String[] args) {
        try {

            String ip = "192.168.0.129";
            String keyspace = "testKeysp";
            // N of articles to be generated.
            int Narticles = 100000;
            // size of the buffer to commit each time
            int commitBufferSize = 100;
            // N of articles to commit before trying reads
            int readStep = 100;

            ArrayList<SimulateReadAndWrite> simulateAll = new ArrayList<SimulateReadAndWrite>();

            int Ndbs = 0;

            for (DBS dbs : DBS.values()) {
                SailRepository sr;

                switch (dbs) {
                    case TITAN:
                        sr = createTitanConnection(ip, keyspace);
                        break;
                    case NEO4J:
                        sr = createNeo4jConnection(keyspace);
                        break;
                    case ORIENT:
                        sr = createOrientConnection(keyspace);
                        break;
                    default:
                        sr = null;
                        break;
                }

                if (sr == null) {
                    throw new Exception("Something wrong while connecting to " + dbs.toString());
                }

                String currentNamespace = "http://mynamespace#";

                simulateAll.add(new SimulateReadAndWrite(sr, "test" + dbs.toString(), Narticles, readStep, commitBufferSize, dbs.toString(), keyspace, currentNamespace, false));

                simulateAll.get(Ndbs).start();
                Ndbs++;
            }

            int Nfinished = 0;
            int k;
            while (Nfinished != Ndbs) {
                Nfinished = 0;
                k = 0;
                for (DBS dbs : DBS.values()) {
                    if (simulateAll.get(k).IsProcessCompleted()) {
                        Nfinished++;
                    } else {
                        System.out.println("Process for db " + dbs.toString() + " is at " + simulateAll.get(k).GetProgress());
                    }
                    k++;
                }
                Thread.sleep(10000);
            }

        } catch (Exception ex) {
            Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static SailRepository createNeo4jConnection(String keyspace) throws RepositoryException {
        Neo4jGraph graph = new Neo4jGraph("tmp/neo4j/" + keyspace);
        SailRepository sr = new SailRepository(new GraphSail(graph));
        sr.initialize();
        return sr;
    }

    public static SailRepository createOrientConnection(String keyspace) throws RepositoryException {
        OrientGraph graph = new OrientGraph("local:tmp/orient/" + keyspace);
        //OrientGraph graph = new OrientGraph("remote:localhost/aaa","root","root");
        SailRepository sr = new SailRepository(new GraphSail(graph));
        sr.initialize();
        return sr;
    }


    public static SailRepository createTitanConnection(String ip, String keyspace) throws RepositoryException {
        String backend = "cassandra";

        Configuration conf = new BaseConfiguration();
        conf.setProperty("storage.backend", backend);
        conf.setProperty("storage.hostname", ip);
        conf.setProperty("storage.keyspace", keyspace);

        TitanGraph g = TitanFactory.open(conf);
        SailRepository sr = new SailRepository(new GraphSail(g));
        sr.initialize();

        return sr;
    }
}
