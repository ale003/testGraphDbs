// ============================================================================================================== 
// © 2014 Cognitum. All rights reserved.  
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance  
// with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 
// Unless required by applicable law or agreed to in writing, software distributed under the License is  
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  
// See the License for the specific language governing permissions and limitations under the License. 
// ============================================================================================================== 

package eu.cognitum.readandwrite;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.oupls.sail.GraphSail;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.nativerdf.NativeStore;
import sun.security.provider.certpath.Vertex;

/**
 * @author Alessandro Seganti (Data Engineer @Cognitum)
 * @version 0.0
 * @copyright Cognitum, Poland 2014
 * @since 2014-04-10
 */
public class App {
    private static final Logger LOGGER = Logger.getLogger(App.class.getName());

    public static enum DBS {
        NATIVE,
        TITAN,
        NEO4J,
        ORIENT
    }

    private static final String
            PROP_STORAGE_DIRECTORY = "storage.directory",
            PROP_STORAGE_HOSTNAME = "storage.hostname",
            PROP_STORAGE_KEYSPACE = "storage.keyspace";

    private static Properties CONFIGURATION;

    public static Properties getConfiguration() {
        return CONFIGURATION;
    }

    public static void main(String[] args) {
        try {
            String configFile = 0 == args.length
                    ? "example.properties"
                    : args[0];

            CONFIGURATION = new Properties();
            File f = new File(configFile);
            if (!f.exists()) {
                LOGGER.warning("configuration not found at " + configFile);
                return;
            }
            LOGGER.info("loading configuration file " + f.getAbsoluteFile());
            CONFIGURATION.load(new FileInputStream(f));

            String ip = CONFIGURATION.getProperty(PROP_STORAGE_HOSTNAME);
            String keyspace = CONFIGURATION.getProperty(PROP_STORAGE_KEYSPACE);
            String directory = CONFIGURATION.getProperty(PROP_STORAGE_DIRECTORY);

            // N of articles to be generated.
            int Narticles = 1000;
            // size of the buffer to commit each time
            int commitBufferSize = 10;
            // N of articles to commit before trying reads
            int readStep = 10;

            ArrayList<SimulateReadAndWrite> simulateAll = new ArrayList<SimulateReadAndWrite>();

            int Ndbs = 0;

            for (DBS dbs : DBS.values()) {
                SailRepository sr;

                switch (dbs) {
                    case NATIVE:
                        sr = createNativeStoreConnection(directory);
                        break;
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
                        System.out.println(String.format("Process for db %s is at %.2f", dbs.toString(), simulateAll.get(k).GetProgress()));
                    }
                    k++;
                }
                Thread.sleep(10000);
            }

        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        }
    }

    public static SailRepository createNativeStoreConnection(String directory) throws RepositoryException {
        File f = new File(directory);
        if (f.exists()) {
            f.delete();
        }

        NativeStore sail = new NativeStore(f);
        SailRepository sr = new SailRepository(sail);
        sr.initialize();
        return sr;
    }

    public static SailRepository createNeo4jConnection(String keyspace) throws RepositoryException {
        String path = "tmp/neo4j/" + keyspace;
        File f = new File(path);
        if (f.exists()) {
            f.delete();
        }

        Neo4jGraph graph = new Neo4jGraph(path);
        SailRepository sr = new SailRepository(new GraphSail(graph));
        sr.initialize();
        return sr;
    }

    public static SailRepository createOrientConnection(String keyspace) throws RepositoryException {
        String path = "tmp/orient/" + keyspace;
        File f = new File(path);
        if (f.exists()) {
            f.delete();
        }

        OrientGraph graph = new OrientGraph("local:" + path);
        //OrientGraph graph = new OrientGraph("remote:localhost/aaa","root","root");
        SailRepository sr = new SailRepository(new GraphSail(graph));
        sr.initialize();
        return sr;
    }

    public static SailRepository createTitanConnection(String ip, String keyspace) throws RepositoryException {
        // note: delete Titan Cassandra's keyspace manually

        String backend = "cassandra";

        Configuration conf = new BaseConfiguration();
        conf.setProperty("storage.backend", backend);
        conf.setProperty("storage.hostname", ip);
        conf.setProperty("storage.keyspace", keyspace);
        conf.setProperty("cache.db-cache", "true");

        TitanGraph g = TitanFactory.open(conf);

        String indexedPatterns = "p,c,pc";

        if (null == g.getType(GraphSail.INFERRED)) {
            g.makeKey(GraphSail.INFERRED).dataType(Boolean.class);
        }
        if (null == g.getType(GraphSail.KIND)) {
            g.makeKey(GraphSail.KIND).dataType(String.class);
        }
        if (null == g.getType(GraphSail.LANG)) {
            g.makeKey(GraphSail.LANG).dataType(String.class);
        }
        if (null == g.getType(GraphSail.TYPE)) {
            g.makeKey(GraphSail.TYPE).dataType(String.class);
        }
        if (null == g.getType(GraphSail.VALUE)) {
            g.makeKey(GraphSail.VALUE).dataType(String.class).unique();
        }
        for (String pattern : indexedPatterns.split(",")) {
            if (null == g.getType(pattern)) {
                g.makeKey(pattern).dataType(String.class);
            }
        }

        SailRepository sr = new SailRepository(new GraphSail(g, indexedPatterns));
        sr.initialize();

        return sr;
    }
}
