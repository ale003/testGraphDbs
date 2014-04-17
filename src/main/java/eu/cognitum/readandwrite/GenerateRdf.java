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

import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.oupls.sail.GraphSail;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.time.StopWatch;
import org.openrdf.model.Model;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.Rio;
import org.openrdf.sail.Sail;
import org.openrdf.sail.memory.MemoryStore;

/**
 * Generate a random graph (using openrdf MemoryStore) and saves it as an RDF
 * file.
 *
 * @author Alessandro Seganti (Data Engineer @Cognitum)
 * @version 0.0
 * @since 2014-04-10
 * @copyright Cognitum, Poland 2014
 */
public class GenerateRdf implements RdfGenerator {

    protected String DEFAULT_NAMESPACE = "http://www.ontorion.com/testontology.owl#";
    protected String _currentNamespace = "http://www.ontorion.com/testontology.owl#";
    protected static String rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    protected static String rdfs = "http://www.w3.org/2000/01/rdf-schema#";
    protected static String xsd = "http://www.w3.org/2001/XMLSchema#";
    protected static String fn = "http://www.w3.org/2005/xpath-functions#";

    public ArrayList<String> artNames = new ArrayList<String>();
    public ArrayList<String> authorNames = new ArrayList<String>();
    public ArrayList<String> titles = new ArrayList<String>();
    public ArrayList<String> cities = new ArrayList<String>();

    public int Ntriples = 0;
    public int Narticles = 0;
    public String outputFileName = "";

    protected SailRepository sr;
    protected RepositoryConnection src;
    protected ValueFactory vf;

    protected Random rand;

    public GenerateRdf(String currentNameSpace, String outputFileName) throws RepositoryException {
        rand = new Random(System.currentTimeMillis());

        if (!outputFileName.contains(".rdf")) {
            outputFileName += ".rdf";
        }

        java.io.File outFile = new java.io.File("tmp/" + outputFileName.replace(".rdf", "")+ UUID.randomUUID());
        Sail sailInMemory = new MemoryStore(outFile);
        sr = new SailRepository(sailInMemory);
        sr.initialize();
        src = sr.getConnection();
        vf = sr.getValueFactory();

        try {
            currentNamespaceUri = vf.createURI(currentNameSpace);
            _currentNamespace = currentNameSpace;
        } catch (IllegalArgumentException ex) {
            Logger.getLogger(App.class.getName()).warning("The uri was invalid. Using the default one: " + DEFAULT_NAMESPACE);
            currentNamespaceUri = vf.createURI(DEFAULT_NAMESPACE);
            _currentNamespace = DEFAULT_NAMESPACE;
        }

        this.outputFileName = outputFileName;
    }

    public void CloseAllConnections() throws RepositoryException {
        src.close();
        sr.shutDown();
    }

    public org.openrdf.model.URI currentNamespaceUri = null;
    public double minPrice = 0;

    // test writing the informations on the db.
    public void generateAndSaveRdf(int Narticles) throws Exception {
        createNames(Narticles);

        CreateArticlesOntology(Narticles);

        SaveRdf();
    }

    // generate names of the articles, titles and authors
    public void createNames(int numberOfArticles) {
        for (int i = 0; i < numberOfArticles; i++) {
            artNames.add("article" + i);
            titles.add("title" + i);
            authorNames.add("author" + i);
        }
    }

    public String getRandomArticleName() throws Exception {
        return getRandomArticleName(Narticles);
    }
    
    public String getRandomArticleName(int limit) throws Exception {
        if (Narticles == 0) {
            throw new Exception("Cannot generate article name if no article has been generated.");
        }
        return "article" + rand.nextInt(limit);
    }

    public String getRandomTitle() throws Exception {
        return getRandomTitle(Narticles);
    }

    public String getRandomTitle(int limit) throws Exception {
        if (Narticles == 0) {
            throw new Exception("Cannot generate title name if no article has been generated.");
        }
        return "title" + rand.nextInt(limit);
    }

    private Map<Integer,Integer> articleTriples = new TreeMap<Integer, Integer>();
    
    // create relationship between articles and store the graph of articles,authors,titles,....
    private void CreateArticlesOntology(int numberOfArticles) throws Exception {

        Narticles = numberOfArticles;

        if (artNames.isEmpty()) {
            throw new Exception("Please generate names before writing the graph.");
        }

        double probabilityOfOtherArticleRelation = 0.15;

        Long acstart = System.currentTimeMillis();

        try {
            src.begin();

            for (int i = 0; i < numberOfArticles; i++) {
                src.add(vf.createURI(_currentNamespace, artNames.get(i)), vf.createURI(rdf, "instanceOf"), vf.createURI(_currentNamespace, "article"), currentNamespaceUri);

                src.add(vf.createURI(_currentNamespace, artNames.get(i)), vf.createURI(_currentNamespace, "title"), vf.createLiteral(titles.get(i)), currentNamespaceUri);
                src.add(vf.createURI(_currentNamespace, artNames.get(i)), vf.createURI(_currentNamespace, "ID"), vf.createLiteral(i), currentNamespaceUri);
                src.add(vf.createURI(_currentNamespace, artNames.get(i)), vf.createURI(_currentNamespace, "author"), vf.createURI(_currentNamespace, authorNames.get(rand.nextInt(authorNames.size()))), currentNamespaceUri);

                double nextPrice = rand.nextDouble() * 200;
                if (minPrice > nextPrice) {
                    minPrice = nextPrice;
                }
                src.add(vf.createURI(_currentNamespace, artNames.get(i)), vf.createURI(_currentNamespace, "price"), vf.createLiteral(nextPrice), currentNamespaceUri);
                src.add(vf.createURI(_currentNamespace, artNames.get(i)), vf.createURI(_currentNamespace, "hasDescription"), vf.createLiteral("This is article -" + artNames.get(i) + "-"), currentNamespaceUri);

                Ntriples += 6;

                int numOfRelated = rand.nextInt(50);
                for (int j = 0; j < numOfRelated; j++) {
                    if (rand.nextDouble() <= probabilityOfOtherArticleRelation) {
                        src.add(vf.createURI(_currentNamespace, artNames.get(i)), vf.createURI(_currentNamespace, "relateTo"), vf.createURI(_currentNamespace, getRandomArticleName()), currentNamespaceUri);
                        Ntriples++;
                    } else {
                        //src.add(vf.createURI(currentNamespace, artNames[i]), vf.createURI(currentNamespace, "relateTo"), vf.createURI(currentNamespace, rand.Next(100).ToString()));
                    }
                }
                articleTriples.put(i+1, Ntriples);
            }
        } finally {
            Long ts = System.currentTimeMillis() - acstart;
            //logInfo.println(String.format("Final committing after %d ms...",ts));

            src.commit();

            ts = System.currentTimeMillis() - acstart;
            //logInfo.println(String.format("Final committed after %d ms...", ts));
            //logInfo.flush();
        }
    }

    /**
     * Saves as RDF the graph created.
     *
     * @throws RepositoryException
     * @throws RDFHandlerException
     * @throws IOException
     */
    private void SaveRdf() throws RepositoryException, RDFHandlerException, IOException {
        RepositoryResult statements = src.getStatements(null, null, null, true);

        Model model = new LinkedHashModel();

        java.util.ArrayList arr = new java.util.ArrayList();
        while (statements.hasNext()) {
            arr.add(statements.next());
        }
        model.addAll(arr);

        model.setNamespace("rdf", RDF.NAMESPACE);
        model.setNamespace("rdfs", RDFS.NAMESPACE);
        model.setNamespace("xsd", XMLSchema.NAMESPACE);
        model.setNamespace("foaf", FOAF.NAMESPACE);
        model.setNamespace("ex", "");

        Rio.write(model, new java.io.FileWriter(new java.io.File(outputFileName),false), RDFFormat.TURTLE);
        
        CloseAllConnections();
    }

    public StdMeasures executeQueries(RepositoryConnection srcLoc,PrintWriter logInfo, int queryType, int Ntrials,int currentNtriples) {
        return executeQueries(srcLoc,logInfo, queryType, Ntrials, currentNtriples,false, "fileLog.txt");
    }

    /**
     * Reads the informations from the graph using sparql.... queryType decides
     * which kind of query we want to do.
     *
     * @param queryType The type of the query to execute
     * @param Ntrials The number of times a query should be executed (more
     * trials, more precision)
     * @return The mean time it took to execute the query
     *
     */
    public StdMeasures executeQueries(RepositoryConnection srcLoc,PrintWriter logInfo, int queryType, int Ntrials, int currentNtriples, boolean printlnAllLogs, String fileLog) {
        long functionTime = System.currentTimeMillis();

        int currentNArticles = GetNElementGivenTriples(currentNtriples);
        
        try {
            Queries.setPrefixes(_currentNamespace);

            String queryString = null;
            if (queryType == 0) {
                queryString = Queries.getQuery0();
            } else if (queryType == 1) {
                queryString = Queries.getQuery1(rand.nextInt((int) (articleTriples.get(currentNArticles) / 4)));
            } else if (queryType == 2) {
                String articleVar = ":" + getRandomArticleName(currentNArticles);
                queryString = Queries.getQuery2(articleVar);
            } else if (queryType == 3) {
                String articleVar = ":" + getRandomArticleName(currentNArticles);
                queryString = Queries.getQuery3(articleVar);
            } else if (queryType == 4) {
                String titleVar = "\"" + getRandomTitle(currentNArticles) + "\"";
                queryString = Queries.getQuery4(titleVar);
            } else if (queryType == 5) {
                String articleVar = getRandomArticleName(currentNArticles);
                queryString = Queries.getQuery5(articleVar);
            } else if (queryType == 6) {
                queryString = Queries.getQuery6(minPrice);
            } else if (queryType == 7) {
                queryString = Queries.getQuery7(minPrice);
            }

            logInfo.println("You chose to do " + Ntrials + " trials and queryType " + queryType + "\n");
            logInfo.println("I will now execute the SPARQL query: \n" + queryString + "\n");
            logInfo.flush();

            double totalTime = 0;
            String res = null;

            PrintWriter swr = null;
            if (printlnAllLogs) {
                swr = new PrintWriter(fileLog, "UTF-8");
                swr.println("Parameters," + Ntrials + ",QueryType," + queryType + "\n");
                swr.println("QueryTrial,Time(ms)\n");
            }

            ArrayList<Long> allTimes = new ArrayList<Long>();

            int Nres = 0;
            for (int i = 0; i < Ntrials; i++) {
                long timing = System.currentTimeMillis();
                TupleQuery tQuery = srcLoc.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
                TupleQueryResult result = tQuery.evaluate();
                Nres = 0;
                while (result.hasNext()) {
                    //res += result.next() + "\n";
                    result.next();
                    Nres++;
                }

                long tmp = System.currentTimeMillis() - timing ;
                allTimes.add(tmp);
                totalTime += tmp;

                if (printlnAllLogs) {
                    swr.println(String.format("%d,%.2f", i, tmp));
                }
            }

            double std = 0;
            double mean = totalTime / Ntrials;
            for (int i = 0; i < allTimes.size(); i++) {
                Long tm = allTimes.get(i);
                std += (tm - mean) * (tm - mean);
            }
            std = Math.sqrt(std / Ntrials);

            //logInfo.println(res);
            logInfo.println("Total Time TRIAL: " + mean + " +- " + std);

            if (printlnAllLogs) {
                swr.println(String.format("Total Time,%.2f,%.2f,%d", mean, std, Nres));
                swr.flush();
                swr.close();
            }

            logInfo.println("Full function execution time: " + (System.currentTimeMillis() - functionTime));
            logInfo.flush();
            return new StdMeasures(mean, std, Nres);
        } catch (Exception ex) {
            logInfo.println("################ Something went wrong while executing query " + queryType + "\n");
            logInfo.println(ex.getMessage());
            return new StdMeasures(0, 0, 0);
        }
    }

    private int GetNElementGivenTriples(int NtriplesAct){
        for (Map.Entry<Integer,Integer>  artTrip : articleTriples.entrySet()) {
            if(artTrip.getValue() > NtriplesAct || artTrip.getValue() == NtriplesAct)
                return artTrip.getKey();
        }
        return Narticles;
    }
    
    public int getNtriples() {
        return Ntriples;
    }

    public String getFileName() {
        return outputFileName;
    }

    public int getElements() {
        return Narticles;
    }

}
