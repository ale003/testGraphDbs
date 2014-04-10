/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.readandwrite;

import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.oupls.sail.GraphSail;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.apache.commons.lang.time.StopWatch;
import org.openrdf.repository.sail.SailRepository;

/**
 *
 * author Admin
 */
public class GraphDbTesting {

    protected String _currentNamespace = "http://www.ontorion.com/testontology.owl#";
    protected static String rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    protected static String rdfs = "http://www.w3.org/2000/01/rdf-schema#";
    protected static String xsd = "http://www.w3.org/2001/XMLSchema#";
    protected static String fn = "http://www.w3.org/2005/xpath-functions#";
    protected static Date dtstart;

    public ArrayList<String> artNames = new ArrayList<String>();
    public ArrayList<String> authorNames = new ArrayList<String>();
    public ArrayList<String> titles = new ArrayList<String>();
    public ArrayList<String> cities = new ArrayList<String>();

    public int Ntriples = 0;
    public int Narticles = 0;

    protected SailRepository sr;
    protected RepositoryConnection src;
    protected ValueFactory vf;

    protected Random rand;

    public int commitBufferSize = 10;

    public GraphDbTesting(SailRepository repExt, String currentNameSpace) throws RepositoryException {
        rand = new Random(System.currentTimeMillis());

        if (currentNameSpace != null) {
            _currentNamespace = currentNameSpace;
        }
        sr = repExt;
        src = repExt.getConnection();
        vf = repExt.getValueFactory();

        currentNamespaceUri = vf.createURI(_currentNamespace);
    }

    public void CloseAllConnections() throws RepositoryException {
        src.close();
        KeyIndexableGraph graph = ((GraphSail) sr.getSail()).getBaseGraph();
        graph.shutdown();
        sr.shutDown();
    }

        /// <summary>
    /// Reads the informations from the graph using sparql.... queryType decides which kind of query we want to do.
    /// </summary>
    /// <param name="queryType">The type of the query to execute</param>
    /// <param name="Ntrials">The number of times a query should be executed (more trials, more precision)</param>
    /// <returns>The mean time it took to execute the query</returns>
    public StdMeasures testRead(PrintWriter logInfo, int queryType,int Ntrials, boolean printlnAllLogs, String fileLog)
        {
            StopWatch functionTime = new StopWatch();
            functionTime.start();
            
        try {
            Queries.setPrefixes(_currentNamespace);

            String queryString = null;
            if (queryType == 0) {
                queryString = Queries.getQuery0();
            } else if (queryType == 1) {
                queryString = Queries.getQuery1(rand.nextInt((int) (Ntriples / 4)));
            } else if (queryType == 2) {
                String articleVar = ":" + getRandomArticleName();
                queryString = Queries.getQuery2(articleVar);
            } else if (queryType == 3) {
                String articleVar = ":" + getRandomArticleName();
                queryString = Queries.getQuery3(articleVar);
            } else if (queryType == 4) {
                String titleVar = "\"" + getRandomTitle() + "\"";
                queryString = Queries.getQuery4(titleVar);
            } else if (queryType == 5) {
                String articleVar = getRandomArticleName();
                queryString = Queries.getQuery5(articleVar);
            } else if (queryType == 6) {
                queryString = Queries.getQuery6(minPrice);
            } else if (queryType == 7) {
                queryString = Queries.getQuery7(minPrice);
            }

            logInfo.println("You chose to do " + Ntrials + " trials and queryType " + queryType+"\n");
            logInfo.println("I will now execute the SPARQL query: \n" + queryString+"\n");
            logInfo.flush();

            double totalTime = 0;
            String res = null;

            PrintWriter swr = null;
            if (printlnAllLogs) {
                swr = new PrintWriter(fileLog, "UTF-8");
                swr.println("Parameters," + Ntrials + ",QueryType," + queryType+"\n");
                swr.println("QueryTrial,Time(ms)\n");
            }

            ArrayList<Long> allTimes = new ArrayList<Long>();

            int Nres = 0;
            for (int i = 0; i < Ntrials; i++) {
                StopWatch timing = new StopWatch();
                timing.start();
                TupleQuery tQuery = src.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
                TupleQueryResult result = tQuery.evaluate();
                Nres = 0;
                while (result.hasNext()) {
                    //res += result.next() + "\n";
                    result.next();
                    Nres++;
                }

                timing.stop();
                Long tmp = timing.getTime();
                allTimes.add(tmp);
                totalTime += tmp;

                if (printlnAllLogs) {
                    swr.println(String.format("%d,%.2f", i, tmp));
                }
            }

            double std = 0;
            double mean = totalTime / Ntrials;
            for(int i=0;i < allTimes.size();i++){
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

            logInfo.println("Full function execution time: " + functionTime.getTime());
            logInfo.flush();
            return new StdMeasures(mean,std,Nres);
        } catch (Exception ex) {
            logInfo.println("################ Something went wrong while executing query " + queryType+"\n");
            logInfo.println(ex.getMessage());
            return new StdMeasures(0,0,0);
        }
    }

    public org.openrdf.model.URI currentNamespaceUri = null;
    public double minPrice = 0;

    // test writing the informations on the db.
    public Map<Integer, Long> testWrite(PrintWriter logInfo, int Narticles, int offset) throws Exception
        {
            createNames(Narticles, offset);

            Map<Integer, Long> tmp = CreateArticlesOntology(logInfo, Narticles, offset);
            artNames.clear();
            titles.clear();

            return tmp;
    }

    // generate names of the articles, titles and authors
    public void createNames(int numberOfArticles, int offset) {
        for (int i = offset; i < numberOfArticles; i++) {
            artNames.add("article" + i);
            titles.add("title" + i);
            authorNames.add("author" + i);
        }
    }

    public String getRandomArticleName() throws Exception {
        if (Narticles == 0) {
            throw new Exception("Cannot generate article name if no article has been generated.");
        }
        return "article" + rand.nextInt(Narticles);
    }

    public String getRandomTitle() throws Exception {
        if (Narticles == 0) {
            throw new Exception("Cannot generate title name if no article has been generated.");
        }
        return "title" + rand.nextInt(Narticles);
    }

    // create relationship between articles and store the graph of articles,authors,titles,....
    private Map<Integer,Long> CreateArticlesOntology(PrintWriter logInfo, int numberOfArticles,int offset) throws Exception
        {

            Narticles = numberOfArticles;

        if (artNames.isEmpty()) {
            throw new Exception("Please generate names before writing the graph.");
        }

        Map<Integer,Long> commitTime = new HashMap<Integer, Long>();

        double probabilityOfOtherArticleRelation = 0.15;

        Long acstart = System.currentTimeMillis();

        try {
            src.begin();

            for (int i = 0; i < numberOfArticles - offset; i++) {
                src.add(vf.createURI(_currentNamespace, artNames.get(i)), vf.createURI(rdf, "instanceOf"), vf.createURI(_currentNamespace, "article"), currentNamespaceUri);

                src.add(vf.createURI(_currentNamespace, artNames.get(i)), vf.createURI(_currentNamespace, "title"), vf.createLiteral(titles.get(i)), currentNamespaceUri);
                src.add(vf.createURI(_currentNamespace, artNames.get(i)), vf.createURI(_currentNamespace, "ID"), vf.createLiteral(i + offset), currentNamespaceUri);
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
                if (i % commitBufferSize == 0 && i > 0) {
                    Long ts = System.currentTimeMillis() - acstart;

                    //logInfo.println("[{0}, {1}] {2} articles, {3} triples, committing after {4:F2} ms...", dtstart.ToLongDateString(), dtstart.ToLongTimeString(), i, Ntriples, ts.TotalMilliseconds);logInfo.println("[{0}, {1}] {2} articles, {3} triples, committing after {4:F2} ms...", dtstart.ToLongDateString(), dtstart.ToLongTimeString(), i, Ntriples, ts.TotalMilliseconds);z
                    logInfo.println(String.format("%d articles, %d triples, committing after %d ms...", i, Ntriples, ts));

                    src.commit();

                    ts = System.currentTimeMillis() - acstart;
                    commitTime.put(Ntriples, ts);

                    //logInfo.println("[{0}, {1}] {2} articles, {3} triples committed after {4:F2} ms.", dtstart.ToLongDateString(), dtstart.ToLongTimeString(), i, Ntriples, ts.TotalMilliseconds);
                    logInfo.println(String.format("%d articles, %d triples, committed after %d ms...", i, Ntriples, ts));
                    logInfo.flush();
                    src.begin();
                }
            }
        } catch (Exception ex) {
            Logger.getLogger(GraphDbTesting.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            Long ts = System.currentTimeMillis() - acstart;
            //logInfo.println("[{0}, {1}] Final commiting after {2} ms...", dtstart.ToLongDateString(), dtstart.ToLongTimeString(), ts.TotalMilliseconds);
            logInfo.println(String.format("Final committing after %d ms...",ts));
            
            src.commit();

            ts = System.currentTimeMillis() - acstart;
//            logInfo.println("[{0}, {1}] Final commited after {2} ms.", dtstart.ToLongDateString(), dtstart.ToLongTimeString(), ts.TotalMilliseconds);
            logInfo.println(String.format("Final committed after %d ms...", ts));
            logInfo.flush();
            
            if (!commitTime.containsKey(Ntriples)) {
                commitTime.put(Ntriples, ts);
            }
        }

        return commitTime;
    }
}
