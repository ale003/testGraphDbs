// ============================================================================================================== 
// � 2014 Cognitum. All rights reserved.  
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance  
// with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 
// Unless required by applicable law or agreed to in writing, software distributed under the License is  
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  
// See the License for the specific language governing permissions and limitations under the License. 
// ============================================================================================================== 
package eu.cognitum.readandwrite;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.Repository;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.helpers.RDFHandlerBase;

/**
 *
 * @author Admin
 */
public class RdfReadHandler extends RDFHandlerBase {

    public int Ntriples = 0;
    public int NtriplesCompleted=0;

    protected int _readStep;
    protected int _commitBufferSize = 10;
    private int[] QUERYINDEXES = new int[]{0, 1, 2, 3, 4, 5, 6, 7};
    protected int Ntrials = 10;

    protected boolean _stop = false;
    protected PrintWriter _swrWrites = null;
    protected PrintWriter _swrReads = null;
    protected PrintWriter _logInfo = null;

    protected Repository sr;
    protected RepositoryConnection src;
    protected ValueFactory vf;

    protected Random rand;

    protected String _currentNamespace = "http://www.ontorion.com/testontology.owl#";
    public org.openrdf.model.URI currentNamespaceUri = null;
    Long writeStartTime;
    long lastCommitTime = 0;
    protected RdfGenerator _rdfGen;

    public RdfReadHandler(SimulateReadAndWrite simRaW, RdfGenerator rdfGen, String currentNameSpace) throws RepositoryException, FileNotFoundException {

        _rdfGen = rdfGen;

        _readStep = simRaW.GetReadStep();
        _commitBufferSize = simRaW.GetCommitBufferSize();

        if (currentNameSpace != null) {
            _currentNamespace = currentNameSpace;
        }
        sr = simRaW.rep;
        src = simRaW.rep.getConnection();
        vf = simRaW.rep.getValueFactory();

        currentNamespaceUri = vf.createURI(_currentNamespace);

        _swrWrites = new PrintWriter(simRaW.outputName + "AllWrites.csv");
        _swrWrites.write("Ntriples,CommitTime (ms)\n");

        _swrReads = new PrintWriter(simRaW.outputName + "AllReads.csv");

        String line = "Ntriples,Ntrials";

        for (int i = 0; i < QUERYINDEXES.length; i++) {
            int qu = QUERYINDEXES[i];
            line += ",Mean Query" + qu + "(ms), Std Query" + qu + "(ms), Nres Query" + qu;
        }
        _swrReads.println(line);

        _logInfo = new PrintWriter(simRaW.outputName + "LogInfo.txt");
    }

    public void readAll(String inputFileName) throws IOException, RDFParseException, RDFHandlerException {
        File inFile = new File(inputFileName);
        ParserConfig config = new ParserConfig();
        org.openrdf.repository.util.RDFLoader loader = new org.openrdf.repository.util.RDFLoader(config, vf);
        loader.load(inFile, _currentNamespace, RDFFormat.TURTLE, this);
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        Ntriples = 0;
        try {
            src.begin();
        } catch (RepositoryException ex) {
            throw new RDFHandlerException(ex);
        }
        writeStartTime = System.currentTimeMillis();
    }

    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        try {
            boolean wasCommitted=false;
            
            src.add(st, currentNamespaceUri);

            Ntriples++;

            if (_stop) {
                Logger.getLogger(RdfReadHandler.class.getName()).log(Level.WARNING, "_stop was TRUE, stopping process.");
                throw new RDFHandlerException("Reading RDF stopped from outside.");
            }
            
            // check if it is time to commit
            if (Ntriples % _commitBufferSize == 0 && Ntriples > 0) {
                src.commit();
                Long ts = System.currentTimeMillis() - writeStartTime;
                _swrWrites.println(String.format("%d,%d", Ntriples, ts + lastCommitTime));
                writeStartTime = System.currentTimeMillis();
                lastCommitTime += writeStartTime;
                wasCommitted=true;
            }

            //check if it is time to read
            if (Ntriples % _readStep == 0 && Ntriples > 0) {
                /*if (src.isActive()) { // should I commit before making queries?
                    src.commit();
                    wasCommitted=true;
                }*/
                Long readStartTime= System.currentTimeMillis();
                executeQueries();
                
                writeStartTime -= System.currentTimeMillis()-readStartTime; // substract to the write time the time it took to make the queries.

                NtriplesCompleted=Ntriples; // when finished reading, we have finished Ntriples...
            }
            
            
            if(wasCommitted)
                src.begin();

        } catch (RepositoryException ex) {
            Logger.getLogger(RdfReadHandler.class.getName()).log(Level.SEVERE, null, ex);
            throw new RDFHandlerException(ex);
        }
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        try {
            if(src.isActive())
                src.commit();
            src.close();
            sr.shutDown();
        } catch (RepositoryException ex) {
            Logger.getLogger(RdfReadHandler.class.getName()).log(Level.SEVERE, null, ex);
            throw new RDFHandlerException(ex);
        }
    }

    private void executeQueries() {
        ArrayList<StdMeasures> measures = new ArrayList<StdMeasures>();

        for (int i = 0; i < QUERYINDEXES.length; i++) {
            measures.add(_rdfGen.executeQueries(_logInfo, QUERYINDEXES[i], Ntrials,Ntriples));
        }

        // write log
        String outp = Ntriples + "," + Ntrials;
        for (int i = 0; i < measures.size(); i++) {
            StdMeasures stdMe = measures.get(i);
            outp += String.format(",%.2f,%.2f,%d", stdMe.Mean, stdMe.Std, stdMe.Nres);
        }
        _swrReads.println(outp);
    }
}
