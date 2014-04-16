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

import java.io.PrintWriter;

/**
 *
 * @author Admin
 */
public interface RdfGenerator {
    
    public int getNtriples();
    
    public String getFileName();
    
    public int getElements();
    
    /**
     * Generates Nelements and Saves it in an RDF file.
     * @param Nelements Number of elements to be generated.
     * @throws Exception 
     */
    public void generateAndSaveRdf(int Nelements) throws Exception;
    
    public StdMeasures executeQueries(PrintWriter logInfo, int queryType, int Ntrials,int currentNtriples);
    
    public StdMeasures executeQueries(PrintWriter logInfo, int queryType, int Ntrials,int currentNtriples, boolean printlnAllLogs, String fileLog);
    
}
