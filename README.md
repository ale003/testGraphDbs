testGraphDbs
============
Author: Alessandro Seganti (Data Engineer @Cognitum, Poland)
Version:0.0
First-created: 2014-04-10
Copyright(c): Cognitum, Poland 2014

Simple java benchmark to test blueprints sail graph.

At the moment the benchmark works for Titan, Orient and Neo4j.

The benchmark is generating an RDF graph of articles and authors with random relations between them. To have a faster data generation,
 at each step the data is written in the graphDb and then some sparql queries are executing against the data.

The data already generated is in TrialsData. In GraphDbsAll there is also data from other trials done in AllegroGraph and Virtuoso (not with this program).


// ============================================================================================================== 
// © 2014 Cognitum. All rights reserved.  
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance  
// with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 
// Unless required by applicable law or agreed to in writing, software distributed under the License is  
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  
// See the License for the specific language governing permissions and limitations under the License. 
// ============================================================================================================== 