/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.mycompany.readandwrite;

/**
 *
 * @author Admin
 */
public class Queries {

    static String prefixes = "";

    public static void setPrefixes(String currentNamespace) {
        prefixes = "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n"
                + "PREFIX fn: <http://www.w3.org/2005/xpath-functions#>\n"
                + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"
                + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
                + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                + "PREFIX : <" + currentNamespace + ">\n";
    }

    public static String getPrefixes(String currentNamespace) {
        setPrefixes(currentNamespace);
        return prefixes;

    }

    public static String getQuery0() {
        return prefixes + "SELECT ?article ?title ?articleII\n"
                + " WHERE{ "
                + "?article :title ?title."
                + "?article :relateTo ?articleII\n"
                + "}";
    }

    public static String getQuery1(int offset) {
        return prefixes + "SELECT ?article ?title ?articleII\n"
                + " WHERE{ "
                + "?article :title ?title."
                + "?article :relateTo ?articleII\n"
                + "} LIMIT 100\n OFFSET " + offset;
    }

    public static String getQuery2(String articleVar) {
        return prefixes + "SELECT ?title ?ID ?author ?price\n"
                + " WHERE{\n"
                + articleVar + " :title ?title.\n"
                + articleVar + " :ID ?ID.\n"
                + articleVar + " :author ?author.\n"
                + articleVar + " :price ?price}\n";
    }

    public static String getQuery3(String articleVar) {
        return prefixes + "SELECT ?title ?ID ?author ?price\n"
                + " WHERE{\n"
                + articleVar + " :title ?title.\n"
                + "OPTIONAL\n{"
                + articleVar + " :ID ?ID.\n"
                + articleVar + " :author ?author.\n"
                + articleVar + " :price ?price}}\n";
    }

    public static String getQuery4(String titleVar) {
        return prefixes + "SELECT ?author ?ID\n"
                + " WHERE{\n"
                + "?article :title " + titleVar + ".\n"
                + "?article :ID ?ID"
                + "}\n";
    }

    public static String getQuery5(String articleVar) {
        return prefixes + "SELECT ?article \n"
                + "WHERE{\n"
                + "?article :hasDescription ?artDescr.\n"
                + "FILTER regex(str(?artDescr),\"-" + articleVar + "-\")"
                + "}\n";
    }

    public static String getQuery6(double minPrice) {
        return prefixes + "SELECT ?article \n"
                + "WHERE{\n"
                + "?article :price ?price.\n"
                + "FILTER(?price < " + (minPrice + 1) + ")"
                + "} ORDER BY DESC(?price) LIMIT 10\n";
    }

    public static String getQuery7(double minPrice) {
        return prefixes + "SELECT DISTINCT ?price \n"
                + "WHERE{\n"
                + "?article :price ?price.\n"
                + "FILTER(?price < " + (minPrice + 1) + ")"
                + "} LIMIT 10\n";
    }
}
