/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package eu.cognitum.readandwrite;

/**
 *
 * @author Admin
 */
public class StdMeasures {
    
    public StdMeasures(double mean,double std,int nres){
        Mean= mean;
        Std = std;
        Nres = nres;
    }
    public double Mean;
    public double Std;
    public int Nres;
}
