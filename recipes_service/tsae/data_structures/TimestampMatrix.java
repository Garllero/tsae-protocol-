/*
* Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
* DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
*
* This file is part of the practical assignment of Distributed Systems course.
*
* This code is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This code is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this code.  If not, see <http://www.gnu.org/licenses/>.
*/

package recipes_service.tsae.data_structures;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 *
 */
public class TimestampMatrix implements Serializable{
	
	private static final long serialVersionUID = 3331148113387926667L;
	ConcurrentHashMap<String, TimestampVector> timestampMatrix = new ConcurrentHashMap<String, TimestampVector>();
	
	public TimestampMatrix(List<String> participants){
		// create and empty TimestampMatrix
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			timestampMatrix.put(it.next(), new TimestampVector(participants));
		}
	}
	//Constructor with parameters so it can get inicializated with timestampMatrix
	public TimestampMatrix(ConcurrentHashMap<String, TimestampVector>timestampMatrix) {
		this.timestampMatrix = new ConcurrentHashMap<String, TimestampVector>(timestampMatrix);
	}
	/**
	 * @param node
	 * @return the timestamp vector of node in this timestamp matrix
	 */
	TimestampVector getTimestampVector(String node){ //FASE 3//
		return timestampMatrix.get(node);
	}
	
	/**
	 * Merges two timestamp matrix taking the elementwise maximum
	 * @param tsMatrix
	 */
	public synchronized void updateMax(TimestampMatrix tsMatrix){  //FASE 4//
		
		TimestampVector valueTs;
		String key;
		//iterate each key from matrix tsMatrix
		for(Map.Entry<String, TimestampVector> tsKey:tsMatrix.timestampMatrix.entrySet()){
			//Save the values in the variables 
			key = tsKey.getKey();
			valueTs = tsKey.getValue();
			
			//Check that the actual timestamp != null and refresh it
			TimestampVector thisValueTs = this.timestampMatrix.get(key);
			if(thisValueTs != null) {
				thisValueTs.updateMax(valueTs);
			}
		}
	}
	
	/**
	 * substitutes current timestamp vector of node for tsVector
	 * @param node
	 * @param tsVector
	 */
	public synchronized void update(String node, TimestampVector tsVector){ //FASE 3//
		
		//check that the node doesnt exit and add the timestampVector to tsVector
		if (timestampMatrix.get(node)==null) {
			timestampMatrix.put(node, tsVector);
		//If the node exist then its replaced
		}else {
			timestampMatrix.replace(node,tsVector);
			
		}
	}
	
	/**
	 * 
	 * @return a timestamp vector containing, for each node, 
	 * the timestamp known by all participants
	 */
	public TimestampVector minTimestampVector(){  //FASE 3//
		
		TimestampVector tsVaux = null;
		
		//Iterate timestampMatrix and save a copy from each timestampVector and refresh the timestamp with the samllest from the list.
		for (TimestampVector timestampVector:this.timestampMatrix.values()) {
			if (tsVaux == null) {
				tsVaux = timestampVector.clone();
			}else {
				tsVaux.mergeMin(timestampVector);
			}
		} 
		return tsVaux;
	}
	
	/**
	 * clone
	 */
	public synchronized TimestampMatrix clone(){  //FASE 3//
		//use of constructor
		return (new TimestampMatrix(timestampMatrix));
	}
	
	/**
	 * equals
	 */
	@Override
	public boolean equals(Object obj) { //FASE 3//

		if(obj==null) {
			return false;
			
		}else if (this == obj) {
			return true;
			
		}else if (obj instanceof TimestampMatrix){
			TimestampMatrix other = (TimestampMatrix)obj;
			
			for (String name:this.timestampMatrix.keySet()) {
				return this.timestampMatrix.get(name).equals(other.timestampMatrix.get(name));
			}
		}
		return false;
	}

	
	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String all="";
		if(timestampMatrix==null){
			return all;
		}
		for(Enumeration<String> en=timestampMatrix.keys(); en.hasMoreElements();){
			String name=en.nextElement();
			if(timestampMatrix.get(name)!=null)
				all+=name+":   "+timestampMatrix.get(name)+"\n";
		}
		return all;
	}
}
