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
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import recipes_service.data.Operation;
//LSim logging system imports sgeag@2017
//import lsim.coordinator.LSimCoordinator;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.library.api.LSimLogger;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 *
 */
public class Log implements Serializable{
	// Only for the zip file with the correct solution of phase1.Needed for the logging system for the phase1. sgeag_2018p 
//	private transient LSimCoordinator lsim = LSimFactory.getCoordinatorInstance();
	// Needed for the logging system sgeag@2017
//	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();

	private static final long serialVersionUID = -4864990265268259700L;
	/**
	 * This class implements a log, that stores the operations
	 * received  by a client.
	 * They are stored in a ConcurrentHashMap (a hash table),
	 * that stores a list of operations for each member of 
	 * the group.
	 */
	private ConcurrentHashMap<String, List<Operation>> log= new ConcurrentHashMap<String, List<Operation>>();  

	public Log(List<String> participants){
		// create an empty log
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			log.put(it.next(), new Vector<Operation>());
		}
	}

	/**
	 * inserts an operation into the log. Operations are 
	 * inserted in order. If the last operation for 
	 * the user is not the previous operation than the one 
	 * being inserted, the insertion will fail.
	 * 
	 * @param op
	 * @return true if op is inserted, false otherwise.
	 */
public synchronized boolean add(Operation op){
		
		String hostId = op.getTimestamp().getHostid();
		/* Create a list with the timestamps and its hostid*/
		List <Operation>listOfOperations = log.get(hostId);
		/* Boolean that check that the list is empty or not*/
		Boolean state = listOfOperations.isEmpty();
		/*Check if empty or the value from the timestamp is newer or not to add it or not */
		if(state || listOfOperations.get(listOfOperations.size()-1).getTimestamp().compare(op.getTimestamp())<0) {
			return listOfOperations.add(op);
		}
		
		return false;
	}
	
	/**
	 * Checks the received summary (sum) and determines the operations
	 * contained in the log that have not been seen by
	 * the proprietary of the summary.
	 * Returns them in an ordered list.
	 * @param sum
	 * @return list of operations
	 */
public List<Operation> listNewer(TimestampVector sum){
	
	List<Operation> missingList = new Vector<Operation>();
	
	//get keys from hosts
	
	for(String node:log.keySet()) {
		
		Timestamp tRef = sum.getLast(node);
		
		List<Operation> logOperation = log.get(node);
		
		for (int j = 0; j < logOperation.size();j++) {
			if(logOperation.get(j).getTimestamp().compare(tRef)>0) {
				missingList.add(logOperation.get(j));
			}
		}
	}
	
	return missingList;
}
	
	/**
	 * Removes from the log the operations that have
	 * been acknowledged by all the members
	 * of the group, according to the provided
	 * ackSummary. 
	 * @param ack: ackSummary.
	 */
public void purgeLog(TimestampMatrix ack){  //FASE 3//
	
	//Create a list with the keys from logs and save the smallest timestamp
	String auxKey;
	List<String> keyList = new Vector<String>(this.log.keySet());
	TimestampVector timestampVectorMin = ack.minTimestampVector();
	
	
	//iterator that check the keys from a hashmap
	for(Iterator<String> key = keyList.iterator(); key.hasNext();) {
	
		//Save the next key
		auxKey = key.next();
		
		//iterator that check the operations from the list of the key 
		for (Iterator<Operation> ops = log.get(auxKey).iterator();ops.hasNext();) {
			//Check timestamp not null and higher than next one
			if(!(timestampVectorMin.getLast(auxKey)== null) && !(ops.next().getTimestamp().compare(timestampVectorMin.getLast(auxKey))>0)) {
				ops.remove();
			}
		}
	}
}

	/**
	 * equals
	 */
	@Override
	public synchronized boolean equals(Object obj) {
		if (obj != null && obj instanceof Log) {
			
			Log objLog = (Log) obj;
			return log.equals(objLog.log);
		}
		
		return false;
	}

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String name="";
		for(Enumeration<List<Operation>> en=log.elements();
		en.hasMoreElements(); ){
		List<Operation> sublog=en.nextElement();
		for(ListIterator<Operation> en2=sublog.listIterator(); en2.hasNext();){
			name+=en2.next().toString()+"\n";
		}
	}
		
		return name;
	}
}
