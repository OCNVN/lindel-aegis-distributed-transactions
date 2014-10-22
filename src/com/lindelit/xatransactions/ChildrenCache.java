/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lindelit.xatransactions;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author carloslucero
 */
public class ChildrenCache {
    protected List<String> children;
    
    public ChildrenCache() {
        this.children = null;        
    }
    
    public ChildrenCache(List<String> children) {
        this.children = children;        
    }
        
    public List<String> getList() {
        return children;
    }
        
    public List<String> addedAndSet( List<String> newChildren) {
        ArrayList<String> diff = null;
        
        if(children == null) {
            diff = new ArrayList<String>(newChildren);
        } else {
            for(String s: newChildren) {
                if(!children.contains( s )) {
                    if(diff == null) {
                        diff = new ArrayList<String>();
                    }
                
                    diff.add(s);
                }
            }
        }
        this.children = newChildren;
            
        return diff;
    }
        
    public List<String> removedAndSet( List<String> newChildren) {
        List<String> diff = null;
            
        if(children != null) {
            for(String s: children) {
                if(!newChildren.contains( s )) {
                    if(diff == null) {
                        diff = new ArrayList<String>();
                    }
                    
                    diff.add(s);
                }
            }
        }
        this.children = newChildren;
        
        return diff;
    }
    
    public List<String> onlyAdded( List<String> newChildren) {
        ArrayList<String> diff = null;
        
        if(children == null) {
            diff = new ArrayList<String>(newChildren);
        } else {
            for(String s: newChildren) {
                if(!children.contains( s )) {
                    if(diff == null) {
                        diff = new ArrayList<String>();
                    }
                
                    diff.add(s);
                }
            }
        }
            
        return diff;
    }
        
    public List<String> onlyRemoved( List<String> newChildren) {
        List<String> diff = new ArrayList<String>();
            
       if(children != null){
            for(String s: children) {
                if(!newChildren.contains( s )) {
                    diff.add(s);
                }
            }
       }
        
        
        return diff;
    }
    
    public List<String> onlyAdd( List<String> newChildren) {
        this.children = newChildren;
        
        return this.children;
    }
}
