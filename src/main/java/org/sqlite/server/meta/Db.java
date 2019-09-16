/**
 * Copyright 2019 little-pan. A SQLite server based on the C/S architecture.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.sqlite.server.meta;

/** The user privileges on DB.
 * 
 * @author little-pan
 * @since 2019-09-08
 *
 */
public class Db {
    
    private String host;
    private String user;
    private String db;
    
    // privileges
    private int allPriv;
    
    private int selectPriv;
    private int insertPriv; // includes replace
    private int updatePriv;
    private int deletePriv;
    
    private int createPriv;
    private int alterPriv;
    private int dropPriv;
    
    private int pragmaPriv;
    private int vacuumPriv;
    private int attachPriv; // include detach
    
    public Db() {
        
    }
    
    public Db(String user, String host, String db) {
        this.user = user;
        this.host = host;
        this.db = db;
    }
    
    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }
    
    /**
     * @return the allPriv
     */
    public int getAllPriv() {
        return allPriv;
    }

    /**
     * @param allPriv the allPriv to set
     */
    public void setAllPriv(int allPriv) {
        this.allPriv = allPriv;
    }

    /**
     * @return the selectPriv
     */
    public int getSelectPriv() {
        return selectPriv;
    }

    /**
     * @param selectPriv the selectPriv to set
     */
    public void setSelectPriv(int selectPriv) {
        this.selectPriv = selectPriv;
    }

    /**
     * @return the insertPriv
     */
    public int getInsertPriv() {
        return insertPriv;
    }

    /**
     * @param insertPriv the insertPriv to set
     */
    public void setInsertPriv(int insertPriv) {
        this.insertPriv = insertPriv;
    }

    /**
     * @return the updatePriv
     */
    public int getUpdatePriv() {
        return updatePriv;
    }

    /**
     * @param updatePriv the updatePriv to set
     */
    public void setUpdatePriv(int updatePriv) {
        this.updatePriv = updatePriv;
    }

    /**
     * @return the deletePriv
     */
    public int getDeletePriv() {
        return deletePriv;
    }

    /**
     * @param deletePriv the deletePriv to set
     */
    public void setDeletePriv(int deletePriv) {
        this.deletePriv = deletePriv;
    }

    /**
     * @return the createPriv
     */
    public int getCreatePriv() {
        return createPriv;
    }

    /**
     * @param createPriv the createPriv to set
     */
    public void setCreatePriv(int createPriv) {
        this.createPriv = createPriv;
    }

    /**
     * @return the alterPriv
     */
    public int getAlterPriv() {
        return alterPriv;
    }

    /**
     * @param alterPriv the alterPriv to set
     */
    public void setAlterPriv(int alterPriv) {
        this.alterPriv = alterPriv;
    }

    /**
     * @return the dropPriv
     */
    public int getDropPriv() {
        return dropPriv;
    }

    /**
     * @param dropPriv the dropPriv to set
     */
    public void setDropPriv(int dropPriv) {
        this.dropPriv = dropPriv;
    }

    /**
     * @return the pragmaPriv
     */
    public int getPragmaPriv() {
        return pragmaPriv;
    }

    /**
     * @param pragmaPriv the pragmaPriv to set
     */
    public void setPragmaPriv(int pragmaPriv) {
        this.pragmaPriv = pragmaPriv;
    }

    /**
     * @return the vacuumPriv
     */
    public int getVacuumPriv() {
        return vacuumPriv;
    }

    /**
     * @param vacuumPriv the vacuumPriv to set
     */
    public void setVacuumPriv(int vacuumPriv) {
        this.vacuumPriv = vacuumPriv;
    }
    
    /**
     * @return the attachPriv
     */
    public int getAttachPriv() {
        return attachPriv;
    }

    /**
     * @param attachPriv the attachPriv to set
     */
    public void setAttachPriv(int attachPriv) {
        this.attachPriv = attachPriv;
    }
    
    public boolean hasPriv(String command) {
        if (1 == getAllPriv()) {
            return true;
        }
        
        switch(command) {
        case "SELECT":
            return (1 == getSelectPriv());
        case "INSERT":
        case "REPLACE":
            return (1 == getInsertPriv());
        case "UPDATE":
            return (1 == getUpdatePriv());
        case "DELETE":
            return (1 == getDeletePriv());
        case "CREATE":
            return (1 == getCreatePriv());
        case "ALTER":
            return (1 == getAlterPriv());
        case "DROP":
            return (1 == getDropPriv());
        case "PRAGMA":
            return (1 == getPragmaPriv());
        case "VACUUM":
            return (1 == getVacuumPriv());
        case "ATTACH":
        case "DETACH":
            return (1 == getAttachPriv());
        default:
            return false;
        }
    }
    
}
