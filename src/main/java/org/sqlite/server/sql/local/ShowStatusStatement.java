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
package org.sqlite.server.sql.local;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.sqlite.server.SQLiteProcessor;

/** "SHOW STATUS" statement that shows server current status, includes memory, 
 * thread, runtime and OS information.
 * 
 * @author little-pan
 * @since 2019-12-21
 *
 */
public class ShowStatusStatement extends LocalStatement {
    
    public static final String TBL_NAME = "status";
    
    public ShowStatusStatement(String sql) {
        super(sql, "SHOW STATUS", true);
    }

    @Override
    protected String getSQL(String localSchema) throws SQLException {
        final String f = 
                "select Mem_Committed, Mem_Max, Mem_Used, "
                + "OS_Arch, OS_Name, OS_Version, "
                + "RT_Name, RT_Start_Time, RT_Uptime, RT_Vendor, RT_Version, "
                + "Thread_Count, Thread_Daemon_Count, Thread_Peak_Count, Thread_Started_Count, "
                + "Sys_Load_Average "
                + "from '%s'.%s";
        return format(f, localSchema, TBL_NAME);
    }
    
    @Override
    protected void init() throws SQLException {
        super.init();
        
        String localSchema = super.localDb.getSchemaName();
        String f, sql;
        // CREATE TABLE
        f = "create table if not exists '%s'.%s("
                + "`Mem_Committed` bigint null,"
                + "`Mem_Max` bigint not null,"
                + "`Mem_Used` bigint not null,"
                + "`OS_Arch` varchar(80),"
                + "`OS_Name` varchar(64),"
                + "`OS_Version` varchar(64),"
                + "`RT_Name` varchar(64),"
                + "`RT_Start_Time` varchar(20),"
                + "`RT_Uptime` bigint,"
                + "`RT_Vendor` varchar(80),"
                + "`RT_Version` varchar(64),"
                + "`Thread_Count` integer,"
                + "`Thread_Daemon_Count` integer,"
                + "`Thread_Peak_Count` integer,"
                + "`Thread_Started_Count` bigint,"
                + "`Sys_Load_Average` double)";
        sql = format(f, localSchema, TBL_NAME);
        execute(sql);
    }
    
    @Override
    public void preExecute(int maxRows) throws SQLException, IllegalStateException {
        super.preExecute(maxRows);
        
        String localSchema = super.localDb.getSchemaName(), f, sql;
        // DELETE old data
        f = "delete from '%s'.%s";
        sql = format(f, localSchema, TBL_NAME);
        execute(sql);
        
        // Collect processor state
        SQLiteProcessor processor = super.getContext();
        // INSERT new data for query
        f = "insert into '%s'.%s(`Mem_Committed`, `Mem_Max`, `Mem_Used`, `OS_Arch`, `OS_Name`, `OS_Version`, "
                + "`RT_Name`, `RT_Start_Time`, `RT_Uptime`, `RT_Vendor`, `RT_Version`, "
                + "`Thread_Count`, `Thread_Daemon_Count`, `Thread_Peak_Count`, `Thread_Started_Count`, "
                + "`Sys_Load_Average`)"
                + "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        sql = format(f, localSchema, TBL_NAME);
        try (PreparedStatement ps = processor.getConnection().prepareStatement(sql)) {
            MemoryMXBean memMxBean = ManagementFactory.getMemoryMXBean();
            MemoryUsage heapMemUsage = memMxBean.getHeapMemoryUsage();
            MemoryUsage nonheapMemUsage = memMxBean.getNonHeapMemoryUsage();
            RuntimeMXBean rtMxBean = ManagementFactory.getRuntimeMXBean();
            OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();
            ThreadMXBean thrMxBean = ManagementFactory.getThreadMXBean();
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            int i = 0;
            long v = -1;
            
            // Memory committed
            if (heapMemUsage.getCommitted() > 0) {
                v  = heapMemUsage.getCommitted();
            }
            if (nonheapMemUsage.getCommitted() > 0) {
                if (v > 0) {
                    v += nonheapMemUsage.getCommitted();
                } else {
                    v  = nonheapMemUsage.getCommitted();
                }
            }
            ps.setLong(++i, v);
            // Memory max
            v = -1;
            if (heapMemUsage.getMax() > 0) {
                v  = heapMemUsage.getMax();
            }
            if (nonheapMemUsage.getMax() > 0) {
                if (v > 0) {
                    v += nonheapMemUsage.getMax();
                } else {
                    v  = nonheapMemUsage.getMax();
                }
            }
            ps.setLong(++i, v);
            // Memory used
            v = -1;
            if (heapMemUsage.getUsed() > 0) {
                v  = heapMemUsage.getUsed();
            }
            if (nonheapMemUsage.getUsed() > 0) {
                if (v > 0) {
                    v += nonheapMemUsage.getUsed();
                } else {
                    v  = nonheapMemUsage.getUsed();
                }
            }
            ps.setLong(++i, v);
            
            ps.setString(++i, osMxBean.getArch());
            ps.setString(++i, osMxBean.getName());
            ps.setString(++i, osMxBean.getVersion());
            
            ps.setString(++i, rtMxBean.getVmName());
            ps.setString(++i, df.format(new Date(rtMxBean.getStartTime())));
            ps.setLong(++i, rtMxBean.getUptime());
            ps.setString(++i, rtMxBean.getVmVendor());
            ps.setString(++i, format("Java %s(build %s)", 
                    rtMxBean.getSpecVersion(), rtMxBean.getVmVersion()));
            
            ps.setInt(++i, thrMxBean.getThreadCount());
            ps.setInt(++i, thrMxBean.getDaemonThreadCount());
            ps.setInt(++i, thrMxBean.getPeakThreadCount());
            ps.setLong(++i, thrMxBean.getTotalStartedThreadCount());
            
            ps.setDouble(++i, osMxBean.getSystemLoadAverage());

            ps.executeUpdate();
        }
    }

}
