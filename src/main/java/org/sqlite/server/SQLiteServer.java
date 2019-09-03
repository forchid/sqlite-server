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
package org.sqlite.server;

import java.io.PrintStream;
import java.net.Socket;

import org.sqlite.server.pg.PgServer;

/**<p>
 * The SQLite server based on the C/S architecture.
 * </p>
 * 
 * @author little-pan
 * @since 2019-03-19
 *
 */
public class SQLiteServer extends Server {
    
    protected Server base;
    
    public static void main(String args[]) {
        final SQLiteServer server = new SQLiteServer();
        server.init(args);
        server.start();
        server.listen();
    }
    
    @Override
    public void init(String... args) {
        if (args != null) {
            for (int i = 0, argc = args.length; i < argc; i++) {
                String a = args[i];
                if ("--help".equals(a) || "-?".equals(a)) {
                    help(0);
                } else if ("--protocol".equals(a)) {
                    String proto = args[++i];
                    if ("pg".equalsIgnoreCase(proto)) {
                        this.base = new PgServer();
                    } else {
                        help(1);
                    }
                }
            }
        }
        
        if (this.base == null) {
            this.base = new PgServer();
        }
        
        this.base.init(args);
    }

    @Override
    public void start() {
        this.base.start();
    }

    @Override
    public void listen() {
        this.base.listen();
    }

    @Override
    public void stop() {
        this.base.stop();
    }
    
    @Override
    protected Processor newProcessor(Socket s, int processId) {
        return this.base.newProcessor(s, processId);
    }

    static void help(int status) {
        final PrintStream out = System.out;
        out.println("Usage: java org.sqlite.server.SQLiteServer [OPTIONS]\n"+
                "OPTIONS: \n"+
                "  --data-dir|-D  <path>       Server data directory, default data in work dir\n"+
                "  --user|-U      <user>       User's name, default "+USER_DEFAULT+"\n"+
                "  --password|-p  <password>   User's password, must be provided in md5 auth method\n"+
                "  --host|-H      <host>       Server listen host or IP, default localhost\n"+
                "  --port|-P      <number>     Server listen port, default "+PORT_DEFAULT+"\n"+
                "  --max-conns    <number>     Max connections limit, default "+MAX_CONNS_DEFAULT+"\n"+
                "  --trace|-T                  Trace the server execution\n" +
                "  --protocol     <pg>         The server protocol, default pg\n" +
                "  --help|-?                   Show this message");
        System.exit(status);
    }

}
