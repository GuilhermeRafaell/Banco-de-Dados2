package com.oracle.tutorial.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Scanner;
import java.sql.DatabaseMetaData;
import java.sql.Date;

public class MyQueries7{
  
  Connection con;
  JDBCUtilities settings;  
  
  public MyQueries7(Connection connArg, JDBCUtilities settingsArg) {
    this.con = connArg;
    this.settings = settingsArg;
  }

  public static void populateTable(Connection con) throws SQLException, IOException {
    Statement stmt = null;
    String create = "";
    BufferedReader inputStream = null;
    Scanner scanned_line = null;
    String line;
    String[] value;
    value = new String[7];
    int countv;
    con.setAutoCommit(false);
    try {
        stmt = con.createStatement();
        stmt.executeUpdate("truncate table debito;");
        inputStream = new BufferedReader(new FileReader("/home/brenolinux/Trab7/debito-populate-table.txt"));
        while ((line = inputStream.readLine()) != null) {
            countv = 0;
            //split fields separated by tab delimiters 
            scanned_line = new Scanner(line);
            scanned_line.useDelimiter("\t");
            while (scanned_line.hasNext()) {
              System.out.println(value[countv++] = scanned_line.next());
            } //while
            if (scanned_line != null) {
                scanned_line.close();
            }
            
            create = "insert into debito (numero_debito, valor_debito,  motivo_debito, data_debito, numero_conta, nome_agencia, nome_cliente) " + "values (" + value[0] + ", " + value[1] + ", " + value[2] + ", '" + value[3] + "', " + value[4] + ", '" + value[5] + "', '" + value[6] + "');";
            stmt.executeUpdate(create);
        }
    } 
    catch (SQLException e) {
        JDBCUtilities.printSQLException(e);
    } 
    catch (IOException e) {
      e.printStackTrace(); 
    }
    finally {
      if (stmt != null) {
          stmt.close();
      }
      if (inputStream != null) {
              inputStream.close();
      }
        con.setAutoCommit(true);
    }
  }

  public static void getMyData3(Connection con) throws SQLException {
     Statement stmt = null;
     String query = "select C.nome_cliente , sum(deposito.saldo_deposito) as total_dep,sum(emprestimo.valor_emprestimo) as total_emp from conta as C natural full join (emprestimo natural full join deposito) group by C.nome_cliente ,C.nome_agencia ,C.numero_conta";
    try {
       stmt = con.createStatement();
       ResultSet rs = stmt.executeQuery(query);
       System.out.println("Contas da Instituicao Bancaria e seus totais de depositos e emprestimos: ");
       while (rs.next()) {
         String nome = rs.getString("nome_cliente");
         Float depositos = rs.getFloat("total_dep");
         Float emprestimos = rs.getFloat(3);

         System.out.println(nome +", " + depositos.toString() +", "+emprestimos.toString());
      }     
    }
    catch (SQLException e) {
       JDBCUtilities.printSQLException(e);
     }
     finally {
       if (stmt != null) {
        stmt.close(); 
        }     
    }   
} 

    public static void cursorHoldabilitySupport(Connection conn)     throws SQLException {
     DatabaseMetaData dbMetaData = conn.getMetaData();
     System.out.println("ResultSet.HOLD_CURSORS_OVER_COMMIT = " + ResultSet.HOLD_CURSORS_OVER_COMMIT);
     System.out.println("ResultSet.CLOSE_CURSORS_AT_COMMIT = " + ResultSet.CLOSE_CURSORS_AT_COMMIT);
     System.out.println("Default cursor holdability: " +  dbMetaData.getResultSetHoldability());     System.out.println("Supports HOLD_CURSORS_OVER_COMMIT? " + dbMetaData.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
     System.out.println("Supports   CLOSE_CURSORS_AT_COMMIT? " + dbMetaData.supportsResultSetHoldability(             ResultSet.CLOSE_CURSORS_AT_COMMIT));
    
     System.out.println("Supports CONCUR_READ_ONLY TYPE_FORWARD_ONLY?" + dbMetaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY));
     System.out.println("Supports CONCUR_READ_ONLY TYPE_SCROLL_INSENSITIVE? " + dbMetaData.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY));
     System.out.println("Supports CONCUR_READ_ONLY TYPE_SCROLL_SENSITIVE? " + dbMetaData.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY));

    System.out.println("Supports   CONCUR_UPDATABLE TYPE_FORWARD_ONLY? " + dbMetaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_UPDATABLE));
    System.out.println("Supports   CONCUR_UPDATABLE TYPE_SCROLL_INSENSITIVE? " + dbMetaData.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE));
    System.out.println("Supports   CONCUR_UPDATABLE TYPE_SCROLL_SENSITIVE? " + dbMetaData.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_UPDATABLE));


 } 

public static void modifyPrices(Connection con) throws SQLException {
     Statement stmt = null;
     try {
         stmt = con.createStatement();
         stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_UPDATABLE);
         ResultSet uprs = stmt.executeQuery( "SELECT * FROM deposito");
         System.out.println("Digite o multiplicador como um numero real (Ex.: 5% = 1,05):");
         Scanner in = new Scanner(System.in);
         float percentage = in.nextFloat(); 
         while (uprs.next()) {
             float f = uprs.getFloat("saldo_deposito");
             uprs.updateFloat("saldo_deposito", f*percentage);
             uprs.updateRow();
         }     
    } 
    catch (SQLException e ) {
         JDBCTutorialUtilities.printSQLException(e);
     } 
    finally {
         if (stmt != null) { stmt.close();
        }    
    } 
} 

public static void insertRow(Connection con)
  throws SQLException {     Statement stmt = null;
     try {
         stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_UPDATABLE);
         ResultSet uprs = stmt.executeQuery("SELECT * FROM debito");
         uprs.moveToInsertRow(); //posiciona no ponto de inserção da tabela
         uprs.updateInt("numero_debito",5000);
         uprs.updateFloat("valor_debito",150);
         uprs.updateInt("motivo_debito",1);
         uprs.updateDate("data_debito", Date.valueOf("2014-01-23") );
         uprs.updateInt("numero_conta",46248);
         uprs.updateString("nome_agencia","UFU");
         uprs.updateString("nome_cliente","Carla Soares Sousa");
         uprs.insertRow(); //insere a linha na tabela
         uprs.beforeFirst(); //posiciona-se novamente na posição anterior ao primeiro registro
        uprs.moveToInsertRow();

        uprs.updateInt("numero_debito",6001);
         uprs.updateFloat("valor_debito",200);
         uprs.updateInt("motivo_debito",2);
         uprs.updateDate("data_debito", Date.valueOf("2014-01-23") );
         uprs.updateInt("numero_conta",26892);
         uprs.updateString("nome_agencia","Glória");
         uprs.updateString("nome_cliente","Carolina Soares Souza");
         uprs.insertRow(); //insere a linha na tabela
         uprs.beforeFirst(); //posiciona-se novamente na posição anterior ao primeiro registro
         uprs.moveToInsertRow();

        uprs.updateInt("numero_debito",2002);
         uprs.updateFloat("valor_debito",500);
         uprs.updateInt("motivo_debito",3);
         uprs.updateDate("data_debito", Date.valueOf("2014-01-23") );
         uprs.updateInt("numero_conta",70044);
         uprs.updateString("nome_agencia","Cidade Jardim");
         uprs.updateString("nome_cliente","Eurides Alves da Silva");
         uprs.insertRow(); //insere a linha na tabela
         uprs.beforeFirst();


    } catch (SQLException e ) {
         JDBCTutorialUtilities.printSQLException(e);
     } finally {
         if (stmt != null) {
             stmt.close();
         }     
       }
}

public static void insertMyData1(Connection con) throws SQLException {
     long startTime = System.currentTimeMillis();
     Statement stmt = null;
     String query = null;
    query  = "insert into debito (numero_debito, valor_debito, motivo_debito, data_debito, numero_conta, nome_agencia, nome_cliente) " +       "values (3000,3000,5,'2014-02-06',36593,'UFU','Pedro Alvares Sousa');" ;
     try {
       stmt = con.createStatement();
       stmt.executeUpdate(query);
       if (stmt != null) {
        stmt.close();
        } 
      System.out.println("Debitos da Instituicao Bancaria atualizados.");
     } catch (SQLException e) {
       JDBCUtilities.printSQLException(e);
     }
    long endTime = System.currentTimeMillis();System.out.println("Um debito em IB2 com a função InsertMyData1 inserido em " + (endTime - startTime) + " milisegundos");
}

public static void insertMyData2(Connection con) throws SQLException {
     long startTime = System.currentTimeMillis();
     PreparedStatement stmt = null;
     String query = null;
    query  = "insert into debito (numero_debito, valor_debito, motivo_debito, data_debito, numero_conta, nome_agencia, nome_cliente) " +       "values (?,?,?,?,?,?,?);" ;
     try {
       stmt = con.prepareStatement(query);
        stmt.setInt(1, 3001);
          stmt.setDouble(2, 3001);
         stmt.setInt(3, 4);
         stmt.setDate(4, Date.valueOf("2014-02-06") );
         stmt.setInt(5, 36593);
         stmt.setString(6, "UFU");
         stmt.setString(7, "Pedro Alvares Sousa");
         stmt.executeUpdate();
       if (stmt != null) {
        stmt.close();
        } 
      System.out.println("Debitos da Instituicao Bancaria atualizados.");
     } catch (SQLException e) {
       JDBCUtilities.printSQLException(e);
     }
    long endTime = System.currentTimeMillis();System.out.println("Um debito em IB2 com a função InsertMyData2 inserido em " + (endTime - startTime) + " milisegundos");
}


public static void insertMyData1000(Connection con) throws SQLException {
     long startTime = System.currentTimeMillis();
     Statement stmt = null;
     String query = null;
     System.out.println("Statement:");
    for(int numdeb = 3002; numdeb < 4002; numdeb++){
        query="insert into debito (numero_debito, valor_debito, motivo_debito, data_debito, numero_conta,nome_agencia, nome_cliente) " + "values ("+Integer.toString(numdeb)+","+Integer.toString(numdeb)+",5,'2014-02-06',36593,'UFU','Pedro Alvares Sousa');" ; 
  
     try {
       stmt = con.createStatement();
         stmt.executeUpdate(query);
       if (stmt != null) {
        stmt.close();
        }
      if((numdeb%50)==0){ long endTime = System.currentTimeMillis();
        System.out.println(numdeb-3000 + "\t" + (endTime - startTime)); } 
     } catch (SQLException e) {
       JDBCUtilities.printSQLException(e);
     }
  } 
}

public static void insertMyData2000(Connection con) throws SQLException {
     long startTime = System.currentTimeMillis();
     PreparedStatement stmt = null;
     String query = null;
     query  = "insert into debito (numero_debito, valor_debito, motivo_debito, data_debito, numero_conta, nome_agencia, nome_cliente) " + "values (?,?,?,?,?,?,?);" ;
     System.out.println("PreparedStatement:");
    for(int numdeb = 9002; numdeb < 10002; numdeb++){
     try {
       stmt = con.prepareStatement(query);
         stmt.setInt(1, numdeb);
         stmt.setDouble(2, numdeb);
         stmt.setInt(3, 4);
         stmt.setDate(4, Date.valueOf("2014-02-06") );
         stmt.setInt(5, 36593);
         stmt.setString(6, "UFU");
         stmt.setString(7, "Pedro Alvares Sousa");
         stmt.executeUpdate();
       if (stmt != null) {
        stmt.close();
        }
      if((numdeb%50)==0){ long endTime = System.currentTimeMillis();
        System.out.println(numdeb-9000 + "\t" + (endTime - startTime)); } 
     } catch (SQLException e) {
       JDBCUtilities.printSQLException(e);
     }
  } 
}

public static void insertMyData3000(Connection con) throws SQLException {
     long startTime = System.currentTimeMillis();
     PreparedStatement stmt = null;
     String query = null;
     query  = "insert into debito (numero_debito, valor_debito, motivo_debito, data_debito, numero_conta, nome_agencia, nome_cliente) " + "values (?,?,?,?,?,?,?);" ;
     System.out.println("Commit PreparedStatement:");
     try {
         con.setAutoCommit(false);
         stmt = con.prepareStatement(query);
     } catch (SQLException e) {
       JDBCUtilities.printSQLException(e);
     } 
    for(int numdeb = 6002; numdeb < 7002; numdeb++){
     try {
         stmt.setInt(1, numdeb);
         stmt.setDouble(2, numdeb);
         stmt.setInt(3, 4);
         stmt.setDate(4, Date.valueOf("2014-02-06") );
         stmt.setInt(5, 36593);
         stmt.setString(6, "UFU");
         stmt.setString(7, "Pedro Alvares Sousa");
         stmt.executeUpdate();
      if((numdeb%50)==0){ long endTime = System.currentTimeMillis();
        System.out.println(numdeb-6000 + "\t" +(endTime - startTime)); } 
     } catch (SQLException e) {
       JDBCUtilities.printSQLException(e);
     }
  }
     try {
       
        con.commit();
        if (stmt != null) {
         stmt.close();
        }     
     } catch (SQLException e) {
       JDBCUtilities.printSQLException(e);
     } 
}

public static void insertMyData4000(Connection con) throws SQLException {
     long startTime = System.currentTimeMillis();
     Statement stmt = null;
     String query = null;
    con.setAutoCommit(false);
     System.out.println("Commit Statement:");
    for(int numdeb = 7002; numdeb < 8002; numdeb++){
        query="insert into debito (numero_debito, valor_debito, motivo_debito, data_debito, numero_conta,nome_agencia, nome_cliente) " + "values ("+Integer.toString(numdeb)+","+Integer.toString(numdeb)+",5,'2014-02-06',36593,'UFU','Pedro Alvares Sousa');" ; 
  
     try {
       stmt = con.createStatement();
         stmt.executeUpdate(query);
      if((numdeb%50)==0){ long endTime = System.currentTimeMillis();
        System.out.println(numdeb-7000 + "\t" + (endTime - startTime)); } 
     } catch (SQLException e) {
       JDBCUtilities.printSQLException(e);
     }
  }

     try {
       
        con.commit();
        if (stmt != null) {
         stmt.close();
        }     
     } catch (SQLException e) {
       JDBCUtilities.printSQLException(e);
     } 
}

  public static void main(String[] args) {
    JDBCUtilities myJDBCUtilities;
    Connection myConnection = null;
    if (args[0] == null) {
      System.err.println("Properties file not specified at command line");
      return;
    } else {
      try {
        myJDBCUtilities = new JDBCUtilities(args[0]);
      } catch (Exception e) {
        System.err.println("Problem reading properties file " + args[0]);
        e.printStackTrace();
        return;
      }
    }

    try {
      myConnection = myJDBCUtilities.getConnection();
      MyQueries7.insertMyData1(myConnection);
      MyQueries7.insertMyData2(myConnection);
      MyQueries7.insertMyData1000(myConnection);
      MyQueries7.insertMyData2000(myConnection);
      MyQueries7.insertMyData3000(myConnection);
      MyQueries7.insertMyData4000(myConnection);


    } catch (SQLException e) {
      JDBCUtilities.printSQLException(e);
    } 
  
    finally {
      JDBCUtilities.closeConnection(myConnection);
    }

  }
}
