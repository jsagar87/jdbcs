import java.sql.*;

public class JDBCTest {
    /**
     *
     * @param conInf Information to pass DB connection parameters like, username, password, url
     * @return Object of Connection type is returned if successfully able to get connection from driver
     * @throws SQLException if could not able to retrieve connection
     */
    public Connection openConnection(ConnectionInfo conInf)
            throws SQLException {
        Connection connection = DriverManager.getConnection(
                conInf.url,
                conInf.user,
                conInf.password);
        return connection;
    }
    /*******************************************************
     *  Test suit / client code / main method
     *******************************************************/
    public static void main(String[] args) {

        String crud[] = {"read","201"} ;
        if (args.length > 1)
            crud[1] = args[1];

        /// JDBC rolling
        JDBCTest jdbcHelper = new JDBCTest();

        /// DAO
        DataAccessObject dao = new ActorDaoImpl();

        // Connection properties
        String url = "jdbc:postgresql://localhost/dvdrental";   // database specific url.
        String user = "postgres";
        String password = "postgres";

        // Required driverf
        String driverClassName = "org.postgresql.Driver";

        try {
            Class.forName(driverClassName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        // Provide Connection info
        ConnectionInfo connInfo = new ConnectionInfo()
                .setUrl(url)
                .setUser(user)
                .setPassword(password);

        // Opening the connection
        Connection connection = null;
        try {
            connection = jdbcHelper.openConnection(connInfo);
            ResultSet result = null;
            try {
                if (crud[0].equals("create")) {
                    // Create an object to be persisted
                    Actor actor = new Actor();
                    actor.setActor_id(201);
                    actor.setFirstName("Aditi");
                    actor.setLastName("Parikh");
                    // Create a new Actor
                    boolean success = dao.create(connection, actor);
                    System.out.println("success:" + success);
                } else if ("update".equals(crud[0])) {
                    Actor actor = new Actor();
                    actor.setLastName("Jariwala");
                    actor.setActor_id(201);
                    int[] success = dao.update(connection,actor);
                    System.out.println(success + " rows updated");
                } else if ("delete".equals(crud[0])) {
                    int success = dao.delete(connection, 201);
                    System.out.println(success + " rows deleted");
                } else {
                    if (!"".equals(crud[1])){
                        if ("200".equals(crud[1])) {
                            // Query the database
                            result = dao.get(connection, 201);
                            // Iterating result set
                            while (result.next()) {
                                Integer id = result.getInt("actor_id");
                                String f_name = result.getString("first_name");
                                String l_name = result.getString("last_name");
                                System.out.println("Id : " + id + " f_name: " + f_name + " l_name : " + l_name);
                            }
                        }
                        else
                        {
                            result = dao.getFilmsByActor(connection,200);
                            // Iterating result set
                            System.out.println("Actor  \t  Movie   \t Description ");
                            while (result.next()) {
                                String actor = result.getString("actor_name");
                                String movie = result.getString("film_title");
                                String description = result.getString("film_description");
                                System.out.println(actor  + " | " + movie + " | " + description);
                            }
                        }
                    }else {
                        // Query the database
                        result = dao.getAll(connection);
                        // Iterating result set
                        while (result.next()) {
                            Integer id = result.getInt("actor_id");
                            String f_name = result.getString("first_name");
                            String l_name = result.getString("last_name");
                            System.out.println("Id : " + id + " f_name: " + f_name + " l_name : " + l_name);
                        }
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if(result!=null) result.close();
            }
        }catch (SQLException se){
            se.printStackTrace();
        }finally {
            if(connection != null)
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
        }
    }
}
/* *********************************************************************************************************************
 *  All Major Interfaces, Implementation, Model and helper classes
 **********************************************************************************************************************/

/* *********
 *   Model
 ***********/
class Actor{
    private String firstName;
    private String lastName;
    private int actor_id;
    public String getFirstName() { return firstName; }
    public void setFirstName(String firstName) { this.firstName = firstName; }
    public String getLastName() { return lastName; }
    public void setLastName(String lastName) { this.lastName = lastName; }
    public int getActor_id() { return actor_id; }
    public void setActor_id(int actor_id) { this.actor_id = actor_id; }
}
/* *********
 *   DAO
 ***********/
interface DataAccessObject<Actor>{
    boolean create(Connection conn, Actor item) throws SQLException;
    /**
     * <p>
     *     Read operation. Gets all the actor information that includes first and last name, as well as id
     * </p>
     * @param conn - Supply active connection object
     * @return resultset with result for actor(s) with actor_id = id
     * @throws SQLException if a database access error occurs, this method is called on a closed connection or the given parameters are not ResultSet constants indicating type and concurrency
     */
    ResultSet getAll(Connection conn) throws SQLException;
    /**
     *
     * @param conn Supply active connection object
     * @param id actor_id for which data to be fetched
     * @return resultset with result for actor(s) with actor_id = id
     * @throws SQLException if a database access error occurs, this method is called on a closed connection or the given parameters are not ResultSet constants indicating type and concurrency
     */
    ResultSet get(Connection conn,int id) throws SQLException;

    ResultSet getFilmsByActor(Connection conn,int id) throws SQLException;

    /**
     *
     * @param conn Supply active connection object
     * @param item
     * @return number of updated records
     * @throws SQLException if a database access error occurs, this method is called on a closed connection or the given parameters are not ResultSet constants indicating type and concurrency
     */
    int[] update(Connection conn,Actor item) throws SQLException;

    /**
     *
     * @param conn Supply active connection object
     * @param id actor_id for which record to be fetched
     * @return
     * @throws SQLException if a database access error occurs, this method is called on a closed connection or the given parameters are not ResultSet constants indicating type and concurrency
     */
    int delete(Connection conn,int id) throws SQLException;
}
class ActorDaoImpl implements DataAccessObject<Actor>{
    /* **********************************
     *  Methods for CRUD operation
     ************************************/
    /********* CREATE **********/
    public boolean create(Connection conn, Actor actor)
            throws SQLException {
        Statement statement = conn.createStatement();
        //String sql = "INSERT INTO actor VALUES ({0},{1},{2})";
        String sql = "INSERT INTO actor VALUES (" + actor.getActor_id() + ",\'" + actor.getFirstName() + "\',\'" + actor.getLastName() + "\')";
        System.out.println(sql);
        boolean result = statement.execute(sql);
        return result;
    }
    /********* READ **********/
    public ResultSet getAll(Connection conn)
            throws SQLException {
        /*
         * To query a database we need to send SQL statement to database for that using JDBC we need to,
         *  1. Create statement object
         *  2. form a query as a string
         *  3. Execute query-statement
         * */
        Statement statement = conn.createStatement();
        String sql = "SELECT a.actor_id, a.first_name, a.last_name FROM actor a where a.actor_id > 195;";
        ResultSet result = statement.executeQuery(sql);
        return result;
    }
    public ResultSet get(Connection conn,int id)
            throws SQLException {

        String sql = "SELECT a.actor_id, a.first_name, a.last_name FROM actor a where a.actor_id = ?";
        PreparedStatement statement = conn.prepareCall(
                sql,
                ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT
        );
        statement.setInt(1,id);
        ResultSet result = statement.executeQuery();
        return result;
    }
    public ResultSet getFilmsByActor(Connection conn,int id)
            throws SQLException {
        CallableStatement filmsByActorProcedure = conn.prepareCall("{call films_by_actors(?)}");
        filmsByActorProcedure.setInt(1,id);
        ResultSet result = filmsByActorProcedure.executeQuery();
        return result;
    }
    /********* UPDATE **********/ /***** added batch & transaction logic *****/
    public int[] update(Connection conn,Actor actor)
            throws SQLException {
        Statement statement = null;
        try {
            conn.setAutoCommit(false);

            statement = conn.createStatement();
            // Add 1st item to the batch
            String sql = "UPDATE actor SET last_name='" + actor.getLastName()
                    + "' where actor_id=" + actor.getActor_id();
            statement.addBatch(sql);
            // Add 2nd item to the batch

            int[] results = statement.executeBatch();

            conn.commit();
            return results;
        }catch (SQLException sqlException){
            conn.rollback();
            System.out.println("Transaction rolled back");
            throw new SQLException();
        }finally {
            if (statement!= null) statement.close();
        }
    }
    /********* DELETE **********/
    public int delete(Connection conn,int id)
            throws SQLException {
        Statement statement = conn.createStatement();
        String sql = "DELETE FROM actor where actor_id = "+id;
        System.out.println(sql);
        int result = statement.executeUpdate(sql);
        return result;
    }
}
class ConnectionInfo {
    String url, user, password;
    ConnectionInfo() { }
    public ConnectionInfo setUrl(String url) {
        this.url = url;
        return this;
    }
    public ConnectionInfo setUser(String user) {
        this.user = user;
        return this;
    }
    public ConnectionInfo setPassword(String password) {
        this.password = password;
        return this;
    }
}