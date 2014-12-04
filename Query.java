import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Date;

import java.io.FileInputStream;

/**
 * Runs queries against a back-end database
 */
public class Query {
    private static Properties configProps = new Properties();

    private static String imdbUrl;
    private static String customerUrl;

    private static String postgreSQLDriver;
    private static String postgreSQLUser;
    private static String postgreSQLPassword;

    // DB Connection
    private Connection _imdb;
    private Connection _customer_db;

    // Canned queries

    private String _search_sql = "SELECT * FROM movie WHERE name ilike ? ORDER BY id";
    private PreparedStatement _search_statement;

    private String _movie_name_mid_sql = "SELECT MOVIE.name FROM MOVIE WHERE MOVIE.id=?";
    private String _director_mid_sql = "SELECT y.* "
                     + "FROM Movie_Directors x, Directors y "
                     + "WHERE x.mid = ? and x.did = y.id";

    private String _actor_mid_sql = "SELECT y.* "
                     + "FROM casts x, actor y "
                     + "WHERE x.mid = ? and x.pid = y.id";
               
    private String _director_join_sql = "SELECT m.id, d.fname, d.lname "
                                      + "FROM MOVIE as m, Directors as d, MOVIE_DIRECTORS as md "
                                      + "WHERE m.name ilike ? AND m.id = md.mid AND d.id = md.did "                                        
                                      + "ORDER BY m.id ";
    private String _actor_join_sql = "SELECT m.id, a.fname, a.lname "
                                      + "FROM MOVIE as m, Actor as a, Casts as c "
                                      + "WHERE m.name ilike ? AND m.id = c.mid AND a.id = c.pid "
                                      + "ORDER BY m.id ";
                                     
    private String _personal_info_sql = "SELECT c.fname, c.lname, r.planid "
                                      + "FROM customers as c, RentalPlans as r "
                                      + "WHERE c.custid = ? AND c.planid = r.planid " ;
                       
    private String _rental_mid_sql = "SELECT cr.custid FROM currentrentals cr WHERE cr.movieid = ?";
    private String _rental_time_sql = "SELECT * FROM currentrentals cr WHERE cr.movieid = ?";
    
    private String _all_rentals_sql = "SELECT * FROM RentalPlans";
    private String _list_user_rentals_sql = "SELECT cr.movieID "
                                          + "FROM currentRentals as cr "
                                          + "WHERE cr.custID = ? ";
                                          
    private String _rent_sql = "INSERT into CurrentRentals VALUES(?, ?, ?, ?)";
    
    private String _available_rents_sql = "SELECT (rentalmax - (select count(*) from currentrentals where custid = ?)) "                                       
                                          +"FROM customers, rentalplans "
                                          +"WHERE customers.planid=rentalplans.planid and custID = ?";
                                          
    private String _return_sql = "INSERT INTO pastRentals VALUES (?, ?, ?, ?, ?) ";
    private String _delete_sql = "DELETE FROM currentRentals WHERE movieID = ? ";
    
    private String _update_plan_sql = "UPDATE customers SET planid = ? "
                                    + "WHERE custID = ? ";
    private String _max_ups_sql = "select rentalmax from rentalplans where planid = ? ";
    private String _curr_max_sql = " Select rentalmax FROM rentalplans as r, customers as c WHERE c.custid = ? AND  r.planid = c.planid ";

    //private String _update_plan_sql = 
        
    private PreparedStatement _director_mid_statement;
    private PreparedStatement _actor_mid_statement;
    private PreparedStatement _rental_mid_statement;
    private PreparedStatement _director_join_statement;
    private PreparedStatement _actor_join_statement;
    /* uncomment, and edit, after your create your own customer database */
    private String _customer_login_sql = "SELECT * FROM customers WHERE username = ? and password = ?";
    private PreparedStatement _customer_login_statement;

    private String _begin_transaction_read_write_sql = "BEGIN TRANSACTION READ WRITE";
    private PreparedStatement _begin_transaction_read_write_statement;

    private String _commit_transaction_sql = "COMMIT TRANSACTION";
    private PreparedStatement _commit_transaction_statement;

    private String _rollback_transaction_sql = "ROLLBACK TRANSACTION";
    private PreparedStatement _rollback_transaction_statement;
    private PreparedStatement _all_rentals_statement;
    private PreparedStatement _list_user_rentals_statement;
    private PreparedStatement _personal_info_statement;
    private PreparedStatement _available_rents_statement;
    private PreparedStatement _rent_statement;
    private PreparedStatement _list_user_rentals_sql_statement;
    private PreparedStatement _return_statement;
    private PreparedStatement _delete_statement;
    private PreparedStatement _update_plan_statement;
    private PreparedStatement _rental_time_statement;
    private PreparedStatement _max_ups_statement;
    private PreparedStatement _curr_max_statement;
    private PreparedStatement _movie_name_mid_statement;
    public Query() {
    }

    /**********************************************************/
    /* Connections to postgres databases */

    public void openConnection() throws Exception {
        configProps.load(new FileInputStream("dbconn.config"));
        
        
        imdbUrl        = configProps.getProperty("imdbUrl");
        customerUrl    = configProps.getProperty("customerUrl");
        postgreSQLDriver   = configProps.getProperty("postgreSQLDriver");
        postgreSQLUser     = configProps.getProperty("postgreSQLUser");
        postgreSQLPassword = configProps.getProperty("postgreSQLPassword");


        /* load jdbc drivers */
        Class.forName(postgreSQLDriver).newInstance();

        /* open connections to TWO databases: imdb and the customer database */
        _imdb = DriverManager.getConnection(imdbUrl, // database
                postgreSQLUser, // user
                postgreSQLPassword); // password

        _customer_db = DriverManager.getConnection(customerUrl, // database
                postgreSQLUser, // user
                postgreSQLPassword); // password
    }

    public void closeConnection() throws Exception {
        _imdb.close();
        _customer_db.close();
    }

    /**********************************************************/
    /* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */

    public void prepareStatements() throws Exception {

        _search_statement = _imdb.prepareStatement(_search_sql);
        _director_mid_statement = _imdb.prepareStatement(_director_mid_sql);
        _actor_mid_statement = _imdb.prepareStatement(_actor_mid_sql);
        _rental_mid_statement = _customer_db.prepareStatement(_rental_mid_sql);
        _director_join_statement = _imdb.prepareStatement(_director_join_sql);
        _actor_join_statement = _imdb.prepareStatement(_actor_join_sql);
        /* uncomment after you create your customers database */
        
        _customer_login_statement = _customer_db.prepareStatement(_customer_login_sql);
        _begin_transaction_read_write_statement = _customer_db.prepareStatement(_begin_transaction_read_write_sql);
        _commit_transaction_statement = _customer_db.prepareStatement(_commit_transaction_sql);
        _rollback_transaction_statement = _customer_db.prepareStatement(_rollback_transaction_sql);
        _all_rentals_statement = _customer_db.prepareStatement(_all_rentals_sql);
        _list_user_rentals_statement = _customer_db.prepareStatement(_list_user_rentals_sql);
        _rental_mid_statement = _customer_db.prepareStatement(_rental_mid_sql);
        _personal_info_statement = _customer_db.prepareStatement(_personal_info_sql);
        _available_rents_statement = _customer_db.prepareStatement(_available_rents_sql);
        _rent_statement = _customer_db.prepareStatement(_rent_sql);
        /* add here more prepare statements for all the other queries you need */
        _return_statement = _customer_db.prepareStatement(_return_sql);
        _delete_statement = _customer_db.prepareStatement(_delete_sql);
        _update_plan_statement = _customer_db.prepareStatement(_update_plan_sql);
        _rental_time_statement = _customer_db.prepareStatement(_rental_time_sql);
        _max_ups_statement = _customer_db.prepareStatement(_max_ups_sql);
        _curr_max_statement = _customer_db.prepareStatement(_curr_max_sql);
	_movie_name_mid_statement = _imdb.prepareStatement(_movie_name_mid_sql);
        /* . . . . . . */
    }


    /**********************************************************/
    /* suggested helper functions  */

    public int helper_compute_remaining_rentals(int cid) throws Exception {
        /* how many movies can she/he still rent ? */
        /* you have to compute and return the difference between the customer's plan
           and the count of oustanding rentals */
        int result = 0;
        _available_rents_statement.clearParameters();
        _available_rents_statement.setInt(1, cid);
        _available_rents_statement.setInt(2, cid);
        ResultSet rentsAvailable_set = _available_rents_statement.executeQuery();
        while(rentsAvailable_set.next()){
              result = rentsAvailable_set.getInt(1);
        }
        return (result);
        
    }

    public String helper_compute_customer_name(int cid) throws Exception {
        /* you find  the first + last name of the current customer */
        return ("JoeFirstName" + " " + "JoeLastName");

    }

    public boolean helper_check_plan(int plan_id) throws Exception {
        /* is plan_id a valid plan id ?  you have to figure out */
        if(plan_id > 4 || plan_id < 1)
           return false;
           
        return true;
    }

    public boolean helper_check_movie(int mid) throws Exception {
        /* is mid a valid movie id ? you have to figure out  */
        return true;
    }

    private int helper_who_has_this_movie(int mid) throws Exception {
        /* find the customer id (cid) of whoever currently rents the movie mid; return -1 if none */
        return (77);
    }

	public String getMovieFromId(int mid) throws Exception
	{
		_movie_name_mid_statement.clearParameters();
		_movie_name_mid_statement.setInt(1, mid);
		ResultSet movie = _movie_name_mid_statement.executeQuery();
		while (movie.next()) 
		{
                	String temp = movie.getString(1);
			movie.close();
			return temp;
            	}
		return "No corresponding movie";
	}

    /**********************************************************/
    /* login transaction: invoked only once, when the app is started  */
    public int transaction_login(String name, String password) throws Exception {
        /* authenticates the user, and returns the user id, or -1 if authentication fails */

        /* Uncomment after you create your own customers database */
        
        int cid;

        _customer_login_statement.clearParameters();
        _customer_login_statement.setString(1,name);
        _customer_login_statement.setString(2,password);
        ResultSet cid_set = _customer_login_statement.executeQuery();
        if (cid_set.next()) cid = cid_set.getInt(1);
        else cid = -1;
        return(cid);
         
        //return (55);
    }

    public void transaction_personal_data(int cid) throws Exception {
        /* println the customer's personal data: name, and plan number */
        _personal_info_statement.clearParameters();
        _personal_info_statement.setInt(1, cid);
        ResultSet personal_info_set = _personal_info_statement.executeQuery();
        while(personal_info_set.next()){
            System.out.println("" + personal_info_set.getString(1) + " " + personal_info_set.getString(2) + " PlanID: " + personal_info_set.getInt(3) + " Rents Remaining: " + helper_compute_remaining_rentals(cid));
        }
        
    }


    /**********************************************************/
    /* main functions in this project: */

    public void transaction_search(int cid, String movie_title)
            throws Exception {
        /* searches for movies with matching titles: SELECT * FROM movie WHERE name LIKE movie_title */
        /* prints the movies, directors, actors, and the availability status:
           AVAILABLE, or UNAVAILABLE, or YOU CURRENTLY RENT IT */

        /* set the first (and single) '?' parameter */
        _search_statement.clearParameters();
        _search_statement.setString(1, '%' + movie_title + '%');

        ResultSet movie_set = _search_statement.executeQuery();
        while (movie_set.next()) {
            int mid = movie_set.getInt(1);
            System.out.println("ID: " + mid + " NAME: "
                    + movie_set.getString(2) + " YEAR: "
                    + movie_set.getString(3));
            /* do a dependent join with directors */
            _director_mid_statement.clearParameters();
            _director_mid_statement.setInt(1, mid);
            ResultSet director_set = _director_mid_statement.executeQuery();
            while (director_set.next()) {
                System.out.println("\t\tDirector: " + director_set.getString(3)
                        + " " + director_set.getString(2));
            }
            director_set.close();
            /* now you need to retrieve the actors, in the same manner */
            _actor_mid_statement.clearParameters();
            _actor_mid_statement.setInt(1, mid);
            ResultSet _actor_set = _actor_mid_statement.executeQuery();
            System.out.println("\t\tActors: ");
            while(_actor_set.next())
            {
                System.out.println("\t\t" + _actor_set.getString(2) + " " + _actor_set.getString(3));
            }
            _actor_set.close();           
            /* then you have to find the status: of "AVAILABLE" "YOU HAVE IT", "UNAVAILABLE" */
            _rental_mid_statement.clearParameters();
            _rental_mid_statement.setInt(1, mid);
            ResultSet _rental_set = _rental_mid_statement.executeQuery();
           // _rental_set.afterLast();
            int size = _rental_set.getRow();
            if(size > 0)
            {
                if(cid == _rental_set.getInt(2))
                    System.out.println("\t\tAvailability: YOU HAVE IT");
                else{
                    System.out.println("\t\tAvailability: UNAVAILABLE");
                }
            }
            else if(size == 0)
            {
                System.out.println("\t\tAvailability: AVAILABLE");
            }
            _rental_set.close();
        }
        System.out.println();
    }

    public void transaction_choose_plan(int cid, int pid) throws Exception {
        /* updates the customer's plan to pid: UPDATE customers SET plid = pid */
        /* remember to enforce consistency ! */
        int npMax = 0;
        int currMax = 0;
        if(helper_check_plan(pid) == false){
            System.out.println("Not a valid plan ID");
        }
        else
        {
          _max_ups_statement.clearParameters();
          _max_ups_statement.setInt(1, pid);
          _customer_db.setAutoCommit(false);
          ResultSet _new_plan_set = _max_ups_statement.executeQuery();
          while(_new_plan_set.next())
          {
            npMax = _new_plan_set.getInt(1);
          }
          _curr_max_statement.clearParameters();
          _curr_max_statement.setInt(1, cid);
          ResultSet _curr_plan_set = _curr_max_statement.executeQuery();
          while(_curr_plan_set.next())
          {
            currMax = _curr_plan_set.getInt(1);
          }
          if((currMax - helper_compute_remaining_rentals(cid)) > npMax)
          {
            _customer_db.rollback();
            System.out.println("You've rented more movies than the new plan allows");
          } 
            else
            {
              _update_plan_statement.clearParameters();
              _update_plan_statement.setInt(1, pid);
              _update_plan_statement.setInt(2, cid);
              int _up_plan_set = _update_plan_statement.executeUpdate();
              _customer_db.commit();
              System.out.println("New plan id: " +  pid);
            }
        }
    }

    public void transaction_list_plans() throws Exception {
        ResultSet listPlans = _all_rentals_statement.executeQuery();
        System.out.println("Plans: ");
        while(listPlans.next())
        {
           System.out.println("\t\t planid: " + listPlans.getInt(1) + " Plan Name: " + listPlans.getString(2) + " Price: " + listPlans.getInt(3)+ " Max Rentable Movies: " + listPlans.getInt(4));
        }
    }
    
    public void transaction_list_user_rentals(int cid) throws Exception {
        /* println all movies rented by the current user*/
        _list_user_rentals_statement.clearParameters();
        _list_user_rentals_statement.setInt(1, cid);
        ResultSet _rental_result = _list_user_rentals_statement.executeQuery();
        System.out.println("Rented Movies: ");
        while(_rental_result.next())
        {
	    String name = getMovieFromId(Integer.parseInt(_rental_result.getString(1)));
            System.out.println("\t\t" + _rental_result.getString(1) + " (" + name + ")");
        }
    }

    public void transaction_rent(int cid, int mid) throws Exception {
        /* rend the movie mid to the customer cid */
        /* remember to enforce consistency ! */        
        //Check if movieID is in currentRantals
        _rental_mid_statement.clearParameters();
        _rental_mid_statement.setInt(1, mid);
        _customer_db.setAutoCommit(false);
        ResultSet _result_rental = _rental_mid_statement.executeQuery();
        //If the movie is found in current rentals
        if(_result_rental.next())
        {
            System.out.println("Sorry, that movie is already rented!");
            _customer_db.rollback();
            return;
        }
        
        //Check that customer is not maxed on rentals
        if(helper_compute_remaining_rentals(cid) == 0)
        {
            System.out.println("You have no rentals available");
            _customer_db.rollback();
            return;
        }
        
        //Construct tuple: generate rentId and a timestamp
        java.util.Date utilDate = new java.util.Date();
        java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());
        int rentID = ((int)(sqlDate.getTime())) + mid;
        
        //Insert the tuple into the current rentals table
        _rent_statement.clearParameters();
        _rent_statement.setInt(1, rentID);
        _rent_statement.setInt(2, cid);
        _rent_statement.setInt(3, mid);
        _rent_statement.setDate(4, sqlDate);
        _rent_statement.executeUpdate();
        _customer_db.commit();
        System.out.println("Successfully rented movie with ID: " + mid);
    }

    public void transaction_return(int cid, int mid) throws Exception 
    {
        //Check if the user has actually rented the movie
        _rental_time_statement.clearParameters();
        _rental_time_statement.setInt(1, mid);
        ResultSet _result_rental = _rental_time_statement.executeQuery();
        if(!_result_rental.next())
        {
            System.out.println("You aren't currently renting this movie with Movie ID: " + mid + " so you cannot return it");
        }
        //If so, insert into the pastrentals and delete from the currentrentals
        else
        {
            java.util.Date utilDate = new java.util.Date();
            java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());
            _return_statement.clearParameters();
            _delete_statement.clearParameters();
            _return_statement.setInt(1,_result_rental.getInt(1));
            _return_statement.setInt(2,cid);
            _return_statement.setInt(3,mid);
            _return_statement.setDate(4,_result_rental.getDate(4));
            _return_statement.setDate(5, sqlDate);
            _delete_statement.setInt(1,mid);
            _delete_statement.executeUpdate();
            _return_statement.executeUpdate();
            System.out.println("Successfully returned movie with Movie ID: " + mid);
        }
    }

    public void transaction_fast_search(int cid, String movie_title)
            throws Exception 
    {
        /* like transaction_search, but uses joins instead of independent joins
           Needs to run three SQL queries: (a) movies, (b) movies join directors, (c) movies join actors
           Answers are sorted by mid.
           Then merge-joins the three answer sets */
        _search_statement.clearParameters();
        _search_statement.setString(1, '%' + movie_title + '%');   
        ResultSet movie_set = _search_statement.executeQuery();
        
        _director_join_statement.clearParameters();
        _director_join_statement.setString(1, '%' + movie_title + '%');   
        ResultSet director_set = _director_join_statement.executeQuery();
        
        _actor_join_statement.clearParameters();
        _actor_join_statement.setString(1, '%' + movie_title + '%');
        ResultSet actor_set = _actor_join_statement.executeQuery();
        boolean startDir = director_set.next();
        boolean startAct = actor_set.next();
        while(movie_set.next())
        {
            System.out.println("ID: " + movie_set.getInt(1) + " NAME " + movie_set.getString(2) + " YEAR: "  + movie_set.getString(3));
             if(startDir)
             {
                 while ((director_set.getString(1)).equals(movie_set.getString(1)))
                 {
                    System.out.println("\t\tDirector: " + director_set.getString(2) + " " + director_set.getString(3));  
                    if(director_set.isLast())
                    {
                        break;
                    }
                    director_set.next();
                }
            }
            System.out.println("\t\tActors: ");
            if(startAct)
            {
                while((actor_set.getString(1)).equals(movie_set.getString(1)))
                {
                    System.out.println("\t\t" + actor_set.getString(2) + " " + actor_set.getString(3));
                    if(actor_set.isLast())
                    {
                        break;
                    }
                    actor_set.next();
                }
            }
        }
    }
}
