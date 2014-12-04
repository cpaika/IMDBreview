


CREATE TABLE RentalPlans(planID int primary key, name varchar(30), price float, rentalMax int);
INSERT into RentalPlans values(1, 'Basic', 9.99, 5);
INSERT into RentalPlans values(2, 'Premium', 19.99, 10);
INSERT into RentalPlans values(3, 'Platinum', 24.99, 15);
INSERT into RentalPlans values(4, 'MovieFreak', 49.99, 100);

CREATE TABLE Customers(custID int primary key, fname varchar(30), lname varchar(30), username varchar(15), password varchar(15), email varchar(30), streetAddress varchar(30), cityName varchar (30), state varchar(2), zipCode int, planID int references RentalPlans(planID));
--primary key is custID
--foreign key is planID, which references the current rental plan that the customer is subscribed to
--a "Subscription" entity was originally created, but since there is a many to one relationship between Customers and RentalPlans, it makes more sense for "Subscription" to simply be an attribute "planID" of the Customer relation. This also enforces that a customer may only have up to one plan at a time. 
-- It will also be necessary to store payment information for customers in the future, but since it is not mentioned specifically in the directions, we are assuming this detail will be handled in Part 2.

INSERT into Customers values(1234, 'John', 'Smith', 'jsmith1', 'apple4!', 'jsmith1@gmail.com', '6 Main Street', 'Amherst', 'MA', 01002, 1);
INSERT into Customers values(1546, 'Aly', 'Johnson', 'ajohnson','cat13', 'ajohnson@gmail.com', '18 Mulberry Street', 'Northampton', 'MA', 01060, 4);
INSERT into Customers values(5643, 'Justin', 'Thomas', 'jthomas', 'baseball56', 'jthomas@gmail.com', '55 Bridge Street', 'Beverly', 'MA', 01915, 2);

CREATE TABLE currentRentals(rentID int unique, custID int references Customers(custID), movieID int primary key, dateRented date);
-- primary key is movieid so that only one movie may be rented at a time
-- rentID attribute with a UNIQUE constraint exists so that each rental can be recorded by the database and later stored in "pastRentals"

INSERT into currentRentals values(00010, 1234, 35, '2014-10-13');
INSERT into currentRentals values(00234, 1546, 534443, '2014-10-13');
INSERT into currentRentals values(00013, 5643, 235391, '2014-10-13');

CREATE TABLE pastRentals(rentID int primary key, custID int references Customers(custID), movieID int, dateRented date, dateReturned date);
-- this relation is created so that once rentals are returned, a history is maintained
-- this information would be useful and have practical uses
INSERT into pastRentals values(00009, 1234, 33215, '2013-10-01', '2013-10-02');
INSERT into pastRentals values(00056, 1546, 14939, '2013-10-04', '2013-10-05');
INSERT into pastRentals values(00007, 5643, 6947, '2013-10-05', '2013-10-07');


                                                                              
