# TopBloc Backend Code Challenge
Welcome to the TopBloc Backend Code Challenge! This challenge is designed to test your ability to write clean, well-documented, and efficient code. 
Your task is to write a program that will connect to a SQLite database, perform some basic data manipulation, and return the manipulated data as JSON via
an API connection.

## Requirements
- Java 11+ (This challenge was built with Java 11, 8 may work but is not guaranteed)
- A Java IDE (IntelliJ IDEA is recommended)
- A method to hit the API (Postman, cURL, etc.)


## Getting Started
1. Clone this repository to your local machine with `git clone https://github.com/TopBloc/backend-code-challenge.git`
2. Open the project in your IDE
3. Run the project via the `Main` class or with the run button in your IDE
4. Hit the API with your method of choice (Postman, cURL, etc.) at `localhost:4567/version` to ensure the project is running correctly.

## Challenge
At the start, this project has two classes: `Main` and `DatabaseManager`. `Main` is the location you will write any routes you
need to add, and `DatabaseManager` contains the code for connecting and interacting with the database.

The scenario of this application is as follows: You are a software engineer at a company that sells candy. Your team has been tasked with building a backend
for the companies new ECP (Enterprise Candy Planning) software. The scaffolding of the project and the database have already been built, and the frontend team
has the following feature requirements to complete their work:
- GET routes
  - Inventory routes that return the following:
    - All items in your inventory, including the item name, ID, amount in stock, and total capacity
    - All items in your inventory that are currently out of stock, including the item name, ID, amount in stock, and total capacity
    - All items in your inventory that are currently overstocked, including the item name, ID, amount in stock, and total capacity
    - All items in your inventory that are currently low on stock (<35%), including the item name, ID, amount in stock, and total capacity
    - A dynamic route that, when given an ID, returns the item name, ID, amount in stock, and total capacity of that item
  -  Distributor routes that return the following:
      - All distributors, including the id and name
      - A dynamic route that, given a distributors ID, returns the items distributed by a given distributor, including the item name, ID, and cost
      - A dynamic route that, given an item ID, returns all offerings from all distributors for that item, including the distributor name, ID, and cost
- POST/PUT/DELETE routes
  - Routes that allow you to:
    - Add a new item to the database
    - Add a new item to your inventory
    - Modify an existing item in your inventory
    - Add a distributor
    - Add items to a distributor's catalog (including the cost)
    - Modify the price of an item in a distributor's catalog
    - Get the cheapest price for restocking an item at a given quantity from all distributors
    - Delete an existing item from your inventory
    - Delete an existing distributor given their ID
- Challenges
  - Implement a route that streams database updates to a client over an open connection. Use any protocol you want to accomplish this.
  - Create a React front-end client that can query the API. Only basic functionality is all we're looking for here, a SPA with buttons and text boxes that execute API calls is fine.
  - Containerize the application(s) and add configuration to run it/them in Docker compose
  - Create an endpoint to export any table from the database in CSV format by passing the table name as a query parameter. 

## Considerations
- Feel free to go about this in any way you see fit. You can add any classes or methods you need, and you can modify classes and methods that aren't
explicitly marked with a comment saying not to modify.
- Any data returned from the API should be in JSON format unless otherwise specified. Responses that return no data can return anything, as long as it also returns a 200 on a success.
or a 400/500 on a failure. Bonus points for proper error messaging!
- For the Java API, avoid using external libraries that are not already included if possible. Some external libraries may be required for the first challenge problem.
- The database is already populated with some data, but feel free to add more if you need to.
- Any approach is valid. For example, no points will be taken off for doing business logic via Java if you are not particularly familiar with SQL.
Clean code and proper documentation are the most important things.
- Unit tests are not required, but are encouraged.
- The challenge problems are not required, but completing 2-3 of them will benefit your consideration.

## Submitting
1. Create a new repository within GitHub and name it as your favorite animal (ex. Sloth, Zebra)
2. Set the remote origin of this cloned project to your newly created GitHub repository:
3. git remote set-url --push origin https://github.com/<github_username>/<favorite_animal>
4. Push your completed code challenge!
