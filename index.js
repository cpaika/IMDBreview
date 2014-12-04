var express = require("express");
var app = express();
var bodyParse = require('body-parser');
var util = require("util");
var exec = require("child_process").exec
var fs = require("fs");
var server = require('http').Server(app);
var io = require('socket.io')(server);
const webAddress = "http://71.234.176.156:8888/";
var cookieParser = require("cookie-parser");
app.use(cookieParser());
app.use(bodyParse.json());       // to support JSON-encoded bodies
app.use(bodyParse.urlencoded({extended: true})); //to support URL encoded bodies

app.get("/", function(request, response)
{
	console.log(request.connection.remoteAddress + ": Connection to login page received");
	fs.readFile("login.html", function(error,data)
	{
		if(error)
		{
			response.writeHead(404);
			response.write("Oops this page doesn't exist - 404");
		}
		else
		{
			response.writeHead(200, {"Content-Type":"text/html"});
			response.write(data,"utf-8");
		}
	response.end();
	});
});

app.get("/search", function(request,response)
{
	console.log(request.connection.remoteAddress + ": Connection to search page received");
	fs.readFile("test.html", function(error,data)
	{
		if(error)
		{
			response.writeHead(404);
			response.write("Oops this page doesn't exist - 404");
		}
		else
		{
			response.writeHead(200, {"Content-Type":"text/html"});
			response.write(data,"utf-8");
		}
	displayUserInfo(request, response);
	response.end();
	});
});

app.post("/return", function(request, response)
{
	displayUserInfo(request, response);
	console.log(request.connection.remoteAddress + ": Connection to return page received");
	fs.readFile("test.html", function(error,data)
	{
		if(error)
		{
			response.writeHead(404);
			response.write("Oops this page doesn't exist - 404");
		}
		else
		{
			response.writeHead(200, {"Content-Type":"text/html"});
			response.write(data,"utf-8");
		}
	});
	var returnId = request.body.returnid;
	console.log(request.connection.remoteAddress + ": Received a request to return movie with id " + returnId);
	var data = request.cookies.auth;
	var username = data.username;
	var password = data.password;
	console.log(request.connection.remoteAddress + ": Command is ---> java VideoStore " + username + " " + password + " return " + returnId);
	var child = exec("java VideoStore " + username + " " + password + " return " + returnId,
	function (error, stdout, stderr) 
	{
		if(error == null)
		{
			if(stdout == "")
			{
				console.log(request.connection.remoteAddress + ": Cannot return movie");
				response.write("Cannot return movie");
			}
			else
			{
				console.log(request.connection.remoteAddress + ": Successfully returned movie");
				response.write(stdout);
			}
		}
	    	else
		{
      			console.log('exec error: ' + error);
    		}
		console.log(request.connection.remoteAddress + ": Closing return response");
		response.end();
	});
});

app.post("/rent", function(request, response)
{
	displayUserInfo(request, response);
	console.log(request.connection.remoteAddress + ": Connection to rent page received");
	fs.readFile("test.html", function(error,data)
	{
		if(error)
		{
			response.writeHead(404);
			response.write("Oops this page doesn't exist - 404");
		}
		else
		{
			response.writeHead(200, {"Content-Type":"text/html"});
			response.write(data,"utf-8");
		}
	});
	var movieId = request.body.movieid;
	console.log(request.connection.remoteAddress + ": Received a request to rent movie with id " + movieId);
	var data = request.cookies.auth;
	var username = data.username;
	var password = data.password;
	console.log(request.connection.remoteAddress + ": Command is ---> java VideoStore " + username + " " + password + " rent " + movieId);
	var child = exec("java VideoStore " + username + " " + password + " rent " + movieId,
	function (error, stdout, stderr) 
	{
		if(error == null)
		{
			if(stdout == "")
			{
				console.log(request.connection.remoteAddress + ": Searched for movie and found NOTHING");
				response.write("No Results Found for " + searchTerm);
			}
			else
			{
				console.log(request.connection.remoteAddress + ": Successfully rented movie");
				response.write(stdout);
			}
		}
	    	else
		{
      			console.log('exec error: ' + error);
    		}
		console.log(request.connection.remoteAddress + ": Closing rent response");
		response.end();
	});
});

app.post("/search", function(request, response)
{
	displayUserInfo(request, response);
	console.log(request.connection.remoteAddress + ": Connection to search page received");
	fs.readFile("test.html", function(error,data)
	{
		if(error)
		{
			response.writeHead(404);
			response.write("Oops this apge doesn't exist - 404");
		}
		else
		{
			response.writeHead(200, {"Content-Type":"text/html"});
			response.write(data,"utf-8");
		}
	});
	console.log(request.connection.remoteAddress + ": Received a request to search for movie " + request.body.searchterm);
	var searchterm = request.body.searchterm;
	var data = request.cookies.auth;
	var username = data.username;
	var password = data.password;
	console.log(request.connection.remoteAddress + ": Command is ---> java VideoStore " + username + " " + password + " fastsearch " + searchterm);
	var child = exec("java VideoStore " + username + " " + password + " fastsearch " + searchterm,
	function (error, stdout, stderr) 
	{
		if(error == null)
		{
			if(stdout == "")
			{
				console.log(request.connection.remoteAddress + ": Searched for movie and found NOTHING");
				response.write("No Results found");
			}
			else
			{
				console.log(request.connection.remoteAddress + ": Searched for movie and received results");
				var lines = stdout.toString().split('\n');
				var result = "";
				lines.forEach(function(line)
				{
					if(line.charAt(0) == "I" && line.charAt(1) == "D")
					{
						result = result + "<br>";
					}
					result = result + "<br>" + line;
				});
				response.write(result);
			}
		}
	    	else
		{
      			console.log('exec error: ' + error);
    		}
		console.log(request.connection.remoteAddress + ": Closing search response");
		response.end();
	});
});


app.post("/login", function(request, response)
{
	fs.readFile("login.html", function(error,data)
	{
		if(error)
		{
			response.writeHead(404);
			response.write("Oops this page doesn't exist - 404");
		}
		else
		{
			response.writeHead(200, {"Content-Type":"text/html"});
			response.write(data,"utf-8");
		}
	});
	console.log(request.connection.remoteAddress + ": Received a post request for a login");
	var username = request.body.username;
	var password = request.body.password;
	console.log(request.connection.remoteAddress + ": Username: " + username + " Password: " + password);
	response.cookie('auth', {"username": username, "password": password}, {maxAge:9000000, httpOnly:true});
	console.log(request.connection.remoteAddress + ": Created a cookie");
	var child = exec("java VideoStore " + username + " " + password, function (error, stdout, stderr) 
	{
		if(stdout == -1)
		{
			console.log("LOGIN ATTEMPT BY USER " + username + " FAILED");
			response.write('<div style="text-align:center;color:#ff0033; "> User Authentication failed, please try again </div>');
		}
		if(stdout == 1)
		{
			console.log(request.connection.remoteAddress + ": User is authenticated!");
			response.write('<div style="text-align:center;">Successfully authenticated!<br><a href="http://71.234.176.156:8888/search"> Click here to proceeed </a></div>');
		}
	    	if (error !== null) 
		{
      			console.log('exec error: ' + error);
    		}
	response.end();
	});
	
});

var server = app.listen(8888, function()
{
	var host = server.address().address;
	var port = server.address.port;

	console.log("IMDB app listening at http://%s:%s", host, port);
});
io.listen(server);

function displayUserInfo(request, response)
{
	if(request.cookies.auth == undefined)
	{
		return;
	}
	var child = exec("java VideoStore " + request.cookies.auth.username + " " + request.cookies.auth.password + " return", function (error, stdout, stderr)
	{
		if (error !== null) 
		{
      			console.log('exec error: ' + error);
    		}
		else
		{
			var lines = stdout.toString().split('\n');
			var result = "";
			lines.forEach(function(line)
			{
				result = result + "<br>" + line;
			});
			var info = '<div id="data"> ' + 'Hello ' + request.cookies.auth.username + result;
			var child = exec("java VideoStore " + request.cookies.auth.username + " " + request.cookies.auth.password + " left", function (error2, stdout2, stderr2)
			{
				if(error == null){response.write(info);response.write("<br>" + stdout2+ ' </div>');}
			});
			
		}
	});
}

	
