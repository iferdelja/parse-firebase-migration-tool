var PropertiesReader = require('properties-reader');
var properties = PropertiesReader('properties');

Parse.Cloud.define("migrate", function(request, response) {
	var query = new Parse.Query(properties.get("parseEntityName"));
	query.limit(1000);
	query.ascending("updatedAt");
	query.find({
		  success: function(parseEntities) {
		  	console.log("Will migrate " + parseEntities.length + " entities.");

		  	// This returns PFObjects (Parse.Object)
		  	var geoFireUtils = require("cloud/geoFireUtils.js");
		  	var pushIdGenerator = require("cloud/firebasePushIdGenerator.js");

		  	var newEntities = new Object();
		  	var users = new Object();
		  	var newEntityLocations = new Object();

		  	var parseEntity;
		  	var firebaseEntity;
		  	for (var i = 0; i < parseEntities.length; i++) {
		  		parseEntity = parseEntities[i];
		  		firebaseEntity = prepareNewEntity(parseEntity);
		  		if (users[firebaseEntity.userId] === undefined) {
		  			users[firebaseEntity.userId] = new Object();
		  			users[firebaseEntity.userId].patches = new Array();
		  		}
		  		var adjustedEntityId = pushIdGenerator.generatePushId(parseEntity.get("createdAt"));

		  		users[firebaseEntity.userId].patches.push(adjustedEntityId);
		  		newEntities[adjustedEntityId] = firebaseEntity;
		  		
		  		var location = [parseEntity.get("location").latitude, parseEntity.get("location").longitude];
		  		var geohash = geoFireUtils.encodeGeohash(location, undefined);
		  		newEntityLocations[adjustedEntityId] = geoFireUtils.encodeGeoFireObject(location, geohash);
		  	}

		  	var saveLocations = function() {
		  		saveLocationsToFirebase(newEntityLocations, function(param) {
				    	console.log("Migrated locations.");
				    	response.success();
				    }, function(param){
				    	console.log("Location migration failed " + param);
				    	response.error(param);
				    });
		  	}

		  	var saveUsers = function() {
		  		saveUsersToFirebase(users, function(param) {
			    	console.log("Migrated users.");
					saveLocations();			    	
			    }, function(param){
			    	console.log("User migration failed " + param);
			    	response.error(param);
			    });
		  	}

		    var saveEntities = function() {
		    	saveEntitiesToFirebase(newEntities, function(param) {
		    		console.log("Migrated " + newEntities.length + " entities.");
		    		saveUsers();
		    	}, function(param){
		    		console.log("Migration failed " + param);
		    		response.error(param);
		    	});
		    }

		    saveEntities();
		  },

		  error: function(error) {
		  	console.log(error);
		    response.error(error);
		  }
			});
});



function prepareNewEntity(parseEntity) {
	var newEntity = new Object();
	newEntity.userId = parseEntity.get("userId");
	newEntity.text = parseEntity.get("text");
	newEntity.imageProvidedByUser = !parseEntity.get("hasPlaceholderImage");
	newEntity.locationDescription = parseEntity.get("locationDescription");
	newEntity.voteCount = parseEntity.get("numberOfVotes");
	
	var createdAtDate = new Date(parseEntity.get("createdAt"));
	newEntity.createdAt = createdAtDate.getTime();
	
	var updatedAtDate = new Date(parseEntity.get("updatedAt"));
	newEntity.updatedAt = updatedAtDate.getTime();

	var imageObj = parseEntity.get("image");
	if (imageObj != undefined) {
		newEntity.imageUrl = parseEntity.get("image")["_url"];
		newEntity.thumbnailUrl = parseEntity.get("thumbnailImage")["_url"];
	}
	return newEntity;
}

function saveEntitiesToFirebase(entities, success, failure) {
  Parse.Cloud.httpRequest({
  	method: 'PUT',
    url: properties.get("firebaseEntitiesUrl"),
    headers: {
    'Content-Type': 'application/json;charset=utf-8'
  	},
  	body:  entities,
    success: function(httpResponse) {
      //console.log(httpResponse.text);
      success(httpResponse.status);
    },
    error: function(httpResponse) {
    //console.log(httpResponse.text);
      console.log('Request failed with response code ' + httpResponse.status + httpResponse.text);
      failure(httpResponse.status);
    }
  });
};

function saveUsersToFirebase(users, success, failure) {
  Parse.Cloud.httpRequest({
  	method: 'PUT',
    url: properties.get("firebaseUsersUrl"),
    headers: {
    'Content-Type': 'application/json;charset=utf-8'
  	},
  	body:  users,
    success: function(httpResponse) {
      //console.log(httpResponse.text);
      success(httpResponse.status);
    },
    error: function(httpResponse) {
    //console.log(httpResponse.text);
      console.log('Request failed with response code ' + httpResponse.status + httpResponse.text);
      failure(httpResponse.status);
    }
  });
};

function saveLocationsToFirebase(locations, success, failure) {
  Parse.Cloud.httpRequest({
  	method: 'PUT',
    url: properties.get("firebaseEntityLocationsUrl"),
    headers: {
    'Content-Type': 'application/json;charset=utf-8'
  	},
  	body:  locations,
    success: function(httpResponse) {
      //console.log(httpResponse.text);
      success(httpResponse.status);
    },
    error: function(httpResponse) {
    //console.log(httpResponse.text);
      console.log('Request failed with response code ' + httpResponse.status + httpResponse.text);
      failure(httpResponse.status);
    }
  });
};


