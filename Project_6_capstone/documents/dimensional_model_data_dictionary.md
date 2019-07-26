
# Category 

Contains distinct categories in business.json			
						
|column_name	|	type	|description|
| :---          |   :----:  | ---:|
|category_id	|	int	    |unique id|
|cateory		|	string	|the business's category|


# Ambience 

Contains distinct ambiences in business.json			
						
|column_name	|	type	|description|
| :---          |   :----:  | ---:|
|ambience_id	|	int	    |unique id|
|ambience		|	string	|the business's ambience|


# State 

Contains distinct states in business.json			
						
|column_name	|	type	|description|
| :---          |   :----:  | ---:|
|state_id		|	int	    |unique id|
|state			|	string	|states|

# Users

|column_name		|	type	|description	|
| :---          	|   :----:  | ---:			|
|user_id			|	String 	|	unique user id, maps to the user in user.json	|
|name				|	String	|	the user's first name	|
|user_review_count	|	integer	| the number of reviews they've written	|
|yelping_since		|	String	|when the user joined Yelp, formatted like YYYY-MM-DD	|
|useful_vote_cnt	|	integer	|number of useful votes sent by the user	|
|funny_vote_cnt		|	integer	|number of funny votes sent by the user	|
|cool_vote_cnt		|	integer	|number of cool votes sent by the user	|
|user_fans			|	integer	|number of fans the user has	|
|user_elite_year_cnt|	integer	|the length of elite column in user.json|
|is_user_elite      | boolean   |it is 'True' if the user is elite in current year|
|user_friend_count  |integer    |the lenght of friends column in user.json file|
|user_average_stars	|	float	|average rating of all reviews	|


# business_Reviews

Contains IDs from dimensions and aggregations from business.json, user.json and review.json.

|column_name		|	type	|description									|
| :---          	|   :----:  | ---:											|
|review_id 			| varchar   | unique review id 								|
|business_id		| varchar	| unique business id 							|
|category_id		| integer	| unique category id, maps to category table 	|
|user_id 			| varchar	| unique user id, maps to users table 			|
|ambience_id		| integer	| unique ambience id, maps to ambience table 	|
|state_id 			| integer	| unique state id, maps to state table 			|
|review_cool		| numeric	| number of cool votes received 				|
|review_funny		| numeric	| number of funny votes received 				|
|review_stars		| float	    | star rating of the review 					|
|review_useful		| numeric	| number of useful votes received 				|
|year 				| integer	| year of the review 							|
|month              | integer	| month of the review 							|
|is_open			| numeric	| 0 if the business is closed,1 open            |
|review_count 		| numeric	|total review count of the business 			|
|business_stars     | float	    | star rating of the business 					|
|takeCreditCards    | boolean	|True if the business takes creditcards			|
|goodForKids        | boolean	|True if the business is good for kids			|
|happyHour          | boolean	|True if the business has hapyhour menu			|
|restaurantsTakeOut | boolean	|True if the business has take out				|
|smoking            | varchar	|smoking options in the business, can be yes,no,outdoor etc|
|wifi               | varchar	|can be free,paid or no							|
|Open24Hours        | v			|v 												|






