
# business.json				
Contains business data including location data, attributes, and categories.				
						
|column_name	|	type		|example		|description|
| :---         |   :----:         |    :----:    | ---:|
|business_id		|	string	|	tnhfDv5Il8EaGSXZGiuQGg	|	unique string business id                      |
|name			|	string	|	Garaje					|	 the business's name                               |
|address			|	string	|	475 3rd St				|	 the full address of the business|
|city			|	string	|	San Francisco			|	the city|
|state			|	string	|	CA						|	state code, if applicable|
|postal_code		|	string	|	94107					|	postal code|
|latitude		|	float	|	37.7817529521,			|	latitude|
|longitude		|	float	|	-122.39612197			|	longitude|
|stars			|	float	|	4,5						|	star rating, rounded to half-stars|
|review_count	|	integer	|	1198					|	number of reviews|
|is_open			|	int		|	1						|	0 or 1 for closed or open, respectively|
|attributes		|	object	|"attributes": {  "RestaurantsTakeOut": true, "BusinessParking": {"garage": false, "street": true, "validated": false, "lot": false, "valet": false},"	|	business attributes note: some attribute values might be objects|
|Categories	|	array	|	"[ "Mexican", "Burgers", "Gastropubs"],"	|	an array of strings of business categories|
hours	|	object	|	"": { "Monday": "10:00-21:00","Tuesday": "10:00-21:00", "Friday": "10:00-21:00", "Wednesday": "10:00-21:00", "Thursday": "10:00-21:00", "Sunday": "11:00-18:00", "Saturday": "10:00-21:00"}"	|	an object of key day to value hours, hours are using a 24hr clock|





# review.json
Contains full review text data including the user_id that wrote the review and the business_id the review is written for.
				
							
|column_name	|	type	|	example	|	description	|
| :---          |   :----:  |    :----: | ---:			|
|review_id		|string, 22 |	"zdSx_SD6obEhz9VrW9uAWA"	|	unique review id	|
|user_id		|string, 22 |	 "Ha3iJu77CxlrFm-vQRs_8g"	|	 unique user id, maps to the user in user.json	|
|business_id	|string, 22 |	tnhfDv5Il8EaGSXZGiuQGg	|	character business id, maps to business in business.json	|
|stars	|	int	|	4		|	 star rating	|
|date	|	string	|	3/9/2016	|	date formatted YYYY-MM-DD	|
|text	|	string	|	Great place to hang out after work: the prices are decent, and the ambience is fun. It's a bit loud, but very lively. The staff is friendly, and the food is good. They have a good selection of drinks	|	the review itself	|
|useful	|	int	|	0	|	number of useful votes received	|
|funny	|	int	|	0	|	number of funny votes received	|
|cool	|	int	|	0	|	number of cool votes received	|


# user.json

User data including the user's friend mapping and all the metadata associated with the user.



|column_name	|	type	|	example	|	description	|
| :---          |   :----:  |    :----: | ---:			|
|user_id	|	String 22	|	"Ha3iJu77CxlrFm-vQRs_8g"	|	unique user id, maps to the user in user.json	|
|name	|	String	|	"Sebastien",	|	the user's first name	|
|review_count	|	integer	|	56	|	 the number of reviews they've written	|
|yelping_since	|	String	|	2011-01-01	|	when the user joined Yelp, formatted like YYYY-MM-DD	|
|friends	|	array of strings	|	"["wqoXYLWmpkEH0YvTmHBsJQ", "KUXLLiJGrjtSsapmxmpvTA", "6e9rJKQC3n0RSKyHLViL-Q" ]"	|	 an array of the user's friend as user_ids	|
|useful	|	integer	|	21	|	 number of useful votes sent by the user	|
|funny	|	integer	|	88	|	number of funny votes sent by the user	|
|cool	|	integer	|	15	|	number of cool votes sent by the user	|
|fans	|	integer	|	1032	|	 number of fans the user has	|
|elite	|	array of integers	|	"[2012,2013 ]"	|	the years the user was elite	|
|average_stars	|	float	|	4.31	|	average rating of all reviews	|
|compliment_hot	|	integer	|	339	|	 number of hot compliments received by the user	|
|compliment_more	|	integer	|	668	|	number of more compliments received by the user	|
|compliment_profile	|	integer	|	42	|	 number of profile compliments received by the user	|
|compliment_cute	|	integer	|	62	|	number of cute compliments received by the user	|
|compliment_list	|	integer	|	37	|	 number of list compliments received by the user	|
|compliment_note	|	integer	|	356	|	number of note compliments received by the user	|
|compliment_plain	|	integer	|	68	|	 number of plain compliments received by the user	|
|compliment_cool	|	integer	|	91	|	 number of cool compliments received by the user	|
|compliment_funny	|	integer	|	99	|	number of funny compliments received by the user	|
|compliment_writer	|	integer	|	95	|	 number of writer compliments received by the user	|
|compliment_photos	|	integer	|	50	|	number of photo compliments received by the user	|
