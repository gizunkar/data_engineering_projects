CREATE TABLE IF NOT EXISTS public.business_reviews(
business_id varchar,
 is_open numeric,
 review_count numeric,
 business_stars float,
 alcohol varchar,
 take_credit_cards boolean,
 good_for_kids boolean,
 happy_hour boolean,
 restaurants_takeout boolean,
 smoking varchar,
 wifi varchar,
 category_id integer,
 ambience_id integer,
 state_id integer,
 review_cool numeric,
 review_funny numeric,
 review_id varchar,
 review_stars float,
 review_useful numeric,
 user_id varchar,
 year integer,
 month integer
);


 CREATE TABLE IF NOT EXISTS public.category(
 restaurant_category varchar,
 category_id integer
 );

 CREATE TABLE IF NOT EXISTS public.ambience(
 ambience varchar,
 ambience_id integer
);

 CREATE TABLE IF NOT EXISTS public.state(
  state varchar,
 state_id integer
 );



 CREATE TABLE IF NOT EXISTS public.users(
 user_id varchar,
 name varchar,
 user_review_count numeric,
 yelping_since varchar,
 user_friend_count integer,
 useful_vote_cnt numeric,
 funny_vote_cnt numeric,
 cool_vote_cnt numeric,
 user_average_stars float,
 user_elite_year_cnt integer,
 is_user_elite boolean,
 user_fans numeric
 );



--TRUNCATE TABLE public.category;
--
--TRUNCATE TABLE public.ambience;
--
--TRUNCATE TABLE public.state;
--
--TRUNCATE TABLE public.user;
--
--TRUNCATE TABLE public.business_reviews;