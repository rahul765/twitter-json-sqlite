## Download the data and put in a sqlite database

#There are 113 `tar.gz` files in the folder. In this section, you are asked to process all files into the data set. There can be several strategies, such as using the methods you have already learned in the previous assignments (c.f. `parseTweets`), or you can try an R-package **readtext**. 

### Create a database

#install.packages('dbplyr')
#install.packages('DBI')
#install.packages('readtext')
#install.packages('streamR')
#install.packages('R.utils')
#install.packages('rjson')
#install.packages('ldply')
#install.packages('plyr')
#install.packages('gtools')
#install.packages('RSQLite')
#install.packages('quanteda')
#install.packages('ggplot2')
#install.packages('data.table')
install.packages("devtools")
#install.packages('gtools')
install_github("ChandlerLutz/crossplotr")


library(devtools)
library(quanteda)
library(dbplyr)
library(DBI)
library(ggplot2)
library(readtext)
library(streamR)
library(R.utils)
library(rjson)
library(data.table)
library(plyr)
library(gtools)
library("RSQLite")

########################################
#initial database
con <- DBI::dbConnect(RSQLite::SQLite(), "twitter.sqlite" )
#adding all files path in to a variable
filenames <- list.files(path = "tweets/", full.names=TRUE)


#********
#Extract all the tar.gz files and unpack the second file store it in

tweets.df = data.frame()
for (i in 1:113){
  a = untar(filenames[i],list = TRUE)
  tweets <- parseTweets(a[2],simplify = TRUE, verbose = TRUE, legacy = FALSE)
  tweets.df <- rbind(tweets.df,tweets)
}

# dropping duplicate rows acording to tweet Id
tweets.df = tweets.df[!duplicated(tweets.df$id_str),]
# taking care of date format
#tweets.df$created_at = strptime(tweets.df$created_at, "%a %b %d %H:%M:%S %z %Y", tz = "")
# Storing all tweets data in a csv file
write.table(tweets.df, "tweets.csv", col.names=TRUE, sep=",", append=TRUE,row.names = FALSE)


# Dataframe for users
users.df <- data.frame()
for (g in 1:113){
  #convert json content to a list
  a = untar(filenames[g],list = TRUE)
  json_raw <- fromJSON(file = a[2])
  print(length(json_raw$user))
  k <- data.frame(t(sapply(json_raw$user,c)))
  print(class(k))
  print(class(users.df))
  users.df <- smartbind(users.df,k)
}

users.df = users.df[!duplicated(users.df$id),]


# To create a sample data csv
write.table(users.df, "users.csv", col.names = TRUE, sep=",", append=TRUE,row.names = FALSE)



##Create two tables and import the data (15 points)

#'''Each JSON file includes both twitter information and user information. 
#You will separate the fields and create two tables: `tweets` and `users`. 
#When you include the data, make sure that there is no duplicates in the `users` and `tweets` table 
#(i.e. `tweet_id` is unique in tweets table, and `user_id` is unique in `users` table). 
#Also make sure that these two tables can be combined afterwards using a field common to two tables.
#Part of `candidate_tweet_data_pset8.rda` can be in the `users` for the future analysis purpose 
#(at least you need to know which accounts are parties and candidates).'''


#Creating tweets table
dbWriteTable(con, 'tweets', value = tweets.df, row.names = pkgconfig::get_config("RSQLite::row.names.table",FALSE), overwrite = TRUE, append = FALSE, field.types = NULL,temporary = FALSE)
# creating users table
dbWriteTable(con, 'users', value = users.df, row.names = pkgconfig::get_config("RSQLite::row.names.table",FALSE), overwrite = FALSE, append = FALSE, field.types = NULL,temporary = FALSE)


#######Quesions
# Getting all the Tables in Database
alltables = dbListTables(con)

#Q1. How many tweets are in the `tweets` table?
num_tweets = dbGetQuery( con,'select count(*) from tweets' )
print(num_tweets)

#How many unique users?
unique_users = dbGetQuery(con,'select COUNT(DISTINCT id) from users')
print(unique_users)





### Description of the data (21 points, 3 per question)

#Answer the following questions (again try to do most aggregation in the query, not in R):
  
#  1. Which screen_name has the highest count of tweets?(in user)
#  2. Who has the highest number of followers? 
#  3. Among politicians, who has the highest number of followers ?
#  4. Which tweet has the earliest timestamp in the data? Which is the latest?
#  5. Who were the top ten most replied account? How many times?
#  6. How many tweets with the word brexit? 
#  7. How many tweets have geolocation information (`lat` or `lon` value)? (You'd be surprised how small the number is.)

# Solution 1

highest_tweet_count = dbGetQuery(con,'select screen_name, count(*) from tweets GROUP BY screen_name having count(*)>1 ORDER BY count(*) DESC LIMIT 1')
print(highest_tweet_count)

# Solution 2

highest_followers = dbGetQuery(con,"select name, MAX(followers_count) from users")
print(highest_followers)

# Solution 3
### How to classify the user as Politician ??
highest_politician_followers = dbGetQuery(con,"select name, MAX(followers_count) from users WHERE verified='FALSE'")
print(highest_politician_followers)

# Solution 4 

earliest_timestamp = dbGetQuery(con,'select text, MIN(created_at) as "Earliest timestamp" from tweets')
print(earliest_timestamp)

latest_timestamp = dbGetQuery(con,'select text, MAX(created_at) as "Latest timestamp" from tweets')
print(latest_timestamp)

# Solution 5

top_10 = dbGetQuery(con,'select in_reply_to_screen_name, count(*) from tweets where in_reply_to_screen_name<>"<NA>" GROUP BY in_reply_to_screen_name having count(*)>1 ORDER BY count(*) DESC LIMIT 10')
print(top_10)


# Solution 6

brexit_tweet = dbGetQuery(con,'select count(*) from tweets where text LIKE "%brexit%"')
print(brexit_tweet)

# Solution 7

geoloc_enabled = dbGetQuery(con,"select count(*) as 'Geolocation ENABLED' from tweets where geo_enabled = '1'")
print(geoloc_enabled)



### Hashtags and mentions (15 points)

#We can analyze twitter hashtags and mentions pretty easilty with `quanteda`. 
#Read https://quanteda.io/articles/pkgdown/examples/twitter.html and answer following questions.
#For creating dfm, you need to create a corpus from a dataframe you created from a sql database 
#(it is like `dbGetQuery() %>% corpus() %>% dfm()`.
                                                                                                
# 1. What are the top 10 popular hashtags? How many of them shows up?
# 2. In the network of hashtags, what do you find?
# 3. Who was mentioned (i.e. '@name') the most? How the mentions and hashtags are related?
# 4. Create a `hashtag` from where there are two fields (`id_str`, `hashtag`).  You can use only top 50 hashtags. The example entary of the table looks like this (HINT: the most efficient way I could think of is to use `quanteda::docvars`, `quanteda::convert`, and `tidyr::gather`).


# Essentials before resuming to solution
text = dbGetQuery(con,"select text from tweets")
myCorpus<-corpus(text)
tweet_dfm <- dfm(myCorpus, remove_punct = TRUE)
head(tweet_dfm)


#Solution 1
tag_dfm <- dfm_select(tweet_dfm, pattern = ("#*"))
toptag <- names(topfeatures(tag_dfm, 50))
head(toptag,10)

# Solution 2
tag_fcm <- fcm(tag_dfm)
head(tag_fcm)
topgat_fcm <- fcm_select(tag_fcm, pattern = toptag)
textplot_network(topgat_fcm, min_freq = 0.1, edge_alpha = 0.8, edge_size = 5)
# We can see that the main emphasis of election is inclined toward labour


# Solution 3
user_dfm <- dfm_select(tweet_dfm, pattern = "@*")
topuser <- names(topfeatures(user_dfm, 50))
head(topuser,1)
user_fcm <- fcm(user_dfm)
head(user_fcm)
user_fcm <- fcm_select(user_fcm, pattern = topuser)
textplot_network(user_fcm, min_freq = 0.1, edge_color = "orange", edge_alpha = 0.8, edge_size = 5)

# Solution 4

#Mentioned data not provided
#load('sample_hashtag_table_entry.rda')
#sample_hashtag_table_entry %>% knitr::kable()



### Timesries and cross-sectional plot (12 points)

#Think about an interesting aggregation of tweets chronologically and draw figures (more than one). You can aggregate the number of tweets by day plus another criteria (e.g. party? hashtags?). Construct a story from the data (preferably that has both time and cross-sectional (e.g. regional/party etc) demension), and support it with table(s) and figure(s).

# Timeseries plot
# Plot 1
ggplot(tweets.df, aes(tweets.df$created_at, tweets.df$favorite_count)) + geom_line() + geom_line(color = "#00AFBB", size = 2)
# Plot 2
ggplot(tweets.df, aes(tweets.df$created_at, tweets.df$retweet_count)) + geom_line() + geom_line(color = "red", size = 2)

# Cross-sectional Plot
crossplot(tweets.df, x.var = tweets.df$retweet_count, y.var = tweets.df$lang, size.var = tweets.df$verified,
          shapes.var = tweets.df$time_zone )




