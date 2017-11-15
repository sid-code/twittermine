require "sqlite3"
require "twitter"

DB_FILE = "./tweets.db"
def init_tweets_db(file = DB_FILE)
  db = SQLite3::Database.new(DB_FILE)
  db.execute(<<-SQL
             CREATE TABLE IF NOT EXISTS tweets(
               id          INTEGER PRIMARY KEY,
               text        CHARACTER(140),
               timestamp   INTEGER,
               userid      INTEGER,
               useful      INTEGER)
             SQL
            )
  db.execute(<<-SQL
             CREATE TABLE IF NOT EXISTS users(
               userid      INTEGER PRIMARY KEY,
               name        VARCHAR(20),
               handle      VARCHAR(20))
             SQL
            )

  return db
end


INSERT_TWEET_SQL = "INSERT OR REPLACE INTO tweets(id, text, timestamp, userid, useful) VALUES(?, ?, ?, ?, 0)"
INSERT_USER_SQL  = "INSERT OR REPLACE INTO users(userid, name, handle) VALUES(?, ?, ?)"
def insert_tweet(db, tweet)
  id = tweet.id
  text = tweet.text
  timestamp = tweet.created_at.to_i
  userid = if tweet.user? then tweet.user.id else 0 end

  db.execute(INSERT_TWEET_SQL, [id, text, timestamp, userid])

  if tweet.user?
    db.execute(INSERT_USER_SQL, [userid, tweet.user.name, tweet.user.screen_name])
  end
end


# only if run directly
if __FILE__ == $0
  db = init_tweets_db

  files = Dir["data/*.dat"]
  tweets = files.each do |f|
    print "Processing #{f}"
    print "  reading..."
    tweets = Marshal.load(File.binread(f))

    print "  inserting..."
    tweets.each do |t|
      insert_tweet(db, t)
    end

    print "  done.\n"
  end
end
