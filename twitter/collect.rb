require "twitter"
require "./todb.rb"

db = init_tweets_db

keys = IO.readlines("./keys.txt").map(&:strip)
topics = IO.readlines("./terms.txt").map(&:strip)
splitsize = 500
tweets = []
count = 0

client = Twitter::Streaming::Client.new do |config|
  config.consumer_key         = keys[0]
  config.consumer_secret      = keys[1]
  config.access_token         = keys[2]
  config.access_token_secret  = keys[3]
end

puts "Twitter client initialized"

puts "Starting twitter stream.\nLooking for topics:"
puts "  #{topics.join("\n  ")}"

client.filter(track: topics.join(",")) do |object|
  count += 1
  if object.is_a?(Twitter::Tweet)
    insert_tweet(db, object)
  end
end
