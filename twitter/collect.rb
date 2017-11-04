require "twitter"

keys = IO.readlines("./keys.txt").map(&:strip)
topics = IO.readlines("./terms.txt").map(&:strip)
outpat = "./data/tweets%d.dat"

client = Twitter::Streaming::Client.new do |config|
  config.consumer_key         = keys[0]
  config.consumer_secret      = keys[1]
  config.access_token         = keys[2]
  config.access_token_secret  = keys[3]
end

index = 0
while File.exists?(outpat % index)
  index += 1
end

splitsize = 100

tweets = []

count = 0
client.filter(track: topics.join(",")) do |object|
  count += 1
  if object.is_a?(Twitter::Tweet)
    tweets << object
  end

  if count > splitsize
    File.open(outpat % index, "wb") do |f|
      f.write(Marshal.dump(tweets))
    end
    tweets.clear
    count = 0
    index += 1
  end
end
