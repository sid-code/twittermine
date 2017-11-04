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

puts "Twitter client initialized"


print "Determining initial output file... "
index = 0
while File.exists?(outpat % index)
  index += 1
end
puts "#{outpat % index}"

splitsize = 500

tweets = []

count = 0
puts "Starting twitter stream.\nLooking for topics:"
puts "  #{topics.join("\n  ")}"

client.filter(track: topics.join(",")) do |object|
  count += 1
  if object.is_a?(Twitter::Tweet)
    tweets << object
  end

  if count > splitsize
    outname = outpat % index
    File.open(outname, "wb") do |f|
      f.write(Marshal.dump(tweets))
      puts "Wrote file #{outname}, moving on"
    end
    tweets.clear
    count = 0
    index += 1
  end
end
