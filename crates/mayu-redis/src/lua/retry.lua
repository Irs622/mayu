-- KEYS[1]=stream KEYS[2]=delayed
-- ARGV[1]=group ARGV[2]=entry id ARGV[3]=eta ms ARGV[4]=delayed member
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
return 1
