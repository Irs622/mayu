-- KEYS[1]=stream KEYS[2]=result key
-- ARGV[1]=group ARGV[2]=entry id ARGV[3]=store result ('1'/'0') ARGV[4]=record ARGV[5]=ttl secs
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
if ARGV[3] == '1' then
  redis.call('SET', KEYS[2], ARGV[4], 'EX', ARGV[5])
end
return 1
