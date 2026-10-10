-- KEYS[1]=stream KEYS[2]=dead stream KEYS[3]=result key
-- ARGV[1]=group ARGV[2]=entry id ARGV[3]=header ARGV[4]=payload ARGV[5]=error
-- ARGV[6]=store result ('1'/'0') ARGV[7]=record ARGV[8]=ttl secs
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
redis.call('XADD', KEYS[2], '*', 'h', ARGV[3], 'p', ARGV[4], 'e', ARGV[5])
if ARGV[6] == '1' then
  redis.call('SET', KEYS[3], ARGV[7], 'EX', ARGV[8])
end
return 1
