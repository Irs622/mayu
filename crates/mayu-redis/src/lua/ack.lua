-- KEYS[1]=stream KEYS[2]=result key KEYS[3..]=follow-up streams
-- ARGV[1]=group ARGV[2]=entry id ARGV[3]=store result ('1'/'0') ARGV[4]=record ARGV[5]=ttl secs
-- ARGV[6..]=follow-up (header, payload) pairs, one per KEYS[3..]
-- Follow-ups (link callbacks) are added only by the call that acks the entry.
local acked = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
if ARGV[3] == '1' then
  redis.call('SET', KEYS[2], ARGV[4], 'EX', ARGV[5])
end
if acked == 1 then
  for i = 3, #KEYS do
    local a = 6 + (i - 3) * 2
    redis.call('XADD', KEYS[i], '*', 'h', ARGV[a], 'p', ARGV[a + 1])
  end
end
return 1
