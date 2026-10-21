-- KEYS[1]=workflow hash KEYS[2..]=stream of each root node, in node order
-- ARGV[1]=ttl secs ARGV[2]=F, the number of field/value pairs ARGV[3..2+2F]=the pairs
-- ARGV[3+2F..]=(header, payload) of each root, one pair per KEYS[2..]
-- Writes the DAG and enqueues the roots as one step. Refuses (returns 0) if the workflow already exists.
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
local f = tonumber(ARGV[2])
-- HSET takes every pair as an argument; unpack is capped (~8000) so a big DAG goes in chunks.
local i = 3
while i < 3 + 2 * f do
  local chunk = {}
  local last = math.min(i + 399, 2 + 2 * f)
  for j = i, last do
    chunk[#chunk + 1] = ARGV[j]
  end
  redis.call('HSET', KEYS[1], unpack(chunk))
  i = last + 1
end
redis.call('EXPIRE', KEYS[1], ARGV[1])
for k = 2, #KEYS do
  local a = 3 + 2 * f + (k - 2) * 2
  redis.call('XADD', KEYS[k], '*', 'h', ARGV[a], 'p', ARGV[a + 1])
end
return 1
