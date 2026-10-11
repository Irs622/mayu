-- KEYS[1]=dead stream KEYS[2]=stream KEYS[3..]=result keys, one per entry
-- ARGV = (dead entry id, header, payload) triples, one per KEYS[3..]
-- Moves each entry that is still dead back to the stream and deletes its stored failure result.
-- XDEL decides: a replay racing another one moves nothing twice. Returns how many moved.
local moved = 0
for i = 3, #KEYS do
  local a = (i - 3) * 3 + 1
  if redis.call('XDEL', KEYS[1], ARGV[a]) == 1 then
    redis.call('XADD', KEYS[2], '*', 'h', ARGV[a + 1], 'p', ARGV[a + 2])
    redis.call('DEL', KEYS[i])
    moved = moved + 1
  end
end
return moved
