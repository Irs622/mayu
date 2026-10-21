-- KEYS[1]=dead stream KEYS[2]=stream KEYS[3..2+n]=result keys, one per entry
-- KEYS[3+n..]=workflow hash of each entry that has a node, in entry order
-- ARGV = (dead entry id, header, payload, node) quadruples, one per entry; node is '' for a plain task
-- Moves each entry that is still dead back to the stream and deletes its stored failure result.
-- XDEL decides: a replay racing another one moves nothing twice. Returns how many moved.
-- A moved workflow node whose hash exists and whose state is dead becomes queued (and the hash's ttl is
-- refreshed), so its eventual ack releases its descendants.
local n = #ARGV / 4
local moved = 0
local w = 2 + n
for i = 1, n do
  local a = (i - 1) * 4 + 1
  local wf = nil
  if ARGV[a + 3] ~= '' then
    w = w + 1
    wf = KEYS[w]
  end
  if redis.call('XDEL', KEYS[1], ARGV[a]) == 1 then
    redis.call('XADD', KEYS[2], '*', 'h', ARGV[a + 1], 'p', ARGV[a + 2])
    redis.call('DEL', KEYS[2 + i])
    moved = moved + 1
    if wf then
      local field = 'n:' .. ARGV[a + 3] .. ':s'
      if redis.call('HGET', wf, field) == 'dead' then
        redis.call('HSET', wf, field, 'queued')
        local ttl = tonumber(redis.call('HGET', wf, 'ttl'))
        if ttl then
          redis.call('EXPIRE', wf, ttl)
        end
      end
    end
  end
end
return moved
