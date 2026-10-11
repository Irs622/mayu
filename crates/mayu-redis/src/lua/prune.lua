-- KEYS[1]=stream ; ARGV[1]=group ARGV[2]=min idle ms ARGV[3]=worker key prefix
-- Deletes consumers with no pending entries, idle >= min, and no heartbeat key.
-- Atomic: a consumer cannot gain a pending entry between the check and the delete.
local ok, consumers = pcall(redis.call, 'XINFO', 'CONSUMERS', KEYS[1], ARGV[1])
if not ok then return 0 end
local removed = 0
for _, c in ipairs(consumers) do
  local name, pending, idle
  for i = 1, #c, 2 do
    if c[i] == 'name' then name = c[i + 1]
    elseif c[i] == 'pending' then pending = c[i + 1]
    elseif c[i] == 'idle' then idle = c[i + 1] end
  end
  if pending == 0 and idle >= tonumber(ARGV[2]) and redis.call('EXISTS', ARGV[3] .. name) == 0 then
    redis.call('XGROUP', 'DELCONSUMER', KEYS[1], ARGV[1], name)
    removed = removed + 1
  end
end
return removed
