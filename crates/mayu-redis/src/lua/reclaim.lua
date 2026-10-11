-- KEYS[1]=stream ; ARGV[1]=group ARGV[2]=consumer ARGV[3]=min idle ms ARGV[4]=cursor ARGV[5]=count
-- Returns {next_cursor, {{id, {field, value, ...}, delivery_count}, ...}}
local r = redis.call('XAUTOCLAIM', KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4], 'COUNT', ARGV[5])
local out = {}
for _, e in ipairs(r[2]) do
  local p = redis.call('XPENDING', KEYS[1], ARGV[1], e[1], e[1], 1)
  local count = 1
  if p[1] then count = p[1][4] end
  table.insert(out, {e[1], e[2], count})
end
return {r[1], out}
